import os

import mysql.connector
from dotenv import load_dotenv
from notion_client import Client

# Load environment variables
load_dotenv()


class CalendarSync:
    def __init__(self):
        # MySQL connection
        self.db = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
        )

        # Notion connection
        self.notion = Client(auth=os.getenv("NOTION_API_KEY"))
        self.database_id = os.getenv("NOTION_DATABASE_ID")

        # Initialize cursor
        self.cursor = self.db.cursor(dictionary=True)

    def get_notion_pages(self):
        """Get all calendar event pages from Notion"""
        pages = {}
        start_cursor = None

        while True:
            response = self.notion.databases.query(
                database_id=self.database_id, start_cursor=start_cursor, page_size=100
            )

            for page in response["results"]:
                event_id = self._get_property_content(
                    page["properties"].get("Event ID", {})
                )
                if event_id:
                    pages[event_id] = page["id"]

            if not response.get("has_more"):
                break
            start_cursor = response["next_cursor"]

        return pages

    def _get_property_content(self, property_data):
        """Extract content from a Notion property"""
        if not property_data or "rich_text" not in property_data:
            return None
        rich_text = property_data["rich_text"]
        if not rich_text:
            return None
        return rich_text[0]["text"]["content"]

    def get_calendar_events(self):
        """Get all calendar events with their attendees"""
        query = """
        SELECT 
            e.id,
            e.event_id,
            e.title,
            e.start_time,
            e.end_time,
            e.all_day,
            e.location,
            e.description,
            c.email as calendar_email,
            c.summary as calendar_name,
            GROUP_CONCAT(
                DISTINCT a.attendee_email
                SEPARATOR ', '
            ) as attendees,
            r.recurrence_type,
            r.`interval`,
            r.by_day
        FROM events e
        LEFT JOIN calendars c ON e.calendar_id = c.id
        LEFT JOIN attendees a ON e.id = a.event_id
        LEFT JOIN recurrence r ON e.id = r.event_id
        GROUP BY e.id
        ORDER BY e.start_time DESC
        """

        self.cursor.execute(query)
        return {event["event_id"]: event for event in self.cursor.fetchall()}

    def _format_recurrence_text(self, event):
        """Format recurrence information into readable text"""
        if not event["recurrence_type"]:
            return None

        parts = []
        recurrence_type = event["recurrence_type"].lower()
        interval = event["interval"] or 1
        by_day = event["by_day"]

        # Handle interval
        if interval == 1:
            parts.append(f"Every {recurrence_type}")
        else:
            parts.append(f"Every {interval} {recurrence_type}s")

        # Handle specific days
        if by_day:
            # Convert abbreviated days to full names
            day_mapping = {
                "MO": "Monday",
                "TU": "Tuesday",
                "WE": "Wednesday",
                "TH": "Thursday",
                "FR": "Friday",
                "SA": "Saturday",
                "SU": "Sunday",
            }
            days = by_day.split(",")
            formatted_days = [day_mapping.get(day.strip(), day.strip()) for day in days]

            if len(formatted_days) == 1:
                parts.append(f"on {formatted_days[0]}")
            else:
                parts.append(
                    f"on {', '.join(formatted_days[:-1])} and {formatted_days[-1]}"
                )

        return " ".join(parts)

    def update_notion_page(self, page_id, event):
        """Update an existing Notion page"""
        # Format attendees list
        attendees = event["attendees"].split(", ") if event["attendees"] else []

        # Format recurrence info
        recurrence_text = self._format_recurrence_text(event)

        # Create the page properties
        properties = {
            "Name": {"title": [{"text": {"content": event["title"]}}]},
            "Date": {
                "date": {
                    "start": event["start_time"].isoformat(),
                    "end": event["end_time"].isoformat(),
                }
            },
            "Calendar": {"select": {"name": event["calendar_name"]}},
            "Location": {"rich_text": [{"text": {"content": event["location"] or ""}}]},
            "All Day": {"checkbox": bool(event["all_day"])},
            "Event ID": {"rich_text": [{"text": {"content": event["event_id"]}}]},
        }

        if event["description"]:
            properties["Description"] = {
                "rich_text": [{"text": {"content": event["description"]}}]
            }

        if recurrence_text:
            properties["Recurrence"] = {
                "rich_text": [{"text": {"content": recurrence_text}}]
            }

        if attendees:
            properties["Attendees"] = {
                "multi_select": [
                    {"name": email.strip()} for email in attendees if email.strip()
                ]
            }

        try:
            self.notion.pages.update(page_id=page_id, properties=properties)
            print(f"Updated Notion page for event: {event['title']}")
            return True
        except Exception as e:
            print(f"Error updating Notion page for {event['title']}: {str(e)}")
            return False

    def create_notion_page(self, event):
        """Create a new Notion page for a calendar event"""
        # Format attendees list
        attendees = event["attendees"].split(", ") if event["attendees"] else []

        # Format recurrence info
        recurrence_text = self._format_recurrence_text(event)

        # Create the page properties
        properties = {
            "Name": {"title": [{"text": {"content": event["title"]}}]},
            "Date": {
                "date": {
                    "start": event["start_time"].isoformat(),
                    "end": event["end_time"].isoformat(),
                }
            },
            "Calendar": {"select": {"name": event["calendar_name"]}},
            "Location": {"rich_text": [{"text": {"content": event["location"] or ""}}]},
            "All Day": {"checkbox": bool(event["all_day"])},
            "Event ID": {"rich_text": [{"text": {"content": event["event_id"]}}]},
        }

        if event["description"]:
            properties["Description"] = {
                "rich_text": [{"text": {"content": event["description"]}}]
            }

        if recurrence_text:
            properties["Recurrence"] = {
                "rich_text": [{"text": {"content": recurrence_text}}]
            }

        if attendees:
            properties["Attendees"] = {
                "multi_select": [
                    {"name": email.strip()} for email in attendees if email.strip()
                ]
            }

        try:
            response = self.notion.pages.create(
                parent={"database_id": self.database_id}, properties=properties
            )
            print(f"Created Notion page for event: {event['title']}")
            return response["id"]
        except Exception as e:
            print(f"Error creating Notion page for {event['title']}: {str(e)}")
            return None

    def delete_notion_page(self, page_id):
        """Archive a Notion page"""
        try:
            self.notion.pages.update(page_id=page_id, archived=True)
            print(f"Archived Notion page: {page_id}")
            return True
        except Exception as e:
            print(f"Error archiving Notion page {page_id}: {str(e)}")
            return False

    def sync_all_events(self):
        """Sync all calendar events to Notion maintaining 1:1 relationship"""
        print("Starting calendar sync to Notion...")

        # Get all current events from MySQL
        mysql_events = self.get_calendar_events()
        print(f"Found {len(mysql_events)} events in MySQL")

        # Get all current pages from Notion
        notion_pages = self.get_notion_pages()
        print(f"Found {len(notion_pages)} pages in Notion")

        # Track statistics
        created = 0
        updated = 0
        deleted = 0
        errors = 0

        # Update existing and create new pages
        for event_id, event in mysql_events.items():
            if event_id in notion_pages:
                # Update existing page
                if self.update_notion_page(notion_pages[event_id], event):
                    updated += 1
                else:
                    errors += 1
                # Remove from notion_pages dict to track what's been processed
                del notion_pages[event_id]
            else:
                # Create new page
                if self.create_notion_page(event):
                    created += 1
                else:
                    errors += 1

        # Delete pages that no longer exist in MySQL
        for event_id, page_id in notion_pages.items():
            if self.delete_notion_page(page_id):
                deleted += 1
            else:
                errors += 1

        print("\nSync completed!")
        print(f"Created: {created}")
        print(f"Updated: {updated}")
        print(f"Deleted: {deleted}")
        if errors > 0:
            print(f"Errors: {errors}")

    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.db.close()


def main():
    sync = CalendarSync()
    try:
        sync.sync_all_events()
    finally:
        sync.close()


if __name__ == "__main__":
    main()
