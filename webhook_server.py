import hashlib
import hmac
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from sync_to_notion import CalendarSync

# Load environment variables
load_dotenv()

app = FastAPI()

# Get webhook secret from environment variable
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")


def verify_signature(signature: str, body: bytes) -> bool:
    """Verify the webhook signature"""
    if not WEBHOOK_SECRET:
        return False

    expected_signature = hmac.new(
        WEBHOOK_SECRET.encode(), body, hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(signature, expected_signature)


@app.post("/webhook/calendar-sync")
async def calendar_sync_webhook(request: Request):
    """Handle webhook from Google Apps Script"""
    # Get the signature from headers
    signature = request.headers.get("X-Webhook-Signature")
    if not signature:
        raise HTTPException(status_code=401, detail="No signature provided")

    # Get the raw body
    body = await request.body()

    # Verify signature
    if not verify_signature(signature, body):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        # Initialize and run the sync
        sync = CalendarSync()
        try:
            sync.sync_all_events()
            return JSONResponse(
                content={"status": "success", "message": "Calendar sync completed"},
                status_code=200,
            )
        finally:
            sync.close()
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
