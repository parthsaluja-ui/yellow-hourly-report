#!/usr/bin/env python3
"""
Yellow Hourly Report
Fetches latest CSV from Gmail, generates report, sends to Slack
"""

import os
import json
import base64
import io
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ── Config ──────────────────────────────────────────────────────────────────
SLACK_TOKEN   = os.environ.get("SLACK_TOKEN", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "C0AR8NQTUJJ")
EMAIL_SUBJECT = "Ashad Daily Report || SUCCESS"
CLIENT_SECRET = os.path.expanduser("~/.config/gws/client_secret.json")
TOKEN_PATH    = os.path.expanduser("~/.config/gws/gmail_token.json")
SCOPES        = ["https://www.googleapis.com/auth/gmail.readonly"]


# ── Gmail auth ───────────────────────────────────────────────────────────────
def get_gmail_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


# ── Fetch latest CSV from download link in Gmail ─────────────────────────────
def get_latest_csv(service):
    results = service.users().messages().list(
        userId="me",
        q=f'subject:"{EMAIL_SUBJECT}"',
        maxResults=1
    ).execute()

    messages = results.get("messages", [])
    if not messages:
        raise Exception(f"No emails found with subject: {EMAIL_SUBJECT}")

    msg = service.users().messages().get(
        userId="me", id=messages[0]["id"], format="full"
    ).execute()

    body = base64.urlsafe_b64decode(msg["payload"]["body"]["data"]).decode("utf-8", errors="ignore")

    # Extract CSV download URL from email body
    import re, html
    match = re.search(r"href='(https://app\.yellow\.ai/minio/exports/[^']+)'", body)
    if not match:
        match = re.search(r'href="(https://app\.yellow\.ai/minio/exports/[^"]+)"', body)
    if not match:
        raise Exception("No CSV download link found in email body")

    csv_url = html.unescape(match.group(1))
    print(f"Downloading CSV from: {csv_url[:80]}...")

    resp = requests.get(csv_url)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


# ── Generate report data ─────────────────────────────────────────────────────
def generate_report(df):
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist       = datetime.now(IST)
    one_hour_ago  = (now_ist - timedelta(hours=1)).replace(tzinfo=None)
    today_start   = now_ist.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # CSV times are in IST (no timezone info) — parse as naive
    df["TICKET_CREATION_TIME"] = pd.to_datetime(df["TICKET_CREATION_TIME"], errors="coerce")

    last_hour_df = df[df["TICKET_CREATION_TIME"] >= one_hour_ago]
    today_df     = df[df["TICKET_CREATION_TIME"] >= today_start]

    # 1. Snapshot — count unique sessions (chats), not tickets
    total_last_hour = last_hour_df["SESSION_ID"].nunique()
    total_today     = today_df["SESSION_ID"].nunique()

    unassigned = last_hour_df[
        last_hour_df["AGENT_NAME"].isna() | (last_hour_df["AGENT_NAME"].astype(str).str.strip() == "")
    ]["SESSION_ID"].nunique()

    avg_wait_secs = last_hour_df["QUEUE_WAIT_DURATION_IN_SECONDS"].mean()
    if pd.isna(avg_wait_secs):
        avg_wait_str = "N/A"
    else:
        avg_wait_str = f"{int(avg_wait_secs / 60)} mins"

    # 2. Source breakdown
    source_breakdown = last_hour_df.drop_duplicates("SESSION_ID")["SOURCE_CHANNEL"].value_counts().to_dict()

    return {
        "total_last_hour": total_last_hour,
        "total_today":     total_today,
        "unassigned":      unassigned,
        "avg_wait":        avg_wait_str,
        "source_breakdown": source_breakdown,
        "timestamp":       now_ist.strftime("%d %b %Y, %I:%M %p IST"),
    }


# ── Send to Slack ─────────────────────────────────────────────────────────────
def send_slack_report(report):
    def fmt(d):
        return "\n".join(f"  • {k}: *{v}*" for k, v in d.items()) if d else "  • No data"

    message = f"""*Yellow Hourly Report — {report['timestamp']}*

*1️⃣ Last Hour Snapshot*
• Chats (last 1hr): *{report['total_last_hour']}*
• Total chats today (cumulative): *{report['total_today']}*
• In queue / unassigned: *{report['unassigned']}*
• Avg wait time: *{report['avg_wait']}*

*2️⃣ Source Breakdown*
{fmt(report['source_breakdown'])}"""

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        json={"channel": SLACK_CHANNEL, "text": message, "mrkdwn": True},
    )
    result = resp.json()
    if not result.get("ok"):
        raise Exception(f"Slack error: {result.get('error')}")
    print("Report sent to Slack!")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[{datetime.now()}] Running Yellow hourly report...")
    service = get_gmail_service()
    df      = get_latest_csv(service)
    report  = generate_report(df)
    send_slack_report(report)
    print("Done!")
