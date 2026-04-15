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
SLACK_TOKEN        = os.environ.get("SLACK_TOKEN", "")
SLACK_CHANNEL      = os.environ.get("SLACK_CHANNEL", "C0AR8NQTUJJ")
EMAIL_SUBJECT      = "Ashad Daily Report || SUCCESS"
CLIENT_SECRET      = os.path.expanduser("~/.config/gws/client_secret.json")
TOKEN_PATH         = os.path.expanduser("~/.config/gws/gmail_token.json")
SCOPES             = ["https://www.googleapis.com/auth/gmail.readonly",
                      "https://www.googleapis.com/auth/drive.file"]
DRIVE_FOLDER_ID    = "1_gpSTsdb2r4BpFPgrbYWP6wiMGH2L0Oy"  # Yellow CSV's folder


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
        maxResults=2
    ).execute()

    messages = results.get("messages", [])
    if not messages:
        raise Exception(f"No emails found with subject: {EMAIL_SUBJECT}")

    import re, html
    dfs = []
    csv_texts = []
    for msg_meta in messages:
        msg = service.users().messages().get(
            userId="me", id=msg_meta["id"], format="full"
        ).execute()
        body = base64.urlsafe_b64decode(msg["payload"]["body"]["data"]).decode("utf-8", errors="ignore")
        match = re.search(r"href='(https://app\.yellow\.ai/minio/exports/[^']+)'", body)
        if not match:
            match = re.search(r'href="(https://app\.yellow\.ai/minio/exports/[^"]+)"', body)
        if not match:
            continue
        csv_url = html.unescape(match.group(1))
        print(f"Downloading CSV from: {csv_url[:80]}...")
        for attempt in range(3):
            resp = requests.get(csv_url, timeout=30)
            if resp.status_code == 200:
                dfs.append(pd.read_csv(io.StringIO(resp.text)))
                csv_texts.append(resp.text)
                break
            print(f"Attempt {attempt+1} failed ({resp.status_code}), retrying...")
            import time; time.sleep(5)

    if not dfs:
        raise Exception("No CSV download link found in emails")

    # Return latest CSV + merged (for queue count)
    latest_df = dfs[0]
    merged_df = pd.concat(dfs).drop_duplicates(subset="SESSION_ID", keep="first")  # keep newest

    # Generate filename with IST timestamp
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)
    csv_filename = f"yellow_{now_ist.strftime('%Y-%m-%d_%H%M')}_IST.csv"

    return latest_df, merged_df, csv_texts[0], csv_filename


# ── Generate report data ─────────────────────────────────────────────────────
def generate_report(df, merged_df):
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist       = datetime.now(IST)
    # Filter for 2 hours ago → 1 hour ago (complete hour with full data in CSV)
    # e.g. at 7:05 PM → filter 5:00-6:00 PM
    hour_end      = now_ist.replace(minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(hours=1)
    one_hour_ago  = hour_end - timedelta(hours=1)
    today_start   = now_ist.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # CSV times are in IST (no timezone info) — parse as naive
    df["TICKET_CREATION_TIME"] = pd.to_datetime(df["TICKET_CREATION_TIME"], errors="coerce")

    last_hour_df = df[(df["TICKET_CREATION_TIME"] >= one_hour_ago) & (df["TICKET_CREATION_TIME"] < hour_end)]
    today_df     = df[df["TICKET_CREATION_TIME"] >= today_start]

    # Cumulative uses merged CSV for more complete count
    merged_df["TICKET_CREATION_TIME"] = pd.to_datetime(merged_df["TICKET_CREATION_TIME"], errors="coerce")
    merged_today_df = merged_df[merged_df["TICKET_CREATION_TIME"] >= today_start]

    # 1. Snapshot — count unique sessions (chats), not tickets
    total_last_hour = last_hour_df["SESSION_ID"].nunique()
    # Cumulative = total rows from latest CSV for today
    latest_today_df = df[df["TICKET_CREATION_TIME"] >= today_start]
    total_today     = len(latest_today_df)

    # Unassigned = ALL queued tickets from merged CSV (more complete)
    unassigned = merged_df[
        merged_df["TICKET_STATUS"].astype(str).str.upper() == "QUEUED"
    ]["SESSION_ID"].nunique()

    # Avg wait time only for tickets that were actually assigned/resolved
    assigned_df = last_hour_df[
        last_hour_df["TICKET_STATUS"].astype(str).str.upper().isin(["ASSIGNED", "RESOLVED"])
    ]
    avg_wait_secs = assigned_df["QUEUE_WAIT_DURATION_IN_SECONDS"].replace(0, pd.NA).mean()
    if pd.isna(avg_wait_secs):
        avg_wait_str = "N/A"
    else:
        avg_wait_str = f"{int(avg_wait_secs / 60)} mins"

    # 2. Total currently assigned to L1 + L2 (across all time, not just last hour)
    assigned_l1_l2 = merged_df[
        (merged_df["TICKET_STATUS"].astype(str).str.upper() == "ASSIGNED") &
        (merged_df["GROUP_CODE"].astype(str).str.upper().isin(["L1", "L2"]))
    ]["SESSION_ID"].nunique()

    # 3. Resolved in last hour (by RESOLUTION_TIME, not creation time)
    df["RESOLUTION_TIME"] = pd.to_datetime(df["RESOLUTION_TIME"], errors="coerce")
    resolved_last_hour = df[
        (df["RESOLUTION_TIME"] >= one_hour_ago) & (df["RESOLUTION_TIME"] < hour_end)
    ]["SESSION_ID"].nunique()

    # 4. Spillover — created in the hour before last, NOT yet resolved
    two_hours_ago = one_hour_ago - timedelta(hours=1)
    prev_hour_df = df[
        (df["TICKET_CREATION_TIME"] >= two_hours_ago) & (df["TICKET_CREATION_TIME"] < one_hour_ago)
    ]
    spillover = prev_hour_df[
        prev_hour_df["TICKET_STATUS"].astype(str).str.upper() != "RESOLVED"
    ]["SESSION_ID"].nunique()

    # 5. Source breakdown
    source_breakdown = last_hour_df.drop_duplicates("SESSION_ID")["SOURCE_CHANNEL"].value_counts().to_dict()

    return {
        "total_last_hour":   total_last_hour,
        "total_today":       total_today,
        "unassigned":        unassigned,
        "avg_wait":          avg_wait_str,
        "assigned_l1_l2":    assigned_l1_l2,
        "resolved_last_hour": resolved_last_hour,
        "spillover":         spillover,
        "source_breakdown":  source_breakdown,
        "timestamp":         one_hour_ago.strftime('%d %b %Y'),
    }


# ── Upload CSV to Google Drive ───────────────────────────────────────────────
def upload_to_drive(csv_content, filename):
    from googleapiclient.http import MediaInMemoryUpload
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    drive = build("drive", "v3", credentials=creds)

    # Check if file already exists in folder
    existing = drive.files().list(
        q=f"name='{filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id)"
    ).execute().get("files", [])

    media = MediaInMemoryUpload(csv_content.encode("utf-8"), mimetype="text/csv")

    if existing:
        # Update existing file
        drive.files().update(fileId=existing[0]["id"], media_body=media).execute()
        print(f"Updated {filename} in Drive")
    else:
        # Create new file
        drive.files().create(
            body={"name": filename, "parents": [DRIVE_FOLDER_ID]},
            media_body=media
        ).execute()
        print(f"Uploaded {filename} to Drive")


# ── Send to Slack ─────────────────────────────────────────────────────────────
def send_slack_report(report):
    def fmt(d):
        return "\n".join(f"  • {k}: *{v}*" for k, v in d.items()) if d else "  • No data"

    message = f"""*Yellow Hourly Report — {report['timestamp']}*

*1️⃣ Last Hour Snapshot*
• Chats (last 1hr): *{report['total_last_hour']}*
• Total chats today (New + Reopen): *{report['total_today']}*
• In queue / unassigned: *{report['unassigned']}*
• Avg wait time: *{report['avg_wait']}*
• Assigned chats: *{report['assigned_l1_l2']}*
• Resolved last hour: *{report['resolved_last_hour']}*
• Spillover (prev hour, unresolved): *{report['spillover']}*

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
    service            = get_gmail_service()
    df, merged_df, csv_content, csv_filename = get_latest_csv(service)
    upload_to_drive(csv_content, csv_filename)
    report             = generate_report(df, merged_df)
    send_slack_report(report)
    print("Done!")
