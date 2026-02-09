#!/usr/bin/env python3
"""
Sync transcripts from Notion to Google Drive.
Creates a Google Doc transcript in each episode's Drive folder.

Usage:
    python3 sync_transcripts_to_drive.py [--limit N] [--resume]
"""

import json
import urllib.request
import os
import time
import sys
from datetime import datetime

# Setup
TOKENS_FILE = os.path.expanduser("~/.clawdbot/genie-email/tokens.json")
NOTION_KEY_FILE = os.path.expanduser("~/.config/notion/api_key_michael")
MATCHED_FILE = "/tmp/matched_final.json"
PROGRESS_FILE = os.path.expanduser("~/clawd/work/transcript_sync_progress.json")
LOG_FILE = os.path.expanduser("~/clawd/work/transcript_sync.log")

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def load_tokens():
    return json.load(open(TOKENS_FILE))

def notion_request(url, data=None):
    notion_key = open(NOTION_KEY_FILE).read().strip()
    headers = {"Authorization": f"Bearer {notion_key}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}
    req = urllib.request.Request(url, headers=headers, method='POST' if data else 'GET')
    if data:
        req.data = json.dumps(data).encode()
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())

def get_notion_transcript(page_id):
    """Get transcript from Notion page blocks"""
    try:
        blocks = notion_request(f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100")
        
        in_transcript = False
        transcript_lines = []
        
        for block in blocks.get("results", []):
            block_type = block["type"]
            
            if block_type.startswith("heading"):
                heading_content = block.get(block_type, {}).get("rich_text", [])
                if heading_content:
                    text = heading_content[0].get("plain_text", "").lower()
                    if "transcript" in text:
                        in_transcript = True
                        continue
                    elif in_transcript:
                        break
            
            if in_transcript and block_type == "paragraph":
                rich_text = block.get("paragraph", {}).get("rich_text", [])
                for rt in rich_text:
                    transcript_lines.append(rt.get("plain_text", ""))
        
        return "\n\n".join(transcript_lines) if transcript_lines else None
    except Exception as e:
        log(f"  Error getting Notion transcript: {e}")
        return None

def create_google_doc(folder_id, title, content, access_token):
    """Create a Google Doc in the specified folder"""
    try:
        doc_metadata = {
            "name": title,
            "mimeType": "application/vnd.google-apps.document",
            "parents": [folder_id]
        }
        
        create_url = "https://www.googleapis.com/drive/v3/files"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        req = urllib.request.Request(create_url, headers=headers, method='POST')
        req.data = json.dumps(doc_metadata).encode()
        
        with urllib.request.urlopen(req, timeout=60) as resp:
            doc = json.loads(resp.read().decode())
        
        doc_id = doc["id"]
        
        # Update content
        docs_url = f"https://docs.googleapis.com/v1/documents/{doc_id}:batchUpdate"
        update_data = {"requests": [{"insertText": {"location": {"index": 1}, "text": content}}]}
        
        req = urllib.request.Request(docs_url, headers=headers, method='POST')
        req.data = json.dumps(update_data).encode()
        
        with urllib.request.urlopen(req, timeout=60) as resp:
            pass
        
        return doc_id
    except Exception as e:
        log(f"  Error creating Google Doc: {e}")
        return None

def check_existing_transcript_doc(folder_id, access_token):
    """Check if transcript doc already exists"""
    try:
        url = f"https://www.googleapis.com/drive/v3/files?q='{folder_id}'+in+parents+and+name+contains+'Transcript'&fields=files(id,name)"
        headers = {"Authorization": f"Bearer {access_token}"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        files = data.get("files", [])
        return files[0] if files else None
    except:
        return None

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        return json.load(open(PROGRESS_FILE))
    return {"synced": [], "skipped": [], "errors": [], "last_index": 0}

def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Max episodes to process")
    parser.add_argument("--resume", action="store_true", help="Resume from last position")
    args = parser.parse_args()
    
    # Load data
    matched = json.load(open(MATCHED_FILE))
    tokens = load_tokens()
    access_token = tokens["access_token"]
    
    progress = load_progress() if args.resume else {"synced": [], "skipped": [], "errors": [], "last_index": 0}
    start_index = progress["last_index"] if args.resume else 0
    
    total = len(matched)
    end_index = min(start_index + args.limit, total) if args.limit else total
    
    log(f"=== Starting sync: {start_index} to {end_index} of {total} ===")
    
    for i in range(start_index, end_index):
        ep = matched[i]
        ep_num = ep.get("episode", "?")
        guest = ep.get("guest", "Unknown")
        folder_id = ep.get("folder_id")
        
        log(f"[{i+1}/{total}] Ep {ep_num}: {guest}")
        
        # Check if exists
        existing = check_existing_transcript_doc(folder_id, access_token)
        if existing:
            log(f"  ⏭️  Exists: {existing['name']}")
            progress["skipped"].append(ep_num)
            progress["last_index"] = i + 1
            save_progress(progress)
            continue
        
        # Get Notion page
        try:
            search_data = notion_request(
                f"https://api.notion.com/v1/databases/13fb1a3e-b70a-4c63-afd6-08bba2e05a3e/query",
                data={"filter": {"property": "Episode No.", "number": {"equals": ep_num}}, "page_size": 1}
            )
            
            if not search_data.get("results"):
                log(f"  ⚠️  No Notion page")
                progress["skipped"].append(ep_num)
                progress["last_index"] = i + 1
                save_progress(progress)
                continue
            
            page_id = search_data["results"][0]["id"]
            transcript = get_notion_transcript(page_id)
            
            if not transcript or len(transcript) < 100:
                log(f"  ⏭️  No transcript in Notion")
                progress["skipped"].append(ep_num)
                progress["last_index"] = i + 1
                save_progress(progress)
                continue
            
            # Create Google Doc
            doc_title = f"Ep{ep_num} - {guest} (Transcript)"
            doc_id = create_google_doc(folder_id, doc_title, transcript, access_token)
            
            if doc_id:
                log(f"  ✅ Created: {doc_title}")
                progress["synced"].append(ep_num)
            else:
                progress["errors"].append(ep_num)
                
        except Exception as e:
            log(f"  ❌ Error: {e}")
            progress["errors"].append(ep_num)
        
        progress["last_index"] = i + 1
        save_progress(progress)
        time.sleep(0.3)
    
    log(f"\n=== Summary ===")
    log(f"✅ Synced: {len(progress['synced'])}")
    log(f"⏭️  Skipped: {len(progress['skipped'])}")
    log(f"❌ Errors: {len(progress['errors'])}")

if __name__ == "__main__":
    main()
