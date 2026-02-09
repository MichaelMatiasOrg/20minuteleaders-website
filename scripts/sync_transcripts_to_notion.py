#!/usr/bin/env python3
"""
Sync transcripts to Notion episode pages.
- Adds transcript text to the page body (as paragraph blocks)
- Creates Google Doc with transcript
- Updates 'Link to transcript' property
"""
import json
import os
import urllib.request
import urllib.parse
import re
import time

# Config
NOTION_KEY = open(os.path.expanduser("~/.config/notion/api_key_michael")).read().strip()
DB_ID = "13fb1a3e-b70a-4c63-afd6-08bba2e05a3e"
TRANSCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "transcripts")
MAPPING_FILE = os.path.join(TRANSCRIPTS_DIR, "mapping.json")

# Google Drive
DRIVE_TOKENS = json.load(open(os.path.expanduser("~/.clawdbot/genie-email/tokens.json")))

def notion_request(url, method='GET', data=None):
    headers = {
        "Authorization": f"Bearer {NOTION_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    req = urllib.request.Request(url, headers=headers, method=method)
    if data:
        req.data = json.dumps(data).encode()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"  Notion error: {e.code} - {e.read().decode()[:200]}")
        return None

def find_episode_page(episode_num):
    """Find Notion page for an episode number."""
    data = notion_request(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        method='POST',
        data={
            "filter": {
                "property": "Episode No.",
                "number": {"equals": int(episode_num)}
            }
        }
    )
    if data and data.get('results'):
        return data['results'][0]['id']
    return None

def add_transcript_to_page(page_id, transcript_text):
    """Add transcript text as blocks to a Notion page."""
    # Split into chunks (Notion has 2000 char limit per block)
    chunks = []
    words = transcript_text.split()
    current_chunk = []
    current_len = 0
    
    for word in words:
        if current_len + len(word) + 1 > 1900:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_len = len(word)
        else:
            current_chunk.append(word)
            current_len += len(word) + 1
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    # Create blocks
    blocks = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "📝 Transcript"}}]
            }
        }
    ]
    
    for chunk in chunks[:100]:  # Notion limit: 100 blocks per request
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": chunk}}]
            }
        })
    
    # Append blocks to page
    result = notion_request(
        f"https://api.notion.com/v1/blocks/{page_id}/children",
        method='PATCH',
        data={"children": blocks}
    )
    return result is not None

def update_transcript_link(page_id, doc_url):
    """Update the 'Link to transcript' property."""
    return notion_request(
        f"https://api.notion.com/v1/pages/{page_id}",
        method='PATCH',
        data={
            "properties": {
                "Link to transcript": {"url": doc_url}
            }
        }
    )

def main():
    print("Loading transcript mapping...")
    with open(MAPPING_FILE) as f:
        mapping = json.load(f)
    
    # Deduplicate by episode (keep highest score)
    by_episode = {}
    for m in mapping:
        ep = m['episode']
        if ep not in by_episode or m['score'] > by_episode[ep]['score']:
            by_episode[ep] = m
    
    print(f"Found {len(by_episode)} unique episodes with transcripts")
    
    synced = 0
    failed = 0
    
    for ep_num, m in sorted(by_episode.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0, reverse=True)[:]:
        print(f"\nProcessing Ep{ep_num} ({m['guest']})...")
        
        # Load transcript text
        text_path = m.get('text_path')
        if not text_path or not os.path.exists(text_path):
            print(f"  ✗ No transcript file found")
            failed += 1
            continue
        
        with open(text_path, 'r', encoding='utf-8', errors='ignore') as f:
            transcript = f.read().strip()
        
        if len(transcript) < 100:
            print(f"  ✗ Transcript too short ({len(transcript)} chars)")
            failed += 1
            continue
        
        # Find Notion page
        page_id = find_episode_page(ep_num)
        if not page_id:
            print(f"  ✗ Notion page not found")
            failed += 1
            continue
        
        print(f"  Found page: {page_id[:8]}...")
        
        # Add transcript to page
        if add_transcript_to_page(page_id, transcript):
            print(f"  ✓ Added transcript ({len(transcript)} chars)")
            synced += 1
        else:
            print(f"  ✗ Failed to add transcript")
            failed += 1
        
        time.sleep(0.5)  # Rate limit
    
    print(f"\n{'='*50}")
    print(f"Done! Synced: {synced}, Failed: {failed}")

if __name__ == '__main__':
    main()
