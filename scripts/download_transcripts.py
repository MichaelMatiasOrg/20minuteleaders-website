#!/usr/bin/env python3
"""Download .srt transcript files from Google Drive and map to episodes."""
import json
import os
import subprocess
import re
from difflib import SequenceMatcher

# Config
TOKENS_FILE = os.path.expanduser("~/.clawdbot/genie-email/tokens.json")
TRANSCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "transcripts")
EPISODES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "episodes.json")

def get_access_token():
    with open(TOKENS_FILE) as f:
        return json.load(f)['access_token']

def drive_search(query, token):
    """Search Drive for files matching query."""
    import urllib.request
    import urllib.parse
    
    url = f"https://www.googleapis.com/drive/v3/files?q={urllib.parse.quote(query)}&pageSize=1000&fields=files(id,name,mimeType)"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode()).get('files', [])

def download_file(file_id, filename, token):
    """Download a file from Drive."""
    import urllib.request
    
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    
    filepath = os.path.join(TRANSCRIPTS_DIR, filename)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            with open(filepath, 'wb') as f:
                f.write(resp.read())
        return filepath
    except Exception as e:
        print(f"  Error downloading {filename}: {e}")
        return None

def normalize_name(name):
    """Normalize a name for matching."""
    # Remove common suffixes and clean up
    name = name.lower()
    name = re.sub(r'\.srt$|\.en_us$|_final$|\(new\)|\(\d+\)|_subtitles$|\.mp4$|\.mov$', '', name)
    name = re.sub(r'[_\-]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def match_guest(srt_name, episodes):
    """Find best matching episode for a transcript filename."""
    norm_srt = normalize_name(srt_name)
    
    best_match = None
    best_score = 0
    
    for ep in episodes:
        guest = ep.get('guest', '').lower()
        if not guest:
            continue
        
        # Try exact match first
        if guest in norm_srt or norm_srt in guest:
            return ep, 1.0
        
        # Fuzzy match
        score = SequenceMatcher(None, norm_srt, guest).ratio()
        if score > best_score and score > 0.6:
            best_score = score
            best_match = ep
    
    return best_match, best_score

def srt_to_text(srt_content):
    """Convert SRT format to plain text."""
    lines = srt_content.split('\n')
    text_lines = []
    
    for line in lines:
        line = line.strip()
        # Skip timestamps and sequence numbers
        if re.match(r'^\d+$', line):
            continue
        if re.match(r'^\d{2}:\d{2}:\d{2}', line):
            continue
        if '-->' in line:
            continue
        if line:
            text_lines.append(line)
    
    return ' '.join(text_lines)

def main():
    os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)
    
    print("Loading episodes...")
    with open(EPISODES_FILE) as f:
        episodes = json.load(f)
    print(f"  Loaded {len(episodes)} episodes")
    
    print("\nGetting access token...")
    token = get_access_token()
    
    print("\nSearching for .srt files...")
    srt_files = drive_search("name contains '.srt'", token)
    print(f"  Found {len(srt_files)} .srt files")
    
    # Filter to episode-related files
    episode_srts = [f for f in srt_files if '_final' in f['name'].lower() 
                    or 'subtitles' in f['name'].lower()
                    or any(c.isalpha() for c in f['name'].split('.')[0])]
    print(f"  {len(episode_srts)} appear to be episode transcripts")
    
    # Match and download
    matched = 0
    downloaded = 0
    mapping = []
    
    print("\nMatching and downloading transcripts...")
    for srt in episode_srts:  # Start with first 50
        ep, score = match_guest(srt['name'], episodes)
        if ep and score >= 0.6:
            matched += 1
            print(f"  ✓ {srt['name']} → Ep{ep.get('episode')} {ep.get('guest')} (score: {score:.2f})")
            
            # Download
            filepath = download_file(srt['id'], srt['name'], token)
            if filepath:
                downloaded += 1
                
                # Convert to text
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    srt_content = f.read()
                text = srt_to_text(srt_content)
                
                # Save text version
                text_path = filepath.replace('.srt', '.txt')
                with open(text_path, 'w') as f:
                    f.write(text)
                
                mapping.append({
                    'episode': ep.get('episode'),
                    'guest': ep.get('guest'),
                    'srt_file': srt['name'],
                    'drive_id': srt['id'],
                    'score': score,
                    'text_path': text_path
                })
        else:
            print(f"  ✗ {srt['name']} - no match found")
    
    # Save mapping
    mapping_file = os.path.join(TRANSCRIPTS_DIR, 'mapping.json')
    with open(mapping_file, 'w') as f:
        json.dump(mapping, f, indent=2)
    
    print(f"\nDone! Matched: {matched}, Downloaded: {downloaded}")
    print(f"Mapping saved to: {mapping_file}")

if __name__ == '__main__':
    main()
