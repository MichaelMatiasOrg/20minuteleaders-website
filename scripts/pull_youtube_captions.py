#!/usr/bin/env python3
"""
Pull YouTube captions for all episodes with youtubeId.
Saves as .txt files in transcripts/youtube/ folder.
"""

import json
import subprocess
import os
import sys
from pathlib import Path

REPO_DIR = Path(__file__).parent.parent
TRANSCRIPTS_DIR = REPO_DIR / "transcripts" / "youtube"
EPISODES_FILE = REPO_DIR / "js" / "episodes.js"
PROGRESS_FILE = REPO_DIR / "scripts" / "caption_progress.json"

def load_episodes():
    with open(EPISODES_FILE) as f:
        content = f.read()
        start = content.find('[')
        end = content.rfind(']') + 1
        return json.loads(content[start:end])

def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"completed": [], "failed": [], "no_captions": []}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def download_captions(youtube_id, episode_num):
    """Download captions for a single video using yt-dlp."""
    output_path = TRANSCRIPTS_DIR / f"ep{episode_num}_{youtube_id}"
    
    # Skip if already downloaded
    txt_file = Path(str(output_path) + ".en.vtt")
    if txt_file.exists() or Path(str(output_path) + ".en.vtt").exists():
        return "exists"
    
    cmd = [
        "yt-dlp",
        "--write-auto-sub",
        "--sub-lang", "en",
        "--skip-download",
        "--output", str(output_path),
        f"https://www.youtube.com/watch?v={youtube_id}"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        # Check if any subtitle file was created
        for ext in [".en.vtt", ".en.srt", ".vtt", ".srt"]:
            if Path(str(output_path) + ext).exists():
                return "success"
        
        if "no subtitles" in result.stderr.lower() or "no automatic captions" in result.stderr.lower():
            return "no_captions"
        
        return "failed"
    except subprocess.TimeoutExpired:
        return "timeout"
    except Exception as e:
        print(f"Error: {e}")
        return "failed"

def convert_vtt_to_txt(vtt_path):
    """Convert VTT to plain text."""
    txt_path = vtt_path.with_suffix('.txt')
    if txt_path.exists():
        return
    
    try:
        with open(vtt_path) as f:
            lines = f.readlines()
        
        # Skip header and timestamps, extract text
        text_lines = []
        for line in lines:
            line = line.strip()
            # Skip empty, timestamps, WEBVTT header, and position markers
            if not line or '-->' in line or line.startswith('WEBVTT') or line.startswith('Kind:') or line.startswith('Language:'):
                continue
            # Skip lines that are just numbers (cue identifiers)
            if line.isdigit():
                continue
            # Remove HTML-like tags
            import re
            line = re.sub(r'<[^>]+>', '', line)
            if line:
                text_lines.append(line)
        
        # Remove duplicate consecutive lines (common in auto-captions)
        deduped = []
        prev = None
        for line in text_lines:
            if line != prev:
                deduped.append(line)
                prev = line
        
        with open(txt_path, 'w') as f:
            f.write('\n'.join(deduped))
        
        print(f"  Converted to: {txt_path.name}")
    except Exception as e:
        print(f"  Error converting {vtt_path}: {e}")

def main():
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    
    episodes = load_episodes()
    progress = load_progress()
    
    # Filter episodes with YouTube IDs
    with_yt = [(e['episode'], e['youtubeId']) for e in episodes if e.get('youtubeId')]
    
    # Skip already processed
    completed_set = set(progress['completed'] + progress['failed'] + progress['no_captions'])
    to_process = [(ep, yt) for ep, yt in with_yt if yt not in completed_set]
    
    print(f"Total with YouTube: {len(with_yt)}")
    print(f"Already processed: {len(completed_set)}")
    print(f"To process: {len(to_process)}")
    print(f"Progress: {len(progress['completed'])} success, {len(progress['no_captions'])} no captions, {len(progress['failed'])} failed")
    print("-" * 50)
    
    for i, (ep_num, yt_id) in enumerate(to_process):
        print(f"[{i+1}/{len(to_process)}] Episode {ep_num} ({yt_id})...", end=" ", flush=True)
        
        result = download_captions(yt_id, ep_num)
        print(result)
        
        if result == "success" or result == "exists":
            progress['completed'].append(yt_id)
        elif result == "no_captions":
            progress['no_captions'].append(yt_id)
        else:
            progress['failed'].append(yt_id)
        
        # Save progress every 10 episodes
        if (i + 1) % 10 == 0:
            save_progress(progress)
            print(f"  Progress saved: {len(progress['completed'])} completed")
    
    save_progress(progress)
    
    # Convert all VTT files to TXT
    print("\nConverting VTT to TXT...")
    for vtt_file in TRANSCRIPTS_DIR.glob("*.vtt"):
        convert_vtt_to_txt(vtt_file)
    
    print("\n" + "=" * 50)
    print(f"DONE!")
    print(f"  Success: {len(progress['completed'])}")
    print(f"  No captions: {len(progress['no_captions'])}")
    print(f"  Failed: {len(progress['failed'])}")

if __name__ == "__main__":
    main()
