#!/usr/bin/env python3
"""
AssemblyAI Transcription Pipeline for 20 Minute Leaders
Transcribes YouTube videos with speaker diarization
"""

import os
import sys
import json
import time
import requests
from pathlib import Path

# Configuration
ASSEMBLYAI_API_KEY = os.environ.get('ASSEMBLYAI_API_KEY') or open(os.path.expanduser('~/.config/assemblyai/api_key')).read().strip()
BASE_URL = "https://api.assemblyai.com/v2"
HEADERS = {"authorization": ASSEMBLYAI_API_KEY}

SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent
TRANSCRIPTS_DIR = REPO_DIR / "transcripts" / "assemblyai"
PROGRESS_FILE = SCRIPT_DIR / "assemblyai_progress.json"


def get_youtube_audio_url(youtube_id: str) -> str:
    """Get direct audio URL from YouTube video ID using yt-dlp"""
    import subprocess
    result = subprocess.run(
        ['yt-dlp', '-f', 'bestaudio', '-g', f'https://youtube.com/watch?v={youtube_id}'],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to get audio URL: {result.stderr}")
    return result.stdout.strip()


def submit_transcription(audio_url: str, speaker_labels: bool = True) -> str:
    """Submit audio for transcription, returns transcript ID"""
    data = {
        "audio_url": audio_url,
        "speaker_labels": speaker_labels,
        "auto_chapters": True,
        "entity_detection": True,
    }
    
    response = requests.post(f"{BASE_URL}/transcript", json=data, headers=HEADERS)
    response.raise_for_status()
    return response.json()["id"]


def poll_transcription(transcript_id: str, max_wait: int = 600) -> dict:
    """Poll for transcription completion"""
    polling_endpoint = f"{BASE_URL}/transcript/{transcript_id}"
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        response = requests.get(polling_endpoint, headers=HEADERS)
        response.raise_for_status()
        result = response.json()
        
        status = result["status"]
        if status == "completed":
            return result
        elif status == "error":
            raise Exception(f"Transcription failed: {result.get('error')}")
        
        print(f"  Status: {status}... waiting")
        time.sleep(10)
    
    raise Exception(f"Transcription timed out after {max_wait}s")


def format_transcript_with_speakers(result: dict) -> str:
    """Format transcript with speaker labels"""
    if not result.get("utterances"):
        return result.get("text", "")
    
    lines = []
    current_speaker = None
    
    for utterance in result["utterances"]:
        speaker = utterance["speaker"]
        text = utterance["text"]
        
        if speaker != current_speaker:
            current_speaker = speaker
            # Map speaker labels to likely names (A = host Michael, B = guest typically)
            speaker_name = "Michael" if speaker == "A" else f"Guest"
            lines.append(f"\n**{speaker_name}:** {text}")
        else:
            lines.append(text)
    
    return " ".join(lines)


def load_progress() -> dict:
    """Load progress from file"""
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"completed": [], "failed": [], "pending": {}}


def save_progress(progress: dict):
    """Save progress to file"""
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def transcribe_episode(episode_num: str, youtube_id: str, progress: dict) -> bool:
    """Transcribe a single episode"""
    output_file = TRANSCRIPTS_DIR / f"ep{episode_num}_{youtube_id}.md"
    
    if output_file.exists():
        print(f"  Already exists: {output_file.name}")
        return True
    
    if episode_num in progress.get("completed", []):
        print(f"  Already completed in progress")
        return True
    
    try:
        # Check if we have a pending transcription
        if episode_num in progress.get("pending", {}):
            transcript_id = progress["pending"][episode_num]
            print(f"  Resuming pending transcription: {transcript_id}")
        else:
            print(f"  Getting audio URL...")
            audio_url = get_youtube_audio_url(youtube_id)
            
            print(f"  Submitting to AssemblyAI...")
            transcript_id = submit_transcription(audio_url)
            
            # Save as pending
            progress.setdefault("pending", {})[episode_num] = transcript_id
            save_progress(progress)
        
        print(f"  Waiting for transcription (ID: {transcript_id})...")
        result = poll_transcription(transcript_id)
        
        # Format and save
        formatted = format_transcript_with_speakers(result)
        
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        output_file.write_text(f"# Episode {episode_num} Transcript\n\n{formatted}")
        
        # Also save raw JSON for reference
        raw_file = TRANSCRIPTS_DIR / f"ep{episode_num}_{youtube_id}.json"
        raw_file.write_text(json.dumps(result, indent=2))
        
        # Update progress
        progress["completed"].append(episode_num)
        progress["pending"].pop(episode_num, None)
        save_progress(progress)
        
        print(f"  ✓ Saved: {output_file.name}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        progress.setdefault("failed", []).append({"episode": episode_num, "error": str(e)})
        progress["pending"].pop(episode_num, None)
        save_progress(progress)
        return False


def get_episodes_to_transcribe(limit: int = None) -> list:
    """Get list of episodes that need transcription"""
    episodes_file = REPO_DIR / "episodes.json"
    episodes = json.loads(episodes_file.read_text())
    
    to_transcribe = []
    for ep in episodes:
        episode_num = ep.get("episode")
        youtube_link = ep.get("link") or ""
        
        # Extract YouTube ID
        youtube_id = None
        if "youtube.com/watch?v=" in youtube_link:
            youtube_id = youtube_link.split("v=")[1].split("&")[0]
        elif "youtu.be/" in youtube_link:
            youtube_id = youtube_link.split("youtu.be/")[1].split("?")[0]
        
        if not youtube_id:
            continue
        
        # Check if already transcribed
        output_file = TRANSCRIPTS_DIR / f"ep{episode_num}_{youtube_id}.md"
        if not output_file.exists():
            to_transcribe.append({
                "episode": episode_num,
                "youtube_id": youtube_id,
                "guest": ep.get("guest", "Unknown")
            })
    
    # Sort by episode number descending (newest first)
    to_transcribe.sort(key=lambda x: int(x["episode"]), reverse=True)
    
    if limit:
        to_transcribe = to_transcribe[:limit]
    
    return to_transcribe


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Transcribe 20 Minute Leaders episodes with AssemblyAI")
    parser.add_argument("--limit", type=int, help="Max episodes to process")
    parser.add_argument("--episode", type=str, help="Specific episode number to transcribe")
    parser.add_argument("--list", action="store_true", help="List episodes needing transcription")
    args = parser.parse_args()
    
    if not ASSEMBLYAI_API_KEY:
        print("Error: ASSEMBLYAI_API_KEY not set")
        print("Set it via: export ASSEMBLYAI_API_KEY=your_key")
        print("Or save to: ~/.config/assemblyai/api_key")
        sys.exit(1)
    
    progress = load_progress()
    
    if args.list:
        episodes = get_episodes_to_transcribe()
        print(f"Episodes needing transcription: {len(episodes)}")
        for ep in episodes[:20]:
            print(f"  Ep {ep['episode']}: {ep['guest']}")
        if len(episodes) > 20:
            print(f"  ... and {len(episodes) - 20} more")
        return
    
    if args.episode:
        # Find specific episode
        episodes_file = REPO_DIR / "episodes.json"
        episodes = json.loads(episodes_file.read_text())
        ep_data = next((e for e in episodes if e["episode"] == args.episode), None)
        
        if not ep_data:
            print(f"Episode {args.episode} not found")
            sys.exit(1)
        
        youtube_link = ep_data.get("link") or ""
        youtube_id = None
        if "youtube.com/watch?v=" in youtube_link:
            youtube_id = youtube_link.split("v=")[1].split("&")[0]
        elif "youtu.be/" in youtube_link:
            youtube_id = youtube_link.split("youtu.be/")[1].split("?")[0]
        
        if not youtube_id:
            print(f"No YouTube link for episode {args.episode}")
            sys.exit(1)
        
        print(f"Transcribing Episode {args.episode}: {ep_data.get('guest', 'Unknown')}")
        transcribe_episode(args.episode, youtube_id, progress)
        return
    
    # Batch mode
    episodes = get_episodes_to_transcribe(limit=args.limit)
    print(f"Processing {len(episodes)} episodes...")
    
    success = 0
    failed = 0
    
    for i, ep in enumerate(episodes):
        print(f"\n[{i+1}/{len(episodes)}] Episode {ep['episode']}: {ep['guest']}")
        if transcribe_episode(ep["episode"], ep["youtube_id"], progress):
            success += 1
        else:
            failed += 1
    
    print(f"\n✓ Completed: {success}")
    print(f"✗ Failed: {failed}")


if __name__ == "__main__":
    main()
