from youtube_transcript_api import YouTubeTranscriptApi

def format_time(seconds):
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"

video_id = "qYbKel-Oa6Q"
output_file = "Z19.YT.time.txt"

transcript = YouTubeTranscriptApi().fetch(video_id)

with open(output_file, "w", encoding="utf-8") as f:
    for item in transcript:
        start = item.start
        duration = item.duration
        end = start + duration
        text = item.text.replace("\n", " ")

        line = f"[{format_time(start)} - {format_time(end)}] {text}\n"
        f.write(line)

print("Transcript saved ")
