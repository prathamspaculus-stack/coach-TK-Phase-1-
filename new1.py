import os
import re
import subprocess
from youtube_transcript_api import YouTubeTranscriptApi
from langchain_community.document_loaders import PyPDFLoader
import whisper
from urllib.parse import urlparse, parse_qs


BASE_FOLDER = r"C:\Users\Administrator\Desktop\Coach TK\Documents"

VIDEO_EXTENSIONS = (".mp4", ".mkv", ".avi", ".mov")
AUDIO_EXTENSIONS = (".mp3", ".wav", ".m4a")

YOUTUBE_LINKS = [
    "3OBREA0u_W4",
]

model = whisper.load_model("base")

def convert_video_to_audio(video_path):
    audio_path = os.path.splitext(video_path)[0] + ".m4a"

    if os.path.exists(audio_path):
        return audio_path

    subprocess.run(
        [
            "ffmpeg", "-i", video_path,
            "-vn", "-c:a", "aac", "-b:a", "128k",
            audio_path
        ],
        check=True
    )

    print(f"Converted: {os.path.basename(video_path)}")
    return audio_path

def transcribe_audio(audio_path):
    txt_path = os.path.splitext(audio_path)[0] + ".txt"

    if os.path.exists(txt_path):
        print(f"Already transcribed: {os.path.basename(audio_path)}")
        return

    print(f"Transcribing: {os.path.basename(audio_path)}")
    result = model.transcribe(audio_path)

    with open(txt_path, "w", encoding="utf-8") as f:
        for seg in result["segments"]:
            start = int(seg["start"])
            end = int(seg["end"])
            sm, ss = divmod(start, 60)
            em, es = divmod(end, 60)

            f.write(
                f"[{sm:02d}:{ss:02d} - {em:02d}:{es:02d}] "
                f"{seg['text'].strip()}\n"
            )

def process_local_files():
    for file in os.listdir(BASE_FOLDER):
        path = os.path.join(BASE_FOLDER, file)

        if not os.path.isfile(path):
            continue

        if file.lower().endswith(VIDEO_EXTENSIONS):
            audio_path = convert_video_to_audio(path)
            transcribe_audio(audio_path)

        elif file.lower().endswith(AUDIO_EXTENSIONS):
            transcribe_audio(path)

def extract_video_id(input_value):
    if len(input_value) == 11 and "http" not in input_value:
        return input_value

    parsed = urlparse(input_value)
    return parse_qs(parsed.query).get("v", [None])[0]


def format_time(seconds):
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"


def transcribe_youtube(input_value):
    video_id = extract_video_id(input_value)
    if not video_id:
        print("Invalid YouTube input")
        return

    output_file = os.path.join(BASE_FOLDER, f"{video_id}.txt")

    if os.path.exists(output_file):
        print(f"YouTube already transcribed: {video_id}")
        return

    print(f"Fetching YouTube transcript: {video_id}")

    try:
        transcript = YouTubeTranscriptApi().fetch(video_id)
    except Exception as e:
        print(f"Failed to fetch transcript: {e}")
        return

    with open(output_file, "w", encoding="utf-8") as f:
        for item in transcript:
            start = item.start
            end = start + item.duration
            text = item.text.replace("\n", " ")

            f.write(
                f"[{format_time(start)} - {format_time(end)}] {text}\n"
            )

    print("YouTube transcript saved")

def clean_pdf_text(text):
    text = re.sub(
        r"c\d+\.indd\s+Page\s+\d+\s+\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s+(AM|PM)",
        "",
        text,
        flags=re.IGNORECASE
    )

    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if re.fullmatch(r"\d{1,4}", line):
            continue
        if "http://" in line or "https://" in line:
            continue
        if len(line) < 4:
            continue
        lines.append(line)

    text = " ".join(lines)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def process_pdfs():
    for file in os.listdir(BASE_FOLDER):
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(BASE_FOLDER, file)
        output_txt = os.path.splitext(pdf_path)[0] + ".txt"

        if os.path.exists(output_txt):
            print(f"PDF already cleaned: {file}")
            continue

        loader = PyPDFLoader(pdf_path)
        docs = loader.load()

        cleaned_pages = [
            clean_pdf_text(doc.page_content)
            for doc in docs if doc.page_content
        ]

        with open(output_txt, "w", encoding="utf-8") as f:
            f.write("\n\n".join(cleaned_pages))

        print(f"PDF cleaned & saved: {file}")

process_local_files()

for link in YOUTUBE_LINKS:
    transcribe_youtube(link)

process_pdfs()
