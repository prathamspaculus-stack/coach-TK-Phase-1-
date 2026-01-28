import os
import re
import subprocess
from youtube_transcript_api import YouTubeTranscriptApi
from langchain_community.document_loaders import PyPDFLoader
import whisper
import hashlib
import mysql.connector
from urllib.parse import urlparse, parse_qs


BASE_FOLDER = r"C:\Users\Administrator\Desktop\Coach TK\Documents"

VIDEO_EXTENSIONS = (".mp4", ".mkv", ".avi", ".mov")
AUDIO_EXTENSIONS = (".mp3", ".wav", ".m4a")

YOUTUBE_LINKS = [
    "3OBREA0u_W4",
]

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "newpassword",
    "database": "coachtk"
}

db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

model = whisper.load_model("base")


# it is generate hash for file path.
def generate_file_hash(file_path):
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

# it is generate hash for text
def generate_text_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# it is check hash is exists in DB or not if yes then give Ture else false
def is_hash_exists(hash_id):
    cursor.execute(
        "SELECT id FROM file_registry WHERE hash_id=%s",
        (hash_id,)
    )
    return cursor.fetchone() is not None

# save hash id in db
def save_hash(hash_id, file_name, file_path, file_type):
    cursor.execute(
        """
        INSERT INTO file_registry (hash_id, file_name, file_path, file_type)
        VALUES (%s, %s, %s, %s)
        """,
        (hash_id, file_name, file_path, file_type)
    )
    db.commit()

#it is create video to audio using ffmpeg
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

    print(f"Converted {os.path.basename(video_path)}")
    return audio_path

# it is use for transcribe for audio if transcribe not. and save hash id save on DB.
def transcribe_audio(audio_path):
    txt_path = os.path.splitext(audio_path)[0] + "_time.txt"

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

    txt_hash = generate_file_hash(txt_path)
    if not is_hash_exists(txt_hash):
        save_hash(
            txt_hash,
            os.path.basename(txt_path),
            txt_path,
            "txt"
        )

# it is work on video and audio and save hash id in DB.
def process_local_files():
    for file in os.listdir(BASE_FOLDER):
        path = os.path.join(BASE_FOLDER, file)

        if not os.path.isfile(path):
            continue

        if file.lower().endswith(VIDEO_EXTENSIONS):
            video_hash = generate_file_hash(path)

            if is_hash_exists(video_hash):
                print(f"Skipped (video already processed): {file}")
                continue

            audio_path = convert_video_to_audio(path)
            transcribe_audio(audio_path)

            save_hash(video_hash, file, path, "video")

        elif file.lower().endswith(AUDIO_EXTENSIONS):
            audio_hash = generate_file_hash(path)

            if is_hash_exists(audio_hash):
                print(f"Skipped (audio already processed): {file}")
                continue

            transcribe_audio(path)
            save_hash(audio_hash, file, path, "audio")



def extract_video_id(input_value):

    # If input is already a YouTube video ID (11 characters)
    # and not a URL, return it directly
    if len(input_value) == 11 and "http" not in input_value:
        return input_value

    # Parse the URL into components
    parsed = urlparse(input_value)

    # Extract the 'v' query parameter (YouTube video ID)
    # Return None if 'v' does not exist
    return parse_qs(parsed.query).get("v", [None])[0]


def format_time(seconds):
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"

# it is work for transcribe youtube video and also save hash in DB.
def transcribe_youtube(input_value):
    video_id = extract_video_id(input_value)
    if not video_id:
        print("Invalid YouTube input")
        return

    yt_hash = generate_text_hash(video_id)

    if is_hash_exists(yt_hash):
        print(f"YouTube already processed: {video_id}")
        return

    print(f"Fetching YouTube transcript: {video_id}")

    try:
        transcript = YouTubeTranscriptApi().fetch(video_id)
    except Exception as e:
        print(f"Failed to fetch transcript: {e}")
        return

    output_file = os.path.join(BASE_FOLDER, f"{video_id}_YT_time.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        for item in transcript:
            start = item.start
            end = start + item.duration
            text = item.text.replace("\n", " ")

            f.write(
                f"[{format_time(start)} - {format_time(end)}] {text}\n"
            )

    save_hash(
        yt_hash,
        f"YouTube-{video_id}",
        input_value,
        "youtube"
    )

    print("YouTube transcript saved")

# it is work for cleaning text
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

# It is work for pdf and save hash
def process_pdfs():
    for file in os.listdir(BASE_FOLDER):
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(BASE_FOLDER, file)
        pdf_hash = generate_file_hash(pdf_path)

        if is_hash_exists(pdf_hash):
            print(f"PDF already processed: {file}")
            continue

        loader = PyPDFLoader(pdf_path)
        docs = loader.load()

        cleaned_pages = [
            clean_pdf_text(doc.page_content)
            for doc in docs if doc.page_content
        ]

        output_txt = os.path.splitext(pdf_path)[0] + "_clean.txt"
        with open(output_txt, "w", encoding="utf-8") as f:
            f.write("\n\n".join(cleaned_pages))

        save_hash(pdf_hash, file, pdf_path, "pdf")
        print(f"PDF cleaned & saved")

process_local_files()

for link in YOUTUBE_LINKS:
    transcribe_youtube(link)

process_pdfs()
