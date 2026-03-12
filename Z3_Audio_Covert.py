import whisper
import os

# Load Whisper model
model = whisper.load_model("base")

BASE_PATH = r"C:\Users\Administrator\Desktop\pratham\project3 video"

for i in range(0, 31):
    audio_file = os.path.join(BASE_PATH, f"module{i}_audio.m4a")
    txt_file = os.path.join(BASE_PATH, f"module_time{i}.txt")

    if not os.path.exists(audio_file):
        print(f"File not found: audio{i}.m4a")
        continue

    print(f"Transcribing: audio{i}.m4a")

    result = model.transcribe(audio_file)

    with open(txt_file, "w", encoding="utf-8") as f:
        for segment in result["segments"]:
            start = segment["start"]
            end = segment["end"]
            text = segment["text"].strip()

            start_min, start_sec = divmod(int(start), 60)
            end_min, end_sec = divmod(int(end), 60)

            f.write(
                f"[{start_min:02d}:{start_sec:02d} - "
                f"{end_min:02d}:{end_sec:02d}] {text}\n"
            )

print("All available audios converted successfully")



# import whisper 
# model = whisper.load_model("base")
# audio_file_path = "module0_audio.m4a" 
# result = model.transcribe(audio_file_path) 
# with open("module0.txt", "w") as f: 
#     f.write(result["text"])

