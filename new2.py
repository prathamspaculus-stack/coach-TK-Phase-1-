import os
import json
import re
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.output_parsers import StrOutputParser


TXT_FILE_PATH = r"C:\Users\Administrator\Desktop\Coach TK\Documents\lyditj_-_module_2_-_strategies_for_promotion_v1 (360p).txt"
SOURCE_TYPE = "Youtube"
REFERENCE_LINK = "https://www.youtube.com/watch?v=k-JJm2iIh98"

load_dotenv()


# ---------------- HELPERS ----------------
def safe_json_load(text: str):
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise ValueError("LLM did not return JSON")
    return json.loads(match.group())


def combine_timestamps(first_ts, last_ts):
    if not first_ts or not last_ts:
        return None
    first_ts = first_ts.strip("[]")
    last_ts = last_ts.strip("[]")
    return f"{first_ts.split(' - ')[0]} - {last_ts.split(' - ')[1]}"


def remove_timestamps(text: str) -> str:
    return re.sub(
        r"\[\d{2}:\d{2}\s*-\s*\d{2}:\d{2}\]",
        "",
        text
    ).strip()


# ---------------- LLM SETUP ----------------
model = ChatGroq(
    model="llama-3.1-8b-instant",
    temperature=0
)

prompt = PromptTemplate(
    template="""
You are an expert content analyst.

The input text may contain timestamps in this exact format:
[MM:SS - MM:SS]

TASK:
1. REMOVE all timestamps from the text.
2. EXTRACT the FIRST timestamp EXACTLY as it appears.
3. EXTRACT the LAST timestamp EXACTLY as it appears.

IMPORTANT:
- Do NOT summarize, explain, or rewrite the text.
- Preserve original wording.
- Return ONLY valid JSON.

RULES:
- domain must be ONE of: Leadership, Mindset, IT, Strategy
- topic must be 1–3 short words
- content_type must be ONE of: Framework, Example, Story, Advice
- If no timestamp exists, timestamp must be null

FIELDS:
- domain
- topic
- content_type
- first_timestamp
- last_timestamp
- cleaned_text

TEXT:
{text}
""",
    input_variables=["text"]
)

chain = prompt | model | StrOutputParser()


# ---------------- MAIN ----------------
if not os.path.exists(TXT_FILE_PATH):
    print("TXT file not found")
    exit()

json_path = TXT_FILE_PATH.replace(".txt", ".json")

if os.path.exists(json_path):
    print("Skipped (JSON already created)")
    exit()

print(f"Processing TXT → JSON: {os.path.basename(TXT_FILE_PATH)}")

loader = TextLoader(TXT_FILE_PATH, encoding="utf-8")
docs = loader.load()

if not docs:
    print("Empty file")
    exit()

full_text = docs[0].page_content

splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200
)

chunks = splitter.split_text(full_text)
processed_chunks = []

for i, chunk in enumerate(chunks, start=1):
    try:
        raw = chain.invoke({"text": chunk})
        metadata = safe_json_load(raw)
    except Exception:
        print(f"Chunk {i} skipped (LLM error)")
        continue

    metadata["timestamp"] = combine_timestamps(
        metadata.get("first_timestamp"),
        metadata.get("last_timestamp")
    )

    metadata.pop("first_timestamp", None)
    metadata.pop("last_timestamp", None)

    metadata["reference_link"] = REFERENCE_LINK
    metadata["source_type"] = SOURCE_TYPE

    cleaned_text = remove_timestamps(
        metadata.pop("cleaned_text", chunk)
    )

    processed_chunks.append({
        "chunk_id": f"chunk_{i}",
        "text": cleaned_text,
        "metadata": metadata
    })

if not processed_chunks:
    print("No valid chunks created")
    exit()

with open(json_path, "w", encoding="utf-8") as f:
    json.dump(processed_chunks, f, indent=2, ensure_ascii=False)

print(f"JSON created successfully: {os.path.basename(json_path)}")
