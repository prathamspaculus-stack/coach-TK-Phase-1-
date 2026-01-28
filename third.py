import os
import json
import hashlib
import mysql.connector
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores.utils import filter_complex_metadata

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FOLDER = r"C:\Users\Administrator\Desktop\Coach TK\Documents"
CHROMA_DIR = os.path.join(BASE_DIR, "chroma_db")
COLLECTION_NAME = "podcast_chunks"

os.makedirs(CHROMA_DIR, exist_ok=True)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "newpassword",
    "database": "coachtk"
}

load_dotenv()
db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def text_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def is_hash_exists(hash_id):
    cursor.execute("SELECT id FROM file_registry WHERE hash_id=%s", (hash_id,))
    return cursor.fetchone() is not None

def save_hash(hash_id, name, path):
    cursor.execute(
        "INSERT INTO file_registry (hash_id, file_name, file_path, file_type) VALUES (%s,%s,%s,%s)",
        (hash_id, name, path, "embedding_done")
    )
    db.commit()

embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2"
)

vectorstore = Chroma(
    collection_name=COLLECTION_NAME,
    persist_directory=CHROMA_DIR,
    embedding_function=embeddings
)

print("Using Chroma (DuckDB/Parquet) at:", CHROMA_DIR)

for file in os.listdir(JSON_FOLDER):
    if not file.endswith(".json"):
        continue

    path = os.path.join(JSON_FOLDER, file)
    f_hash = file_hash(path)

    if is_hash_exists(f_hash):
        print(f"Skipped: {file}")
        continue

    print(f"Processing: {file}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    docs = []
    for item in data:
        text = item.get("text", "").strip()
        if text:
            docs.append(Document(page_content=text))

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=100
    )

    chunks = splitter.split_documents(docs)
    chunks = filter_complex_metadata(chunks)

    new_docs = []
    for c in chunks:
        c_hash = text_hash(c.page_content)
        if not is_hash_exists(c_hash):
            c.metadata["chunk_hash"] = c_hash
            new_docs.append(c)

    if new_docs:
        vectorstore.add_documents(new_docs)
        vectorstore.persist()

    save_hash(f_hash, file, path)

print("ALL FILES PROCESSED SAFELY")
