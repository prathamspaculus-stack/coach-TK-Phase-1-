import re
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter

PDF_PATH = r"ZTE book.pdf"
loader = PyPDFLoader(PDF_PATH)
docs = loader.load()

print("Pages loaded:", len(docs))

def clean_pdf_text(text: str) -> str:

    # c01.indd Page 3 09/11/24 7:37 PM
    text = re.sub(
        r"c\d+\.indd\s+Page\s+\d+\s+\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s+(AM|PM)",
        "",
        text,
        flags=re.IGNORECASE
    )

    lines = text.splitlines()
    clean_lines = []

    for line in lines:
        line = line.strip()

        # remove empty lines
        if not line:
            continue

        # remove standalone page numbers
        if re.fullmatch(r"\d{1,4}", line):
            continue

        # remove footer/header text
        if line.upper() in {"ZERO TO ENGINEER"}:
            continue

        # remove numeric table ranges
        if re.fullmatch(r"[\d,]+–[\d,]+", line):
            continue

        # remove table labels
        if line.upper().startswith("TOTAL JOB"):
            continue
        if line.lower().startswith("source:"):
            continue

        # remove URLs
        if "http://" in line or "https://" in line:
            continue

        # remove very short junk lines
        if len(line) < 4:
            continue

        clean_lines.append(line)

    text = " ".join(clean_lines)

    # remove soft hyphen artifacts
    text = text.replace("\u00ad", "")

    # normalize spaces
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip()


for doc in docs:
    doc.page_content = clean_pdf_text(doc.page_content)

splitter = RecursiveCharacterTextSplitter(
    chunk_size=800,
    chunk_overlap=100
)

chunks = splitter.split_documents(docs)

print("Clean chunks:", len(chunks))

with open("book_clean_final.txt", "w", encoding="utf-8") as f:
    for c in chunks:
        f.write(c.page_content + "\n\n")

print("PDF cleaned successfully (headers, page numbers removed)")
