"""Load html from files, clean up, split, ingest into Weaviate."""
import logging
import os
import re

import weaviate
from bs4 import BeautifulSoup, SoupStrainer
from langchain.document_loaders import RecursiveUrlLoader, SitemapLoader
from langchain.indexes import SQLRecordManager, index
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.utils.html import PREFIXES_TO_IGNORE_REGEX, SUFFIXES_TO_IGNORE_REGEX
from langchain_community.vectorstores import Weaviate
from langchain_core.embeddings import Embeddings
from langchain_openai import OpenAIEmbeddings

from langchain.docstore.document import Document
import tqdm

import os
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


WEAVIATE_DOCS_INDEX_NAME = "LangChain_Combined_Docs_OpenAI_text_embedding_3_small"

def get_embeddings_model() -> Embeddings:
    return OpenAIEmbeddings(model="text-embedding-3-small", chunk_size=200)

def simple_extractor(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return re.sub(r"\n\n+", "\n\n", soup.text).strip()

def load_readthedocs_docs():
    return RecursiveUrlLoader(
        url="https://slicer.readthedocs.io/en/latest/",
        max_depth=3,
        extractor=rtd_extractor,
        prevent_outside=True,
        use_async=True,
        timeout=600,
        # Updated regex to match relative links and include them properly for recursion
        link_regex=(
            r'href=["\'](?!http)(?!#)(?!mailto)(?!javascript)([^"\']*?\.html)["\']'
        ),
        check_response_status=True,
    ).load()

def rtd_extractor(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    content = soup.find('div', class_='document')
    if content:
        return re.sub(r'\n\n+', '\n\n', content.get_text()).strip()
    return ''  # Return empty string if the content area is not found


def parse_md_file(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:  # Ignore errors
        lines = file.readlines()
    
    # Return None if the file is empty
    if not lines:
        print("This filepath is blank: " + str(file_path))
        return None

    title = lines[0].strip().lstrip('#').strip() if lines and lines[0].startswith('#') else "No title"
    content = ''.join(lines[:-1])  # Exclude the last line from content

    # Extract URL from the last line using regex
    url_pattern = r'\]\((https?://[^\)]+)\)'
    last_line = lines[-1].strip() if lines else ''
    match = re.search(url_pattern, last_line)
    source_url = match.group(1) if match else "URL not found"
    
    metadata = {
        'source': source_url,
        'title': title,
        'language': 'en'
    }
    
    return Document(page_content=content, metadata=metadata)


def process_directory(base_path):
    documents = []
    for root, dirs, files in os.walk(base_path):
        for file in tqdm.tqdm(files):
            if file.endswith('.md'):
                file_path = os.path.join(root, file)
                document = parse_md_file(file_path)
                if document is not None:
                    documents.append(document)
    return documents


def ingest_docs():
    WEAVIATE_URL = os.environ["WEAVIATE_URL"]
    WEAVIATE_API_KEY = os.environ["WEAVIATE_API_KEY"]
    RECORD_MANAGER_DB_URL = os.environ["RECORD_MANAGER_DB_URL"]

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=4000, chunk_overlap=200)
    embedding = get_embeddings_model()

    client = weaviate.Client(
        url=WEAVIATE_URL,
        auth_client_secret=weaviate.AuthApiKey(api_key=WEAVIATE_API_KEY),
    )
    vectorstore = Weaviate(
        client=client,
        index_name=WEAVIATE_DOCS_INDEX_NAME,
        text_key="text",
        embedding=embedding,
        by_text=False,
        attributes=["source", "title"],
    )

    record_manager = SQLRecordManager(
        f"weaviate/{WEAVIATE_DOCS_INDEX_NAME}", db_url=RECORD_MANAGER_DB_URL
    )
    record_manager.create_schema()

    docs_from_readthedocs = load_readthedocs_docs()
    logger.info(f"Loaded {len(docs_from_readthedocs)} docs from ReadTheDocs")

    # Path to the base directory containing all the markdown files
    discourse_path = r"C:\repos\chat-langchain\_scripts\archive\rendered-topics"
    discourse_docs = process_directory(discourse_path)

    docs_transformed = text_splitter.split_documents(docs_from_readthedocs + discourse_docs)
    docs_transformed = [doc for doc in docs_transformed if len(doc.page_content) > 10]

    # We try to return 'source' and 'title' metadata when querying vector store and
    # Weaviate will error at query time if one of the attributes is missing from a
    # retrieved document.
    for doc in docs_transformed:
        if "source" not in doc.metadata:
            doc.metadata["source"] = ""
        if "title" not in doc.metadata:
            doc.metadata["title"] = ""

    indexing_stats = index(
        docs_transformed,
        record_manager,
        vectorstore,
        cleanup="full",
        source_id_key="source",
        force_update=(os.environ.get("FORCE_UPDATE") or "false").lower() == "true",
    )

    logger.info(f"Indexing stats: {indexing_stats}")
    num_vecs = client.query.aggregate(WEAVIATE_DOCS_INDEX_NAME).with_meta_count().do()
    logger.info(
        f"LangChain now has this many vectors: {num_vecs}",
    )

if __name__ == "__main__":
    ingest_docs()
