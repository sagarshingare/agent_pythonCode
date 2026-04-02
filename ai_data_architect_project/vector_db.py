"""Vector DB abstractions (FAISS/Chroma) for AI Data Architect project."""

from typing import List

try:
    import faiss
except ImportError:
    faiss = None

try:
    import chromadb
except ImportError:
    chromadb = None


def create_faiss_index(embeddings):
    """Simple FAISS index builder placeholder."""
    if faiss is None:
        raise ImportError("faiss not installed")
    dim = len(embeddings[0])
    idx = faiss.IndexFlatL2(dim)
    import numpy as np
    idx.add(np.array(embeddings, dtype='float32'))
    return idx


def create_chroma_client():
    if chromadb is None:
        raise ImportError("chromadb not installed")
    client = chromadb.Client()
    return client


def example() -> None:
    print("--- AI DATA ARCHITECT VECTOR DB ---")
    try:
        print("FAISS available" if faiss is not None else "FAISS missing")
    except Exception as exc:
        print(exc)
