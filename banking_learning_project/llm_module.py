"""LLM + vector search integration examples (LangChain, LlamaIndex, Hugging Face, OpenAI)."""

from typing import List

try:
    from langchain.llms import OpenAI
    from langchain.chains import RetrievalQA
    from langchain.vectorstores import FAISS
    from langchain.embeddings import OpenAIEmbeddings
except ImportError:
    OpenAI = None
    RetrievalQA = None
    FAISS = None
    OpenAIEmbeddings = None

try:
    import llama_index
except ImportError:
    llama_index = None

try:
    from transformers import pipeline
except ImportError:
    pipeline = None


def langchain_qa_descriptor(api_key: str, docs: List[str]):
    """Minimal LangChain QA setup (requires OpenAI key and libs)."""
    if OpenAI is None or OpenAIEmbeddings is None or FAISS is None:
        raise ImportError("langchain dependencies not installed")

    embeddings = OpenAIEmbeddings(openai_api_key=api_key)
    vector_store = FAISS.from_texts(docs, embeddings)
    retriever = vector_store.as_retriever()
    llm = OpenAI(openai_api_key=api_key)
    qa = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever)
    return qa


def llama_index_query(docs: List[str], query: str):
    """Minimal LlamaIndex retrieval; if not available returns informative value."""
    if llama_index is None:
        raise ImportError("llama_index not installed")

    from llama_index import GPTSimpleVectorIndex, SimpleDirectoryReader

    index = GPTSimpleVectorIndex.from_documents([llama_index.Document(t) for t in docs])
    return index.query(query)


def huggingface_generation(model_name: str, prompt: str):
    """Call Hugging Face text generation pipeline (if available)."""
    if pipeline is None:
        raise ImportError("transformers not installed")
    generator = pipeline("text-generation", model=model_name)
    return generator(prompt, max_length=64)


def example():
    print("--- LLM MODULE ---")
    print("LangChain + LlamaIndex + HF/OpenAI paths are ready; this requires runtime credentials and libraries.")
