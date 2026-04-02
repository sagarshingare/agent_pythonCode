import pytest
from banking_learning_project import llm_module


def test_llm_module_exists():
    assert hasattr(llm_module, "example")


def test_langchain_qa_skip_if_unavailable():
    if llm_module.OpenAI is None:
        pytest.skip("langchain/OpenAI libs missing")


def test_llama_index_skip_if_unavailable():
    if llm_module.llama_index is None:
        pytest.skip("llama_index missing")


def test_huggingface_skip_if_unavailable():
    if llm_module.pipeline is None:
        pytest.skip("transformers missing")
