import pytest
from banking_learning_project import advanced_ai_module


def test_advanced_ai_module_example():
    assert hasattr(advanced_ai_module, "example")


def test_ray_skip_if_unavailable():
    if advanced_ai_module.ray is None:
        pytest.skip("ray not installed")
    assert advanced_ai_module.ray_distributed_task_demo() == 3


def test_vector_db_connections_skip_if_unavailable():
    assert advanced_ai_module.vector_db_connect_faiss() == "faiss available" if advanced_ai_module.faiss is not None else True
    assert advanced_ai_module.vector_db_connect_chroma() == "chromadb available" if advanced_ai_module.chromadb is not None else True
