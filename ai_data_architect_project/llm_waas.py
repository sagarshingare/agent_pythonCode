"""LLM + RAG architecture utilities for AI Data Architect project."""

from typing import Any, List

try:
    from transformers import pipeline
except ImportError:
    pipeline = None


def generate_text(model_name: str, prompt: str, max_length: int = 64) -> Any:
    """Generate text with Hugging Face pipeline (if installed)."""
    if pipeline is None:
        raise ImportError("transformers not installed")
    gen = pipeline("text-generation", model=model_name)
    return gen(prompt, max_length=max_length)


def example() -> None:
    print("--- AI DATA ARCHITECT LLM WAAS ---")
    try:
        result = generate_text("gpt2", "Banking summary:", max_length=20)
        print(result)
    except Exception as exc:
        print("skipped LLM usage", exc)
