#!/usr/bin/env python3
"""Shared vector-store contract for RAG readers and writers."""

import os
from typing import Any, Dict, List

COLLECTION_NAME = str(os.getenv("RAG_COLLECTION_NAME", "strategic_memory_v2")).strip()
EMBEDDING_MODEL = str(os.getenv("RAG_EMBED_MODEL", "mxbai-embed-large:latest")).strip()
EMBEDDING_DIMENSION = max(1, int(os.getenv("RAG_EMBED_DIMENSION", "1024")))
DOCUMENT_FORMAT_VERSION = str(
    os.getenv("RAG_DOCUMENT_FORMAT_VERSION", "narrative_tags_v1")
).strip()
QUERY_FORMAT_VERSION = str(
    os.getenv("RAG_QUERY_FORMAT_VERSION", "natural_audit_v1")
).strip()


def expected_metadata() -> Dict[str, Any]:
    return {
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dimension": EMBEDDING_DIMENSION,
        "document_format_version": DOCUMENT_FORMAT_VERSION,
        "query_format_version": QUERY_FORMAT_VERSION,
    }


def collection_creation_metadata() -> Dict[str, Any]:
    return {"hnsw:space": "cosine", **expected_metadata()}


def metadata_mismatches(metadata: Dict[str, Any]) -> List[str]:
    actual = dict(metadata or {})
    mismatches = []
    for key, expected in expected_metadata().items():
        if str(actual.get(key, "")) != str(expected):
            mismatches.append(f"{key}:actual={actual.get(key)!r}:expected={expected!r}")
    return mismatches


def sample_embedding_dimension(collection) -> int:
    sample = collection.get(limit=1, include=["embeddings"])
    embeddings = sample.get("embeddings")
    if embeddings is None or len(embeddings) == 0:
        return 0
    vector = embeddings[0]
    return len(vector) if vector is not None else 0


def initialize_or_validate_writer_contract(collection, logger) -> None:
    metadata = dict(getattr(collection, "metadata", None) or {})
    contract_keys = set(expected_metadata())
    legacy_missing = not any(key in metadata for key in contract_keys)
    count = int(collection.count())
    if legacy_missing:
        observed_dimension = sample_embedding_dimension(collection) if count else EMBEDDING_DIMENSION
        if observed_dimension not in (0, EMBEDDING_DIMENSION):
            raise RuntimeError(
                f"legacy RAG dimension mismatch: {observed_dimension} != {EMBEDDING_DIMENSION}"
            )
        collection.modify(metadata=expected_metadata())
        logger.warning(
            f"[RAG] initialized vector contract on legacy collection count={count}"
        )
        metadata = expected_metadata()
    mismatches = metadata_mismatches(metadata)
    if mismatches:
        raise RuntimeError("RAG vector contract mismatch: " + "; ".join(mismatches))
    observed_dimension = sample_embedding_dimension(collection) if count else 0
    if observed_dimension not in (0, EMBEDDING_DIMENSION):
        raise RuntimeError(
            f"RAG vector dimension mismatch: {observed_dimension} != {EMBEDDING_DIMENSION}"
        )


def validate_reader_contract(collection) -> None:
    mismatches = metadata_mismatches(dict(getattr(collection, "metadata", None) or {}))
    if mismatches:
        raise RuntimeError("RAG vector contract mismatch: " + "; ".join(mismatches))
    observed_dimension = sample_embedding_dimension(collection)
    if observed_dimension not in (0, EMBEDDING_DIMENSION):
        raise RuntimeError(
            f"RAG vector dimension mismatch: {observed_dimension} != {EMBEDDING_DIMENSION}"
        )
