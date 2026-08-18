#!/usr/bin/env python3

import unittest

from tests import _test_log_isolation  # noqa: F401

from config import rag_contract as contract


class _Collection:
    def __init__(self, metadata, embeddings=None):
        self.metadata = dict(metadata)
        self.embeddings = embeddings or []
        self.modified = False

    def count(self):
        return len(self.embeddings)

    def get(self, **_kwargs):
        return {"embeddings": self.embeddings}

    def modify(self, metadata):
        self.metadata = dict(metadata)
        self.modified = True


class _Logger:
    @staticmethod
    def warning(*_args, **_kwargs):
        pass


class RagContractTest(unittest.TestCase):
    def test_reader_rejects_model_drift(self):
        metadata = contract.expected_metadata()
        metadata["embedding_model"] = "different-model"
        collection = _Collection(metadata, [[0.0] * contract.EMBEDDING_DIMENSION])
        with self.assertRaisesRegex(RuntimeError, "embedding_model"):
            contract.validate_reader_contract(collection)

    def test_writer_initializes_dimension_checked_legacy_collection(self):
        collection = _Collection(
            {"hnsw:space": "cosine"},
            [[0.0] * contract.EMBEDDING_DIMENSION],
        )
        contract.initialize_or_validate_writer_contract(collection, _Logger())
        self.assertTrue(collection.modified)
        self.assertEqual(collection.metadata, contract.expected_metadata())

    def test_writer_rejects_legacy_dimension_mismatch(self):
        collection = _Collection({"hnsw:space": "cosine"}, [[0.0] * 8])
        with self.assertRaisesRegex(RuntimeError, "dimension mismatch"):
            contract.initialize_or_validate_writer_contract(collection, _Logger())


if __name__ == "__main__":
    unittest.main()
