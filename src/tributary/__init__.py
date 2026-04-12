"""Tributary — a lightweight, high-concurrency data ingestion and chunking
pipeline for RAG systems.

Run single-process for small jobs, scale horizontally across machines via
the distributed layer (see ``tributary.workers``). Both modes share the
same sources, extractors, chunkers, embedders, and destinations.
"""

__version__ = "0.2.0"
