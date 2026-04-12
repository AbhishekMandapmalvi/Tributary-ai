"""JSON Schema validation for Tributary pipeline YAML config."""

# FR-SCHEMA: JSON Schema definition and validation for pipeline config

_DESTINATION_OBJECT = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "params": {"type": "object"},
    },
    "required": ["type"],
}

_QUEUE_OBJECT = {
    "type": "object",
    "properties": {
        "backend": {
            "type": "string",
            "enum": ["redis", "sqs", "rabbitmq", "pubsub", "servicebus", "kafka"],
        },
        "params": {"type": "object"},
    },
    "required": ["backend"],
}

CONFIG_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "source": {
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "params": {"type": "object"},
            },
            "required": ["type"],
        },
        "chunker": {
            "type": "object",
            "properties": {
                "strategy": {"type": "string"},
                "params": {"type": "object"},
                "routing": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "strategy": {"type": "string"},
                            "params": {"type": "object"},
                        },
                    },
                },
            },
            "required": ["strategy"],
        },
        "embedder": {
            "type": "object",
            "properties": {
                "provider": {"type": "string"},
                "params": {"type": "object"},
            },
            "required": ["provider"],
        },
        "destination": {
            "oneOf": [
                _DESTINATION_OBJECT,
                {
                    "type": "array",
                    "items": _DESTINATION_OBJECT,
                },
            ],
        },
        "pipeline": {
            "type": "object",
            "properties": {
                "max_workers": {"type": "integer"},
                "batch_size": {"type": "integer"},
                "max_concurrent_embeds": {"type": "integer"},
                "queue_size": {"type": "integer"},
                "checkpoint_interval": {"type": "integer"},
                "state_store": {"type": "object"},
                "retry_policy": {"type": "object"},
                "dead_letter_queue": {"type": "object"},
                "adaptive_batching": {"type": "object"},
                "webhook": {"type": "object"},
            },
        },
        "distributed": {
            "type": "object",
            "properties": {
                "document_queue": _QUEUE_OBJECT,
                "chunk_queue": _QUEUE_OBJECT,
                "extractor": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["text", "markdown", "html", "csv", "json", "pdf"],
                        },
                        "params": {"type": "object"},
                    },
                    "required": ["type"],
                },
                "n_extraction_workers": {"type": "integer", "minimum": 1},
                "n_embedding_workers": {"type": "integer", "minimum": 1},
                "poll_timeout": {"type": "number", "minimum": 0},
            },
            "required": ["document_queue", "chunk_queue"],
        },
        "extends": {"type": "string"},
    },
    "required": ["source", "chunker", "embedder", "destination"],
}


def validate_schema(config: dict) -> list[str]:
    """Validate *config* against CONFIG_SCHEMA.

    Returns a list of human-readable error strings.
    An empty list means the config is valid.
    """
    try:
        from jsonschema import validate, ValidationError
    except ImportError:
        return ["Install jsonschema: pip install jsonschema"]

    try:
        validate(instance=config, schema=CONFIG_SCHEMA)
    except ValidationError:
        # Collect all errors for a friendlier report
        from jsonschema import Draft7Validator

        validator = Draft7Validator(CONFIG_SCHEMA)
        errors: list[str] = []
        for error in sorted(validator.iter_errors(config), key=lambda e: list(e.absolute_path)):
            path = ".".join(str(p) for p in error.absolute_path) or "(root)"
            errors.append(f"{path}: {error.message}")
        return errors

    return []
