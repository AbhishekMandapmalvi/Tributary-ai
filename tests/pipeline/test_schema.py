"""Tests for JSON Schema validation of pipeline config."""

import pytest
from tributary.pipeline.schema import validate_schema, CONFIG_SCHEMA


def _minimal_config(**overrides):
    """Return a minimal valid config, with optional overrides."""
    cfg = {
        "source": {"type": "local", "params": {"directory": "./docs"}},
        "chunker": {"strategy": "fixed", "params": {"chunk_size": 500}},
        "embedder": {"provider": "openai"},
        "destination": {"type": "json", "params": {"file_path": "out.jsonl"}},
    }
    cfg.update(overrides)
    return cfg


# --- FR-SCHEMA: valid config ---------------------------------------------------

class TestValidConfig:
    def test_minimal_valid_config(self):
        errors = validate_schema(_minimal_config())
        assert errors == []

    def test_valid_config_with_pipeline_section(self):
        cfg = _minimal_config(pipeline={
            "max_workers": 4,
            "batch_size": 128,
        })
        assert validate_schema(cfg) == []

    def test_valid_config_with_extends(self):
        cfg = _minimal_config(extends="base.yaml")
        assert validate_schema(cfg) == []


# --- FR-SCHEMA: missing required section ----------------------------------------

class TestMissingRequired:
    @pytest.mark.parametrize("section", ["source", "chunker", "embedder", "destination"])
    def test_missing_required_section(self, section):
        cfg = _minimal_config()
        del cfg[section]
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        # Error should mention the missing section
        assert any(section in e for e in errors), f"Expected '{section}' in errors: {errors}"

    def test_missing_required_key_in_source(self):
        cfg = _minimal_config(source={"params": {}})
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        assert any("type" in e for e in errors)

    def test_missing_required_key_in_chunker(self):
        cfg = _minimal_config(chunker={"params": {}})
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        assert any("strategy" in e for e in errors)

    def test_missing_required_key_in_embedder(self):
        cfg = _minimal_config(embedder={})
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        assert any("provider" in e for e in errors)

    def test_missing_required_key_in_destination(self):
        cfg = _minimal_config(destination={"params": {}})
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        # oneOf failure mentions 'type' requirement
        assert len(errors) > 0


# --- FR-SCHEMA: wrong types -----------------------------------------------------

class TestWrongTypes:
    def test_source_type_is_integer(self):
        cfg = _minimal_config(source={"type": 42})
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        assert any("type" in e.lower() or "string" in e.lower() for e in errors)

    def test_chunker_strategy_is_list(self):
        cfg = _minimal_config(chunker={"strategy": ["a", "b"]})
        errors = validate_schema(cfg)
        assert len(errors) >= 1

    def test_source_is_string(self):
        cfg = _minimal_config(source="not_a_dict")
        errors = validate_schema(cfg)
        assert len(errors) >= 1

    def test_pipeline_max_workers_is_string(self):
        cfg = _minimal_config(pipeline={"max_workers": "four"})
        errors = validate_schema(cfg)
        assert len(errors) >= 1
        assert any("max_workers" in e or "integer" in e for e in errors)


# --- FR-SCHEMA: multi-destination -----------------------------------------------

class TestMultiDestination:
    def test_destination_as_list(self):
        cfg = _minimal_config(destination=[
            {"type": "json", "params": {"file_path": "a.jsonl"}},
            {"type": "json", "params": {"file_path": "b.jsonl"}},
        ])
        assert validate_schema(cfg) == []

    def test_destination_list_missing_type(self):
        cfg = _minimal_config(destination=[
            {"type": "json"},
            {"params": {"file_path": "b.jsonl"}},  # missing type
        ])
        errors = validate_schema(cfg)
        assert len(errors) >= 1


# --- FR-SCHEMA: extra unknown fields (permissive) ------------------------------

class TestAdditionalProperties:
    def test_extra_top_level_field(self):
        cfg = _minimal_config(custom_field="hello")
        assert validate_schema(cfg) == []

    def test_extra_field_in_source(self):
        cfg = _minimal_config(source={"type": "local", "params": {}, "extra": True})
        assert validate_schema(cfg) == []

    def test_extra_field_in_pipeline(self):
        cfg = _minimal_config(pipeline={"max_workers": 2, "custom_option": "yes"})
        assert validate_schema(cfg) == []


# --- FR-SCHEMA: pipeline optional properties validate types ---------------------

class TestPipelineProperties:
    @pytest.mark.parametrize("prop", [
        "max_workers", "batch_size", "max_concurrent_embeds",
        "queue_size", "checkpoint_interval",
    ])
    def test_integer_properties_accept_int(self, prop):
        cfg = _minimal_config(pipeline={prop: 10})
        assert validate_schema(cfg) == []

    @pytest.mark.parametrize("prop", [
        "max_workers", "batch_size", "max_concurrent_embeds",
        "queue_size", "checkpoint_interval",
    ])
    def test_integer_properties_reject_string(self, prop):
        cfg = _minimal_config(pipeline={prop: "not_an_int"})
        errors = validate_schema(cfg)
        assert len(errors) >= 1

    @pytest.mark.parametrize("prop", [
        "state_store", "retry_policy", "dead_letter_queue",
        "adaptive_batching", "webhook",
    ])
    def test_object_properties_accept_dict(self, prop):
        cfg = _minimal_config(pipeline={prop: {"key": "value"}})
        assert validate_schema(cfg) == []

    @pytest.mark.parametrize("prop", [
        "state_store", "retry_policy", "dead_letter_queue",
        "adaptive_batching", "webhook",
    ])
    def test_object_properties_reject_string(self, prop):
        cfg = _minimal_config(pipeline={prop: "not_an_object"})
        errors = validate_schema(cfg)
        assert len(errors) >= 1


# --- FR-SCHEMA: CONFIG_SCHEMA is a valid JSON Schema ---------------------------

class TestSchemaItself:
    def test_schema_has_required_sections(self):
        assert "required" in CONFIG_SCHEMA
        assert set(CONFIG_SCHEMA["required"]) == {"source", "chunker", "embedder", "destination"}

    def test_schema_is_valid_json_schema(self):
        from jsonschema import Draft7Validator
        Draft7Validator.check_schema(CONFIG_SCHEMA)
