# CLAUDE.md

This file provides guidance for Claude Code when working with this repository.

## Project Overview

Lakeflow Community Connectors enable data ingestion from various source systems into Databricks. Built on the Spark Python Data Source API and Spark Declarative Pipeline (SDP).

## Project Structure

```
src/databricks/labs/community_connector/
  interface/             # LakeflowConnect base interface
  sources/               # Source connectors (github/, zendesk/, stripe/, etc.)
    {source}/            # Each connector has: {source}.py, README.md
  libs/                  # Shared utilities (spec_parser.py, utils.py, source_loader.py)
  pipeline/              # SDP orchestration (ingestion_pipeline.py)
  sparkpds/              # PySpark Data Source generic implementation and registry API.
tools/
  community_connector/   # CLI tool to set up and run community connectors in Databricks workspace
  scripts/               # Build tools (merge_python_source.py)
tests/
  unit/
    libs/                # Unit tests for shared libs
    pipeline/            # Unit tests for pipeline
    sources/             # Connector tests and test utilities
      {source}/          # Per-connector test files
      test_suite.py      # Shared test harness
      test_utils.py      # Test utilities
      lakeflow_connect_test_utils.py  # Write-back test utilities
prompts/                 # Templates and guide for AI-assisted development
.claude/skills/          # Claude skill files (development workflow steps)
.claude/agents/          # Claude subagent that handles different phases of connector development
```

## Core Interface

All connectors implement the `LakeflowConnect` class in `src/databricks/labs/community_connector/interface/lakeflow_connect.py`:

```python
class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """Initialize with connection parameters (auth tokens, configs, etc.)"""

    def list_tables(self) -> list[str]:
        """Return names of all tables supported by this connector."""

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the Spark schema for a table."""

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return metadata: primary_keys, cursor_field, ingestion_type (snapshot|cdc|cdc_with_deletes|append)."""

    def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """Yield records as JSON dicts and return the next offset for incremental reads."""

    def read_table_deletes(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """Optional: Yield deleted records for delete synchronization. Only required if ingestion_type is 'cdc_with_deletes'."""
```

## Build & Test Commands

```bash
# Run tests for a specific connector
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v

# Run all unit tests
pytest tests/unit/ -v

# Generate deployable file (temporary workaround)
python tools/scripts/merge_python_source.py {source_name}

# Regenerate all connector merged sources
python tools/scripts/merge_python_source.py all

# Run pylint with CI-equivalent flags (non-test files)
pylint --max-line-length=100 \
  --disable=W,C0114,C0115,R0801,R1705 \
  --ignore-long-lines='^\s*(#|f?".*"|f?'"'"'.*'"'"')$' \
  sources/{source_name}/__init__.py \
  sources/{source_name}/{source_name}.py

# Run pylint for test files (adds C0116 disable)
pylint --max-line-length=100 \
  --disable=W,C0114,C0115,R0801,R1705,C0116 \
  --ignore-long-lines='^\s*(#|f?".*"|f?'"'"'.*'"'"')$' \
  sources/{source_name}/test/test_*.py
```

## Development Workflow

1. **Understand the source** — Gather API specs, auth mechanisms, and schemas using the provided template
2. **Implement the connector** — Implement the `LakeflowConnect` interface methods
3. **Create `__init__.py`** — Each connector needs a package init that exports `LakeflowConnect`
4. **Test & iterate** — Run the standard test suites against a real source system
   - *(Optional)* Implement write-back testing for end-to-end validation (write -> read -> verify cycle)
5. **Run pylint** — Ensure all files pass with CI-equivalent flags before pushing
6. **Generate merged source** — Run `tools/scripts/merge_python_source.py {source_name}`
7. **Generate documentation** — Create user-facing docs using the documentation template

## Implementation Guidelines

- When developing a new connector, only modify `src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py` — do **not** change the library, pipeline, or interface code.
- Shared code (libs, pipeline, interface) should only be updated when explicitly instructed to add new features or improvements to the framework itself.

## Pylint / CI Conventions

The CI runs pylint on all tracked `*.py` files (excluding `_generated_*` files). Common patterns for suppressing false positives:

- **Third-party imports not in root `pyproject.toml`**: Add `# pylint: disable=import-error` on the `from` line. This is needed when a connector depends on packages declared only in its per-source `pyproject.toml` (e.g., `azure-servicebus`, `azure-identity`).
- **Too many instance attributes**: Add `# pylint: disable=too-many-instance-attributes` on the `class` line.
- **Too many arguments / positional arguments**: Add `# pylint: disable=too-many-arguments,too-many-positional-arguments` on the `def` line.
- **Too many locals / branches / statements**: Add `# pylint: disable=too-many-locals` (or `too-many-branches`, `too-many-statements`) on the `def` line.
- **Too many lines in module**: Add `# pylint: disable=too-many-lines` as the first line of the file.
- **Line length**: Max 100 chars. Break long lines using parentheses. The `--ignore-long-lines` flag exempts lines that are entirely comments or string literals.

See `src/databricks/labs/community_connector/sources/github/github.py` and `src/databricks/labs/community_connector/sources/azure_servicebus/azure_servicebus.py` for examples of these patterns.

## Azure Service Bus Connector

The `src/databricks/labs/community_connector/sources/azure_servicebus/` connector supports:
- **Tables**: `queues`, `topics`, `subscriptions`, `queue_messages`, `subscription_messages`, `dead_letter_messages`
- **Auth methods**: Connection string, Azure AD (DefaultAzureCredential), Service Principal, Managed Identity
- **Ingestion types**: `snapshot` for metadata tables, `append` for message tables
- **Session-enabled queues**: Automatically detects and iterates through available sessions
- **Key files**:
  - `azure_servicebus.py` — Main connector implementation
  - `__init__.py` — Package init exporting `LakeflowConnect`
  - `connector_spec.yaml` — Connection parameter specification
  - `test/test_azure_servicebus_lakeflow_connect.py` — Integration tests
  - `test/stress_test.py` — Stress/load tests
  - `setup_test_resources.py` — Script to create test queues/topics in Azure

## Testing Conventions

- Tests use `tests/unit/sources/test_suite.py` via `LakeflowConnectTester`
- Load credentials from `tests/unit/sources/{source_name}/configs/dev_config.json`
- Never mock data - tests connect to real source systems
- Optional write-back testing via `LakeflowConnectTestUtils` in `tests/unit/sources/lakeflow_connect_test_utils.py`

## Key Files to Reference

- `src/databricks/labs/community_connector/interface/lakeflow_connect.py` - Base interface definition
- `src/databricks/labs/community_connector/sources/zendesk/zendesk.py` - Reference implementation
- `src/databricks/labs/community_connector/sources/example/example.py` - Reference implementation
- `src/databricks/labs/community_connector/sources/azure_servicebus/azure_servicebus.py` - Azure Service Bus connector
- `tests/unit/sources/test_suite.py` - Test harness
- `tests/unit/sources/example/test_example_lakeflow_connect.py` - Reference test implementation
- `prompts/README.md` - Development workflow guide (references `.claude/skills/`)
- `prompts/templates/source_api_doc_template.md` - API documentation template
- `prompts/templates/community_connector_doc_template.md` - User documentation template
- `.claude/skills/` - Claude skill files for each development step
- `.claude/agents/` - Claude subagents that handle different phases of connector development


