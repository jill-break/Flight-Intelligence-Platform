"""
DAG Integrity Tests (CI/CD Compatible)
=========================================
Verifies that all DAG files are syntactically valid Python and
contain the expected task definitions and DAG structures.

These tests do NOT import Airflow — they use AST parsing and
text inspection so they run in any environment (including CI/CD).
"""

import pytest
import ast
import os

# Paths to DAG files
DAG_DIR = os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags')
RAW_INGESTION_PATH = os.path.join(DAG_DIR, 'raw_ingestion_dag.py')
ANALYTICS_PIPELINE_PATH = os.path.join(DAG_DIR, 'flight_analytics_pipeline.py')


def _read_file(path):
    """Read a file with UTF-8 encoding."""
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


class TestDAGIntegrity:
    """Verify DAG files parse without errors and have expected structure."""

    def test_raw_ingestion_dag_parses(self):
        """raw_ingestion_dag.py should be valid Python (no syntax errors)."""
        source = _read_file(RAW_INGESTION_PATH)
        tree = ast.parse(source)
        assert tree is not None

    def test_flight_analytics_dag_parses(self):
        """flight_analytics_pipeline.py should be valid Python."""
        source = _read_file(ANALYTICS_PIPELINE_PATH)
        tree = ast.parse(source)
        assert tree is not None

    def test_raw_ingestion_tasks(self):
        """raw_ingestion_pipeline should define expected task IDs."""
        source = _read_file(RAW_INGESTION_PATH)

        assert "'wait_for_local_file'" in source or '"wait_for_local_file"' in source, \
            "Missing task_id 'wait_for_local_file'"
        assert "'upload_files_to_minio'" in source or '"upload_files_to_minio"' in source, \
            "Missing task_id 'upload_files_to_minio'"

    def test_flight_analytics_tasks(self):
        """flight_analytics_pipeline should define expected task IDs."""
        source = _read_file(ANALYTICS_PIPELINE_PATH)

        expected_tasks = [
            'wait_for_flight_data',
            'validate_with_pandera',
            'process_flight_data_spark',
            'archive_processed_files',
        ]
        for task_id in expected_tasks:
            assert f"'{task_id}'" in source or f'"{task_id}"' in source, \
                f"Missing task_id '{task_id}'"

    def test_flight_analytics_dependencies(self):
        """Verify the >> dependency chain exists in the analytics DAG."""
        source = _read_file(ANALYTICS_PIPELINE_PATH)
        assert '>>' in source, "No >> dependency chain found in analytics DAG"

    def test_no_syntax_errors_all_dags(self):
        """Every .py file in the dags folder should parse as valid Python."""
        dag_files = [f for f in os.listdir(DAG_DIR) if f.endswith('.py')]
        errors = {}

        for dag_file in dag_files:
            filepath = os.path.join(DAG_DIR, dag_file)
            try:
                source = _read_file(filepath)
                ast.parse(source)
            except SyntaxError as e:
                errors[dag_file] = str(e)

        assert len(errors) == 0, f"DAG syntax errors: {errors}"
