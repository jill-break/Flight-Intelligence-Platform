"""
DAG Integrity Tests
=====================
Verifies that all DAGs load without import errors and have
the expected task structure. Run in CI/CD to catch broken DAGs early.

NOTE: These tests require a full Apache Airflow installation.
      They are automatically skipped in environments without Airflow.
"""

import pytest
import os
import sys

# Add project root to path so imports work like they do in Airflow
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Skip this entire module if Airflow is not installed (e.g. in CI/CD)
airflow = pytest.importorskip("airflow", reason="Airflow not installed — skipping DAG integrity tests")


class TestDAGIntegrity:
    """Verify DAG files parse without errors."""

    def test_raw_ingestion_dag_loads(self):
        """raw_ingestion_dag.py should load without import errors."""
        from airflow.models import DagBag
        dag_bag = DagBag(
            dag_folder=os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'),
            include_examples=False,
        )

        assert 'raw_ingestion_pipeline' in dag_bag.dags, \
            f"raw_ingestion_pipeline not found. Errors: {dag_bag.import_errors}"
        assert dag_bag.import_errors.get(
            os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags', 'raw_ingestion_dag.py')
        ) is None

    def test_flight_analytics_dag_loads(self):
        """flight_analytics_pipeline.py should load without import errors."""
        from airflow.models import DagBag
        dag_bag = DagBag(
            dag_folder=os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'),
            include_examples=False,
        )

        assert 'flight_analytics_pipeline' in dag_bag.dags, \
            f"flight_analytics_pipeline not found. Errors: {dag_bag.import_errors}"

    def test_raw_ingestion_tasks(self):
        """raw_ingestion_pipeline should have expected tasks."""
        from airflow.models import DagBag
        dag_bag = DagBag(
            dag_folder=os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'),
            include_examples=False,
        )

        dag = dag_bag.dags['raw_ingestion_pipeline']
        task_ids = [t.task_id for t in dag.tasks]

        assert 'wait_for_local_file' in task_ids
        assert 'upload_files_to_minio' in task_ids

    def test_flight_analytics_tasks(self):
        """flight_analytics_pipeline should have expected tasks."""
        from airflow.models import DagBag
        dag_bag = DagBag(
            dag_folder=os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'),
            include_examples=False,
        )

        dag = dag_bag.dags['flight_analytics_pipeline']
        task_ids = [t.task_id for t in dag.tasks]

        assert 'wait_for_flight_data' in task_ids
        assert 'validate_with_pandera' in task_ids
        assert 'process_flight_data_spark' in task_ids
        assert 'archive_processed_files' in task_ids

    def test_flight_analytics_dependencies(self):
        """Verify the task dependency chain is correct."""
        from airflow.models import DagBag
        dag_bag = DagBag(
            dag_folder=os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'),
            include_examples=False,
        )

        dag = dag_bag.dags['flight_analytics_pipeline']
        tasks = {t.task_id: t for t in dag.tasks}

        # validate depends on wait
        assert 'wait_for_flight_data' in [t.task_id for t in tasks['validate_with_pandera'].upstream_list]
        # spark depends on validate
        assert 'validate_with_pandera' in [t.task_id for t in tasks['process_flight_data_spark'].upstream_list]
        # archive depends on spark
        assert 'process_flight_data_spark' in [t.task_id for t in tasks['archive_processed_files'].upstream_list]

    def test_no_import_errors(self):
        """No DAG file should have import errors."""
        from airflow.models import DagBag
        dag_bag = DagBag(
            dag_folder=os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'),
            include_examples=False,
        )

        assert len(dag_bag.import_errors) == 0, \
            f"DAG import errors: {dag_bag.import_errors}"
