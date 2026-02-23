"""
Tests for Airflow DAG integrity — validates DAG structure without
requiring a running Airflow instance.

These tests are automatically SKIPPED if Airflow and its providers
are not properly installed (e.g., in a lightweight CI environment).
They run in environments where the full Airflow stack is available
(e.g., inside the Docker container).
"""
import sys
import os
import pytest
from datetime import timedelta

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _airflow_fully_available():
    """Return True only if Airflow + DagBag + required providers load."""
    try:
        from airflow.models import DagBag  # noqa: F401
        from airflow.operators.python import PythonOperator  # noqa: F401
        return True
    except Exception:
        return False


# Skip entire module if Airflow stack is incomplete
pytestmark = pytest.mark.skipif(
    not _airflow_fully_available(),
    reason="Full Airflow stack is not available — skipping DAG integrity tests",
)


@pytest.fixture(scope='module')
def dag():
    """Import the DAG module and return the DAG object."""
    dags_dir = os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags')
    sys.path.insert(0, os.path.abspath(dags_dir))

    from airflow.models import DagBag
    dagbag = DagBag(
        dag_folder=os.path.abspath(dags_dir),
        include_examples=False,
    )
    dag_obj = dagbag.get_dag('flight_data_ingestion')
    assert dag_obj is not None, (
        f"DAG 'flight_data_ingestion' not found. Import errors: {dagbag.import_errors}"
    )
    return dag_obj


class TestDAGIntegrity:
    """Validate the structure and configuration of the flight ingestion DAG."""

    def test_dag_loaded(self, dag):
        assert dag is not None

    def test_dag_id(self, dag):
        assert dag.dag_id == 'flight_data_ingestion'

    def test_task_count(self, dag):
        assert len(dag.tasks) == 3

    def test_task_ids(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            'wait_for_flight_data',
            'validate_with_pandera',
            'process_flight_data_spark',
        }
        assert task_ids == expected

    def test_task_dependencies(self, dag):
        """wait_for_file >> validate_data >> process_validated_data"""
        wait = dag.get_task('wait_for_flight_data')
        validate = dag.get_task('validate_with_pandera')
        process = dag.get_task('process_flight_data_spark')

        assert 'validate_with_pandera' in [t.task_id for t in wait.downstream_list]
        assert 'process_flight_data_spark' in [t.task_id for t in validate.downstream_list]
        assert len(process.downstream_list) == 0

    def test_default_args(self, dag):
        assert dag.default_args['owner'] == 'senior_de'
        assert dag.default_args['retries'] == 1
        assert dag.default_args['retry_delay'] == timedelta(minutes=5)
