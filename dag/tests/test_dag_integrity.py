import logging
import os

import pytest
from airflow.models.dagbag import DagBag

# Define the root path of the DAGs
DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "src"
)


def test_dags_load_with_no_errors():
    """
    Test that all DAGs in the DAGs folder load with no errors.
    """
    logging.info(f"Loading DAGs from: {DAGS_FOLDER}")
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

    # Check for import errors
    assert len(dag_bag.import_errors) == 0, (
        f"DAG import errors found: {dag_bag.import_errors}"
    )

    # Optional: Verify that at least one DAG was loaded
    assert len(dag_bag.dags) > 0, "No DAGs found!"
