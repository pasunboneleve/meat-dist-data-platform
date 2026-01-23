import os
import sys
from unittest.mock import patch

import pytest

# Add dag/src to path so we can import utils
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


@pytest.mark.skip(reason="This test is currently broken and needs a fix")
def test_dag_import():
    """Verify that the DAG imports correctly and is structured properly."""
    # Patch env vars required by the DAG
    with patch.dict(
        os.environ,
        {
            "BRONZE_BUCKET": "test-bronze-bucket",
            "SILVER_BUCKET": "test-silver-bucket",
            "SYNTHETIC_MEAT_URL": "http://test-url",
            "GOLD_BUCKET": "test-gold-bucket",  # used in assets.py
            "GCP_PROJECT_ID": "test-project",
            "DATAPROC_REGION": "us-central1",
            "CATALOG_NAME": "test_catalog",
            "DATAPROC_BATCH_SERVICE_ACCOUNT": "test-sa@project.iam.gserviceaccount.com",
            "DB_NAME": "test_db",
        },
    ):
        from src.bronze_to_silver_dv2 import bronze_to_silver_dv2

        # Instantiate the DAG
        dag = bronze_to_silver_dv2()

        assert dag is not None
        assert dag.dag_id == "bronze_to_silver_dv2"
        assert len(dag.tasks) > 0

        # Check specific tasks exist
        task_ids = set(dag.task_ids)
        assert "get_config" in task_ids
        assert "submit_spark_transform" in task_ids
        assert "verify_silver" in task_ids
        assert "mark_asset_produced" in task_ids
