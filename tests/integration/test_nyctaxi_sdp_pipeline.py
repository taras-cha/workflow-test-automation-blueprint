import pytest
from datetime import timedelta

try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    from pyspark.sql import SparkSession as SparkSession


@pytest.fixture
def deployed_pipeline_spec(ws, request):
    """Look up the deployed pipeline by name and return its spec.

    The test creates a fresh, isolated pipeline from this spec so that each
    run starts with clean streaming state -- no stale checkpoints or table
    metadata from previous ephemeral schemas.
    """
    pipeline_name = request.config.getoption("--pipeline-name")
    deployed = next(
        (p for p in ws.pipelines.list_pipelines() if p.name == pipeline_name),
        None,
    )
    if deployed is None:
        raise ValueError(f"Pipeline '{pipeline_name}' not found.")

    return ws.pipelines.get(deployed.pipeline_id).spec


@pytest.mark.integration_test
def test_sdp_pipeline(spark, make_schema, ws, deployed_pipeline_spec):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name

    created = ws.pipelines.create(
        name=f"test_{schema_name}",
        libraries=deployed_pipeline_spec.libraries,
        catalog=catalog_name,
        target=schema_name,
        serverless=deployed_pipeline_spec.serverless,
        development=True,
    )
    test_pipeline_id = created.pipeline_id

    try:
        ws.pipelines.start_update(pipeline_id=test_pipeline_id, full_refresh=True)

        result = ws.pipelines.wait_get_pipeline_idle(
            pipeline_id=test_pipeline_id,
            timeout=timedelta(minutes=15),
        )
        assert result.state.name == "IDLE", f"Pipeline ended in state {result.state}"

        latest_update = ws.pipelines.list_updates(test_pipeline_id).updates[0]
        assert latest_update.state.value == "COMPLETED", (
            f"Pipeline update {latest_update.state.value}: check pipeline events"
        )

        trips_df = spark.read.table(f"{catalog_name}.{schema_name}.nyctaxi_trips_raw")
        assert trips_df.count() > 0

        avg_df = spark.read.table(f"{catalog_name}.{schema_name}.avg_distance")
        assert avg_df.count() > 0
    finally:
        ws.pipelines.delete(test_pipeline_id)
