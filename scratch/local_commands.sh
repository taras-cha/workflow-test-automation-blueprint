databricks auth login -p adb-984752964297111 


uv sync --only-group unit-tests
uv run python -m pytest -m unit_test





#export DATABRICKS_CLUSTER_ID=
export DATABRICKS_WAREHOUSE_ID=

export DATABRICKS_HOST=https://adb-984752964297111.11.azuredatabricks.net/
export DATABRICKS_SERVERLESS_COMPUTE_ID=auto

uv sync --only-group integration-tests

uv run python -m pytest -rsx -m integration_test --job-name="[dev taras_chaikovskyi] workflow_test_automation_blueprint_job"