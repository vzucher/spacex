# tasks.py
import logging
# from re import sub # No longer needed as dbt selectors are handled by Cosmos/tags

from airflow.decorators import task, task_group
# Cosmos imports
# from cosmos import DbtTaskGroup # Using individual operators for more control here
from cosmos.config import ProfileConfig, ProjectConfig
# from cosmos.constants import ExecutionMode # Uncomment if using local execution mode
from cosmos.operators import DbtRunOperator, DbtTestOperator


# Project-specific config imports
from .config import (
    DBT_PROJECT_PATH,      # Path to the dbt project directory
    DBT_PROFILES_PATH,     # Path to the profiles.yml directory
    DBT_TARGET_ENV,        # dbt target environment (e.g., 'dev', 'prod')
    DBT_MODEL_PATH,        # Path to dbt models within the project (e.g., "models")
)
from .core.pipeline import SpaceXPipeline
from .core.storage import MinIOStorage, MinIOConfig


def get_entity_pipeline_task(entity: str):
    """
    Wrapper to run the SpaceX pipeline for a given entity (e.g., 'launches').
    Used as an Airflow TaskFlow function.
    """
    @task(task_id=f"run_pipeline_{entity}")
    def run_entity_pipeline_task() -> None:
        pipeline = SpaceXPipeline(entity, MinIOStorage(config=MinIOConfig()))
        pipeline.run()

    return run_entity_pipeline_task

def get_ingestion_pipeline_group(pipeline_name: str, entities: list):
    @task_group(group_id=pipeline_name)
    def ingestion_group():
        for entity in entities:
            get_entity_pipeline_task(entity)()  # <--- call the returned @task function
    return ingestion_group()

# Configuration for Cosmos dbt tasks
# This assumes an Airflow Connection with conn_id 'db_conn' is set up for your dbt database.
# The profile_name 'default' (or adjust to your dbt_project.yml) is used by Cosmos.
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="default",
    target_name=DBT_TARGET_ENV,
    profile_mapping_class_name="cosmos.profiles.PostgresUserPasswordProfileMapping", # Adjust if not using Postgres
    profile_args={"conn_id": "db_conn"}, # Airflow connection ID
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    models_path=DBT_MODEL_PATH, # Relative to DBT_PROJECT_PATH (e.g., "models")
)

def get_transform_pipeline_group(pipeline_name: str, project_name: str): # target_env removed
    """
    Creates an Airflow TaskGroup for running dbt transformations for a specific project (e.g., 'spacex').
    The pipeline runs staging models, then mart models for the project, and finally tests models for that project.
    Uses Cosmos operators for dbt tasks. Assumes dbt models are tagged with 'staging' or 'mart',
    and also with the specific 'project_name'.
    """

    # Operator to run staging models tagged with 'staging' and the project_name
    run_staging_models = DbtRunOperator(
        task_id=f"run_staging_{project_name}_models",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        # operator_args={"install_deps": True}, # Alternative way to pass install_deps
        # install_deps=True, # Set to True for Cosmos to handle 'dbt deps'
        select=[f"tag:staging,tag:{project_name}"],
        # execution_mode=ExecutionMode.LOCAL, # Set if dbt is installed in the Airflow environment
    )

    # Operator to run mart models tagged with 'mart' and the project_name
    run_mart_models = DbtRunOperator(
        task_id=f"run_mart_{project_name}_models",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        # install_deps=True,
        select=[f"tag:mart,tag:{project_name}"],
        # execution_mode=ExecutionMode.LOCAL,
    )

    # Operator to run dbt tests for models tagged with the project_name
    test_project_models = DbtTestOperator(
        task_id=f"test_{project_name}_models",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        # install_deps=True,
        select=[f"tag:{project_name}"], # Test models associated with the current project
        # execution_mode=ExecutionMode.LOCAL,
    )

    @task_group(group_id=pipeline_name)
    def dbt_transform_group():
        run_staging_models >> run_mart_models >> test_project_models

    return dbt_transform_group()
