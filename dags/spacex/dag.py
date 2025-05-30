# spacex/dag.py

from os import environ
from dotenv import load_dotenv

from airflow import DAG
from spacex.config import DEFAULT_ARGS
from spacex.tasks import (
    get_ingestion_pipeline_group,
    get_transform_pipeline_group,
)

# Carrega variáveis de ambiente
load_dotenv()
target_env = environ.get("ENVIRONMENT", "dev")
assert target_env in {"dev", "prod"}, f"Invalid target_env: {target_env}"

# Define entidades da API SpaceX
ENTITIES = sorted({
    "history", "launches", "capsules", "cores", "dragons",
    "payloads", "rockets", "ships", "landpads"
})

project_name="spacex"
project_description="🚀 SpaceX ETL pipeline: API -> Postgres -> DBT"
schedule_interval="@hourly"

with DAG(
    dag_id=f"{project_name}__elt__{target_env}",
    default_args=DEFAULT_ARGS,
    description=project_description,
    schedule_interval=schedule_interval,
    catchup=False,
    tags=[project_name, "elt", "dbt"]
) as dag:

    # Cria o grupo de tarefas da pipeline por entidade
    ingestion_name=f"ingestion_pipeline_{project_name}"
    ingestion_pipeline = get_ingestion_pipeline_group(
        pipeline_name=ingestion_name, 
        entities=ENTITIES
    )

    # Cria tarefas DBT encadeadas
    transform_name=f"transform_pipeline_{project_name}"
    transformation_pipeline = get_transform_pipeline_group(
        pipeline_name=transform_name, 
        project_name=project_name
    )

    # Define ordem de execução
    ingestion_pipeline >> transformation_pipeline
