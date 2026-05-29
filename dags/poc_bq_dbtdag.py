"""
PoC DAG (옵션 3): Cosmos DbtDag — 가장 단순한 패턴.

DbtDag 는 dbt 프로젝트 자체가 하나의 DAG.
- with DAG(...) 블록 없음
- TaskGroup 도 없음
- 모델별 task 가 DAG 의 최상위에 평탄하게 펼쳐짐

옵션 4 (DbtTaskGroup, poc_bq_dbt_run.py) 와 비교 학습용.
관련 노트: 애슬론/PoC/03_bq_dbt_run_in_composer.md
"""
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt_projects/dbt_test"

profile_config = ProfileConfig(
    profile_name="dbt_test",
    target_name="composer",
    profiles_yml_filepath=Path(f"{DBT_PROJECT_PATH}/profiles.yml"),
)

# ─────────────────────────────────────────────────────────────────────────────
# DbtDag — DAG 자체가 dbt 프로젝트
# ─────────────────────────────────────────────────────────────────────────────
dbt_dag = DbtDag(
    # DAG 메타데이터 (Airflow DAG 기본 인자들)
    dag_id="poc_bq_dbtdag",
    schedule=None,                          # 수동 trigger
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["poc", "dbt", "bigquery", "cosmos", "dbtdag"],

    # Cosmos 설정 (DbtTaskGroup 과 동일)
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        manifest_path=f"{DBT_PROJECT_PATH}/target/manifest.json",
    ),
    profile_config=profile_config,
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        enable_mock_profile=False,
    ),
    operator_args={
        "install_deps": False,
    },
)
