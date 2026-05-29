"""
PoC DAG (옵션 4): BigQuery + dbt + Cosmos + Custom tasks 실 실행 검증.

DbtTaskGroup 앞뒤에 BashOperator 를 두어 사내 운영 패턴 (dbt + 부속 task) 학습.

구조:
    start_marker → dbt_models (TaskGroup) → verify_results → end_marker

- Cosmos 가 만드는 것 (Layer 2): dbt_models 안의 .run / .test
- 사용자가 만드는 것 (Layer 3): start_marker, verify_results, end_marker

선행 PoC:
- [[02_dbt_render_in_composer]] (Trino, render-only)

관련 노트: 애슬론/PoC/03_bq_dbt_run_in_composer.md
"""
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode

# ─────────────────────────────────────────────────────────────────────────────
# 경로 설정 (Composer 자동 마운트)
# ─────────────────────────────────────────────────────────────────────────────
DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt_projects/dbt_test"
BQ_PROJECT = "dev-dp-project-354904"
BQ_DATASET = "dbt_test"

# ─────────────────────────────────────────────────────────────────────────────
# Profile 설정
# ─────────────────────────────────────────────────────────────────────────────
profile_config = ProfileConfig(
    profile_name="dbt_test",
    target_name="composer",
    profiles_yml_filepath=Path(f"{DBT_PROJECT_PATH}/profiles.yml"),
)

# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="poc_bq_dbt_run",
    description="PoC: BigQuery + dbt + Cosmos + 부속 task 패턴",
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["poc", "dbt", "bigquery", "cosmos"],
) as dag:

    # ── Layer 3 (사용자) — 시작 marker ───────────────────────────────────────
    start_marker = BashOperator(
        task_id="start_marker",
        bash_command='echo "🚀 dbt pipeline 시작: $(date)"',
    )

    # ── Layer 2 (Cosmos) — dbt 모델 task 자동 생성 ─────────────────────────────
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=f"{DBT_PROJECT_PATH}/target/manifest.json",
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            enable_mock_profile=False,
        ),
        operator_args={"install_deps": False},
    )

    # ── Layer 3 (사용자) — BQ 에서 실제 결과 검증 ───────────────────────────────
    # dbt 가 만든 테이블의 row 수 확인. 운영에서 데이터 sanity check 패턴.
    verify_results = BashOperator(
        task_id="verify_results",
        bash_command=(
            f'bq query --use_legacy_sql=false --format=pretty '
            f'"SELECT '
            f"'my_first_dbt_model' AS model, COUNT(*) AS row_count "
            f"FROM \\`{BQ_PROJECT}.{BQ_DATASET}.my_first_dbt_model\\` "
            f"UNION ALL "
            f"SELECT 'my_second_dbt_model', COUNT(*) "
            f"FROM \\`{BQ_PROJECT}.{BQ_DATASET}.my_second_dbt_model\\`"
            f'"'
        ),
    )

    # ── Layer 3 (사용자) — 끝 marker ────────────────────────────────────────
    end_marker = BashOperator(
        task_id="end_marker",
        bash_command='echo "✅ dbt pipeline 완료: $(date)"',
    )

    # ── Wire: 의존성 연결 ─────────────────────────────────────────────────────
    start_marker >> dbt_tasks >> verify_results >> end_marker
