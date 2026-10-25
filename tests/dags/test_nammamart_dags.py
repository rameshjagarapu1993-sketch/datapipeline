from pathlib import Path

from airflow.models import DagBag


DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
DAG_BAG = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_nammamart_dags_parse_without_import_errors() -> None:
    assert not DAG_BAG.import_errors
    assert {"nammamart_etl", "nammamart_elt"}.issubset(DAG_BAG.dag_ids)


def test_nammamart_schedule_and_reliability_defaults() -> None:
    for dag_id in ("nammamart_etl", "nammamart_elt"):
        dag = DAG_BAG.get_dag(dag_id)
        assert dag is not None
        assert dag.schedule == "0 9 * * 1-6"
        assert dag.timezone.name == "Asia/Kolkata"
        assert dag.catchup is False
        assert dag.default_args["retries"] == 2
        assert dag.default_args["retry_delay"].total_seconds() == 120
        assert dag.default_args["execution_timeout"].total_seconds() == 2700
        assert dag.max_active_runs == 1
