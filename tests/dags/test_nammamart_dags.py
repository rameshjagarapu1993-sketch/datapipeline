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


def test_nammamart_etl_graph_connects_each_pipeline_stage() -> None:
    dag = DAG_BAG.get_dag("nammamart_etl")
    assert dag is not None

    extract_tasks = {
        "extract_orders",
        "extract_payments",
        "extract_customers",
        "extract_products",
        "extract_stores",
        "extract_increment",
    }
    assert extract_tasks.issubset(dag.task_ids)
    assert dag.get_task("validate_sources").upstream_task_ids == extract_tasks
    assert dag.get_task("quarantine").upstream_task_ids == {"validate_sources"}
    assert dag.get_task("transform_and_load").upstream_task_ids == {"quarantine"}
    assert dag.get_task("report").upstream_task_ids == {"transform_and_load"}
    assert dag.get_task("audit_run").upstream_task_ids == {"report"}


def test_nammamart_elt_graph_connects_each_pipeline_stage() -> None:
    dag = DAG_BAG.get_dag("nammamart_elt")
    assert dag is not None

    extract_tasks = {
        "extract_orders",
        "extract_payments",
        "extract_customers",
        "extract_products",
        "extract_stores",
        "extract_increment",
    }
    assert extract_tasks.issubset(dag.task_ids)
    assert dag.get_task("load_raw_and_transform").upstream_task_ids == extract_tasks
    assert dag.get_task("report").upstream_task_ids == {"load_raw_and_transform"}
    assert dag.get_task("audit_run").upstream_task_ids == {"report"}
