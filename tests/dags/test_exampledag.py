from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest
from airflow.models import DagBag


DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from exampledag import _parse_astronaut_response


def test_astronaut_response_is_parsed_and_normalized() -> None:
    assert _parse_astronaut_response(
        {
            "number": 2,
            "people": [
                {"name": "  Ada  ", "craft": " ISS "},
                {"name": "Lin", "craft": "Tiangong"},
            ],
        }
    ) == (2, [{"name": "Ada", "craft": "ISS"}, {"name": "Lin", "craft": "Tiangong"}])


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "JSON object"),
        ({"number": True, "people": []}, "invalid 'number'"),
        ({"number": 1, "people": []}, "does not match"),
        ({"number": 1, "people": [{"name": "Ada"}]}, "has no craft"),
    ],
)
def test_invalid_astronaut_responses_raise(payload: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _parse_astronaut_response(payload)


def test_example_dag_has_timezone_and_failure_defaults() -> None:
    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not dag_bag.import_errors
    dag = dag_bag.get_dag("example_astronauts")
    assert dag is not None
    assert dag.timezone.name == "Asia/Kolkata"
    assert dag.catchup is False
    assert dag.default_args["retries"] == 3
    assert dag.default_args["retry_delay"] == timedelta(minutes=2)
    assert dag.default_args["execution_timeout"] == timedelta(minutes=5)
