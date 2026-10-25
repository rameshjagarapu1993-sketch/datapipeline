"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://www.astronomer.io/docs/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from datetime import timedelta

import requests
from airflow.sdk import Asset, dag, task
from pendulum import datetime


ASTRONAUTS_URL = "http://api.open-notify.org/astros.json"


def _parse_astronaut_response(payload: object) -> tuple[int, list[dict[str, str]]]:
    if not isinstance(payload, dict):
        raise ValueError("Astronaut API response must be a JSON object")

    number = payload.get("number")
    people = payload.get("people")
    if isinstance(number, bool) or not isinstance(number, int) or number < 0:
        raise ValueError("Astronaut API response has an invalid 'number'")
    if not isinstance(people, list):
        raise ValueError("Astronaut API response has an invalid 'people' list")

    parsed_people: list[dict[str, str]] = []
    for index, person in enumerate(people):
        if not isinstance(person, dict):
            raise ValueError(f"Astronaut API person at index {index} is not an object")
        name = person.get("name")
        craft = person.get("craft")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Astronaut API person at index {index} has no name")
        if not isinstance(craft, str) or not craft.strip():
            raise ValueError(f"Astronaut API person at index {index} has no craft")
        parsed_people.append({"name": name.strip(), "craft": craft.strip()})

    if number != len(parsed_people):
        raise ValueError(
            "Astronaut API response count does not match the number of people"
        )
    return number, parsed_people


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 4, 22, tz="Asia/Kolkata"),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["example"],
)
def example_astronauts():
    # Define tasks
    @task(
        # Define an asset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Asset
    def get_astronauts(**context) -> list[dict[str, str]]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        response = requests.get(ASTRONAUTS_URL, timeout=10)
        response.raise_for_status()
        number_of_people_in_space, list_of_people_in_space = (
            _parse_astronaut_response(response.json())
        )

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(
        greeting: str, person_in_space: dict[str, str]
    ) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()  # Define dependencies using TaskFlow API syntax
    )


# Instantiate the DAG
example_astronauts()
