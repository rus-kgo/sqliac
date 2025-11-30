"""Main entrance point of the pipeline.

This module provides:
- main: main function that orchestrates the pipeline;
- str_to_bool: function for bool input vars;
- to_str: function for string input vars that might be empty or null;
"""

import os
import tomllib

from utils import Utils
from errors import TemplateFileError, FileError
from drift import Drift
from rich.console import Console
from dataclasses import dataclass


def str_to_bool(s: str) -> bool:
    """Convert input string to a boolean."""
    s = s.lower()
    if s not in {"true", "false"}:
        raise ValueError(f"Invalid value for boolean: {s}")  # noqa: TRY003
    return s == "true"


def to_str(s: str | None) -> str | None:
    """Make sure it a string, return None empty."""
    if s is None or s in {"None", ""}:
        return None
    return s

@dataclass
class InputConfig:
    """Inputs from the environement."""
    workspace: str
    database_system: str
    definitions_path: str | None
    resources_path: str | None
    operation: bool
    run_mode: str

def parse_env() -> InputConfig:
    """Read and normalize inputs from the environment."""
    try:
        workspace = os.environ["GITHUB_WORKSPACE"]
        database_system = os.environ["INPUT_DATABASE-SYSTEM"]
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e  # noqa: TRY003

    definitions_path = to_str(
        os.environ.get(
            "INPUT_DEFINITIONS-PATH",
            "/definitions",
            ),
        )
    resources_path = to_str(
        os.environ.get(
            "INPUT_RESOURCES-PATH",
            "/sqliac/resources.toml",
            ),
        )

    operation = os.environ.get("INPUT_OPERATION", "Plan")
    run_mode = os.environ.get("INPUT_RUN-MODE", "default")
    return InputConfig(
        workspace=workspace,
        database_system=database_system,
        definitions_path=definitions_path,
        resources_path=resources_path,
        operation=operation,
        run_mode=run_mode,
    )

def run(config: InputConfig) -> None:  # noqa: PLR0912, PLR0915
    """Orchestrate the pipeline."""
    definitions_path = f"{config.workspace}{config.definitions_path}"

    utils = Utils(
        definitions_path=definitions_path,
        resources_path=config.resources_path,
    )

    # Map the dependencies of all the definitions in the yaml files
    d_map:dict = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map:list[str] = utils.dependencies_sort(d_map)

    # Establish the connection
    conn = utils.create_db_sys_connection(database_system=config.database_system)

    # Initate drift class to compare object states
    drift = Drift(conn=conn)

    # Load all resources
    # TODO: separate the load of a resources config with check of nesesary keys
    try:
        with open(config.resources_path, "rb") as f:
            db_sys_config = tomllib.load(f)
            db_sys_resources = db_sys_config[config.database_system]["resources"]
    except FileNotFoundError as err:
        raise FileError(config.resources_path) from err

    # Print out the map planning, excecute if not a dry-run.
    for i in sorted_map:
        resource_type, resource_name = i.split("::")

        file_path = os.path.join(definitions_path, f"{resource_type}.toml")

        try:
            with open(file_path, "rb") as f:
                definition = tomllib.load(f)
        except FileNotFoundError as err :
            raise FileError(path=file_path, resource_type=resource_type) from err

        try:
            for rsc in definition[resource_type]:
                # This is for benefit of following the sorter order
                if rsc["name"] == resource_name:

                    rsc_state_query = utils.render_templates(
                        template=db_sys_resources[resource_type]["state_query"],
                        name=resource_name,
                        definition=rsc,
                    )

                    rsc_drift = drift.resource_state(
                        definition=rsc,
                        state_query=rsc_state_query,
                        name=resource_name,
                        )

                    if config.run_mode.lower() == "create-or-update":
                        # If there is no drift, then it is a new object.
                        if rsc_drift["iac_action"]=="create":
                            sql = utils.render_templates(
                                template=db_sys_resources[resource_type]["template"],
                                definition=rsc_drift["definition"],
                                name=resource_name,
                                iac_action=db_sys_resources[resource_type]["iac_action"]["create"],
                            )

                            utils.execute_rendered_sql_template(
                                connection=conn,
                                sql=sql,
                                dependencies=rsc_drift["definition"]["depends_on"],
                                operation=config.operation,
                                wait_time=rsc.get("wait_time", None),
                            )

                        # Do nothing if the the object has not drifted, definition and the state are the same.
                        elif rsc_drift["iac_action"]=="no action":
                            continue

                        # If the object drifted, alter the properties of the object.
                        elif rsc_drift["iac_action"]=="alter":
                            sql = utils.render_templates(
                                template=db_sys_resources[resource_type]["template"],
                                definition=rsc_drift["definition"],
                                name=resource_name,
                                iac_action=db_sys_resources[resource_type]["iac_action"]["alter"],
                            )
                            utils.execute_rendered_sql_template(
                                connection=conn,
                                sql=sql,
                                dependencies=rsc_drift["definition"]["depends_on"],
                                operation=config.operation,
                                wait_time=rsc.get("wait_time", None),
                            )

                    elif config.run_mode.lower() == "destroy":
                        sql = utils.render_templates(
                            template=db_sys_resources[resource_type]["template"],
                            definition=rsc_drift["definition"],
                            name=resource_name,
                            iac_action=db_sys_resources[resource_type]["iac_action"]["drop"],
                        )

                        utils.execute_rendered_sql_template(
                            connection=conn,
                            sql=sql,
                            dependencies=rsc_drift["definition"]["depends_on"],
                            operation=config.operation,
                            wait_time=rsc.get("wait_time", None),
                        )

        except Exception as err:
            conn.close()
            raise TemplateFileError(resource_name, config.resources_path, err) from err
    conn.close()



def main():
    """Entry point of the pipeline."""
    try:
        cfg = parse_env()
        run(cfg)
    except (TemplateFileError, FileError) as e:
        Console().print(f"[bold red3]Configuration error:[/bold red3] {e}")
        raise
    except Exception:
        Console().print("[bold red3]Unexpected error[/bold red3]")
        raise

if __name__ == "__main__":
    main()
