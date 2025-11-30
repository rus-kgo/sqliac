"""Error classes for the pipeline.

This module provides:
- FileError: exception when the file path is incorrect;
- DefinitionKeyError: exception when the definition yaml file keys are incorrect;
- DependencyError: exception when the names of the resources in the dependecy map are incorrect.
"""

import json
from rich.console import Console
from rich.syntax import Syntax
from rich.panel import Panel
from rich.console import Group

# Maximum number of characters to include in SQL preview in error messages
SQL_PREVIEW_MAX_LENGTH = 500


class FileError(Exception):
    """File path error class."""

    def __init__(self, path:str=None, resource_type:str=None):
        """Initialize the FileError with an optional path and resource type.

        Args:
            path (str, optional): The file path that caused the error.
            resource_type (str, optional): The type of resource related to the file path.
        """
        if resource_type and path:
            message = f"Not a valid file for the resource: '{resource_type}' " \
                f"In the file path {path}"
        elif path:
            message = f"Not a valid file path: '{path}'"
        super().__init__(message)

class TemplateFileError(Exception):
    """Raised when there is an issue with the resources template file."""

    def __init__(self, name: str, file: str, error: Exception):
        """Initialize the exception with detailed error information.

        Args:
            file (str): The name of the template file (without extension).
            error (Exception): The caught Jinja2 exception instance.
            name (str): The name of the resource.

        """
        if hasattr(error, "lineno"):
            message = (
                f"Template syntax error of the resource: '{name}' at line {error.lineno} "
                f"in file = '{file}'\n"
                f"Jinja2 error: {str(error)}"
            )
        elif hasattr(error, "message"):
            message = (
                f"Template rendering error of the resource: '{name}' in file '{file}': {error.message}"
            )
        else:
            message = (
                f"Invalid or missing template for the resource: '{name}' in file = '{file}'.\n"
                f"Jinja2 error: {str(error)}"
            )

        super().__init__(message)

class DefinitionKeyError(Exception):
    """Raised when there are missing or invalid keys in a definition file."""

    def __init__(self, keys:list, name:str=None, file:str=None):
        """Define the message.

        Args:
            keys (list): The invalid or missing keys.
            file (str, optional): The name of the file containing the invalid definition.
            name (str, optional): The specific resource name where the keys are missing or invalid.

        """
        keys_str = "\n- ".join(keys)

        if name:
            message= f"Invalid or missing definition variables for the resource: '{name}'." \
                 "\nVariables: " \
                f"\n- {keys_str}"

        elif file and name:
            message = f"Invalid or missing definition variables for the resource: '{name}'." \
                f"\nDefinition file: '{file}'." \
                 "\nVariables: " \
                f"\n- {keys_str}"
        else:
            message = "One of the definitions files has an invalid or missing variables:" \
                f"\n- {keys_str}"

        super().__init__(message)

class DependencyError(Exception):
    """Dependencies names errors."""

    def __init__(self, d_map:dict,*,is_cyclical:bool = False):
        """Take the dependency map as a variable and format it for readability."""
        formatted_map = json.dumps(d_map, indent=4)
        if is_cyclical:
            message = f"There is a cyclical dependecy in the map: \n{formatted_map}."
        else:
            message = f"There is an incorrect dependency in the map:\n{formatted_map}" \
            "\nMake sure the resources names are correct."

        super().__init__(message)

    """Custom exception for errors occurring during token request."""

class SQLExecutionError(Exception):
    """Raised when SQL execution fails in the database."""

    def __init__(  # noqa: D107
        self,
        error: Exception,
        sql: str = None,
        resource_type: str = None,
        resource_name: str = None,
        action: str = None,
    ):
        console = Console()

        # Error details
        error_message = str(error)
        orig_error = getattr(error, "orig", None)
        db_error_msg = str(orig_error) if orig_error else error_message

        # Optional context line
        operation_line = None
        if action and resource_type and resource_name:
            operation_line = (
                f"[yellow]operation:[/yellow] {action.upper()} {resource_type.upper()} "
                f"→ [bold]{resource_name}[/bold]"
            )

        # Build main message in a structured, compiler-style format
        message_lines = []

        # OPERATION CONTEXT
        if operation_line:
            message_lines.append(operation_line)

        # ERROR MESSAGE BODY
        message_lines.append(f"[red]message:[/red] {db_error_msg}")

        # HELP / SUGGESTION (only if we can infer anything)
        # You can expand this with smarter database-specific heuristics
        if "syntax" in db_error_msg.lower():
            message_lines.append(
                "[cyan]help:[/cyan] check SQL syntax near the reported location",
            )

        # Combine into a Rich Group
        header_group = Group(*message_lines)

        # Render header panel
        console.print(
            Panel(
                header_group,
                border_style="red",
                title="[bold red]SQL Execution Error[/bold red]",
                expand=False,
            ),
        )

        # ───────────────────────────────────────────────────────────────
        # SQL CODE FRAME
        # ───────────────────────────────────────────────────────────────
        if sql:
            sql_preview = (
                sql if len(sql) <= SQL_PREVIEW_MAX_LENGTH
                else sql[:SQL_PREVIEW_MAX_LENGTH] + "..."
            )

            console.print(
                Panel(
                    Syntax(sql_preview, "sql", theme="monokai", line_numbers=True),
                    title="[yellow]SQL Statement[/yellow]",
                    border_style="yellow",
                    expand=False,
                ),
            )

        # ───────────────────────────────────────────────────────────────
        # TRACEBACK MESSAGE FOR PYTHON ERROR
        # (kept short, per best practices)
        # ───────────────────────────────────────────────────────────────
        if action and resource_type and resource_name:
            msg = f"{action.upper()} {resource_type.upper()} failed for '{resource_name}'"
        else:
            msg = "SQL execution failed"

        super().__init__(msg)

        # Store metadata
        self.original_error = error
        self.sql = sql
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.action = action
