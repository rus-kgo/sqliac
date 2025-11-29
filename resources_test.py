from utils import Utils  # noqa: D100
import logging
import tomllib
from rich.console import Console
from rich.syntax import Syntax
from rich.panel import Panel

from sqlglot import parse_one, exp
from sqlglot.errors import ParseError



class LocalTest:
    """Local test class to test the resources templates."""
    console = Console()
    def __init__(
            self,
            db_system: str,
            resource_type: str,
            resource_name: str,
            ):
        """Initialize LocalTest with database system, resource type, and resource name."""
        self.db_system = db_system
        self.resource_type = resource_type
        self.resource_name = resource_name

        with open("resources.toml", "rb") as f:
            config = tomllib.load(f)
            self.db_sys_resources = config[self.db_system]["resources"]

    def _parse_sql(self, sql):
        try:
            parsed = parse_one(
                sql,
                read=self.db_system,
                error_level="ignore",     # do NOT throw until explicitly caught
            )

            # Handle invalid fallback nodes
            if isinstance(parsed, (exp.Command, exp.Placeholder)):
                self.console.print(
                    Panel.fit(
                        f"[red]SQL could not be parsed by dialect '{self.db_system}'.[/red]\n"
                        f"Parsed as: [bold]{type(parsed).__name__}[/bold] (fallback node)\n"
                        f"This usually means invalid syntax or unsupported features.",
                        title="❌ Invalid SQL",
                        border_style="red",
                    ),
                )
                self.console.print(Syntax(sql, "sql", theme="monokai"))
                return

            # Optional: validate allowed statement types
            valid = (exp.Create, exp.Drop, exp.Alter, exp.Select, exp.Grant, exp.Use, exp.Show)
            if not isinstance(parsed, valid):
                self.console.print(f"[yellow]Warning: unusual statement type {type(parsed).__name__}[/yellow]")

        except ParseError as err:
            # Extract useful info
            message = err.args[0]
            line = err.line or "?"
            col = err.col or "?"

            # Pretty print error
            self.console.print(
                Panel.fit(
                    f"[red bold]SQL Parse Error[/red bold]\n"
                    f"[white]Message:[/white] {message}\n"
                    f"[white]Location:[/white] Line {line}, Col {col}",
                    border_style="red",
                ),
            )

            # Show SQL with syntax highlight
            self.console.print(Syntax(sql, "sql", theme="monokai"))

            # Also print a caret indicator under the failing column
            if err.line and err.col:
                lines = sql.splitlines()
                error_line = lines[err.line - 1]

                pointer = " " * (err.col - 1) + "^"

                self.console.print(f"\n[red]{err.line:>3} | {error_line}[/red]")
                self.console.print(f"      [red]{pointer}[/red]\n")

            return  # No traceback, clean exit

        except Exception as err:
            self.console.print(Panel(str(err), title="Unexpected Error", border_style="red"))
            return

        # If no errors:
        self.console.print(
            Panel.fit(
                f"[green]SQL validated successfully[/green]\nParsed as: [bold]{type(parsed).__name__}[/bold]",
                border_style="green",
            ),
        )
        self.console.print(Syntax(sql, "sql", theme="monokai"))

    def test_template_query(self, iac_action: str):
        """Load resource and print out the output for testing."""
        utils = Utils(
            resources_path="resources.toml",
            definitions_path="",
        )

        sql = utils.render_templates(
            template=self.db_sys_resources[self.resource_type]["template"],
            definition=self.db_sys_resources[self.resource_type]["definition"],
            name=self.resource_name,
            iac_action=iac_action,
        )

        self._parse_sql(sql)

    def test_status_query(self):
        """Test the resource status query."""
        utils = Utils(
            resources_path="resources.toml",
            definitions_path="",
        )

        sql = utils.render_templates(
            template=self.db_sys_resources[self.resource_type]["state_query"],
            name=self.resource_name,
        )

        self._parse_sql(sql)


if __name__ == "__main__":
    # Suppress sqlglot logging globally
    logging.getLogger("sqlglot").setLevel(logging.CRITICAL)

    local_test = LocalTest(
        resource_name="my_alert",
        resource_type="alert",
        db_system="snowflake",
    )
    local_test.test_template_query(
        iac_action="create",
    )
    local_test.test_status_query()
