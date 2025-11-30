"""Unit test module."""

import unittest
from unittest.mock import patch
from sqlalchemy.engine import Connection

from utils import Utils
from errors import DefinitionKeyError, DependencyError, TemplateFileError, SQLExecutionError

class TestUtils(unittest.TestCase):  
    """Unit tests for the Utils class and its dependency-related methods."""

    def setUp(self):
        """Set up the Utils loader for each test."""
        self.loader = Utils(
            resources_path="resources.toml",
            definitions_path = "definitions",
        )

    @patch("tomllib.load")
    def test_dependencies_map_with_valid_dependencies(self, mock_tomli_load):
        """Test dependencies_map returns empty list when depends_on is populated."""
        # Mock TOML content
        mock_tomli_load.return_value = {
            "database": [
                {
                    "name": "ajwa_presentation",
                    "depends_on": {
                        "role": ["bi_admin_role"],
                    },
                },
            ],
        }

        expected = {
            "database::ajwa_presentation": ["role::bi_admin_role"],
        }

        result = self.loader.dependencies_map()
        self.assertEqual(result, expected)

    @patch("tomllib.load")
    def test_dependencies_map_with_no_dependencies(self, mock_tomli_load):
        """Test dependencies_map returns empty list when depends_on is not defined."""
        mock_tomli_load.return_value = {
            "role": [
                {
                    "name": "viewer",
                    "depends_on": {},
                },
            ],
        }

        expected = {
            "role::viewer": [],
        }

        result = self.loader.dependencies_map()
        self.assertEqual(result, expected)

    @patch("tomllib.load")
    def test_dependencies_map_missing_depends_on_raises(self, mock_tomli_load):
        """Test dependencies_map raises DefinitionKeyError when depends_on is missing."""
        mock_tomli_load.return_value = {
            "database": [
                {
                    "name": "missing_depends",
                    # depends_on is missing
                },
            ],
        }

        with self.assertRaises(DefinitionKeyError) as context:
            self.loader.dependencies_map()

        self.assertIn("depends_on", str(context.exception))

    def test_dependencies_sort_with_valid_dependencies_map(self):
        """Tests the dependencies_sort method to ensure it returns a correctly sorted list of objects based on their dependencies.

        Given a map of objects and their dependencies, verifies that the method returns the expected topologically sorted order
        and the appropriate success message.
        """
        d_map = {
            "role::bi_god_role": ["user::god"],
            "role::bi_admin_role": [],
            "user::god":[],
            "database::ajwa_presentation": ["role::bi_god_role","role::bi_admin_role"],
        }

        expected = ["user::god","role::bi_admin_role","role::bi_god_role","database::ajwa_presentation"]
        result = self.loader.dependencies_sort(d_map)
        self.assertEqual(result, expected)

    def test_dependencies_sort_with_no_dependencies_map(self):
        """Test that the dependencies sorting function correctly handles an map without depencies.

        Ensure that it does not raise errors and returns the expected result - a reversed list of the mapped objects.
        """
        d_map = {
            "role::bi_god_role": [],
            "role::bi_admin_role": [],
            "user::god":[],
            "database::ajwa_presentation": [],
        }

        expected = [
            "database::ajwa_presentation",
            "user::god",
            "role::bi_admin_role",
            "role::bi_god_role",
            ]
        result = self.loader.dependencies_sort(d_map)
        self.assertEqual(result, expected)

    def test_dependencies_sort_with_invalid_dependencies_map(self):
        """Test that dependencies_sort raises a DependencyError.
        
        The error is raised when the dependencies map contains references to resources that 
        do not exist in the definitions config file.
        This ensures that the function correctly identifies and handles invalid dependencies, such as when
        a resource listed in the 'depend_on' field does not correspond to any defined resource name.
        """
        d_map = {
            "role::bi_god_role": ["wrong::god"],
            "role::bi_admin_role": [],
            "user::god":[],
        }

        with self.assertRaises(DependencyError):
            self.loader.dependencies_sort(d_map)


    def test_render_templates_with_valid_temmplate(self):
        """Test that render_templates correctly renders a template with the given definition and action."""
        template = """
        {% if iac_action.upper() == 'CREATE' %}

        CREATE ROLE {{ name }} 
        {%if comment != "" %}COMMENT = {{ comment }}{% endif %};

        {% elif iac_action.upper() == 'DROP' %}
        DROP ROLE {{ name }};

        {% endif %}
        """

        obj_name = "bi_god_role"

        definition = {
            "name": "bi_god_role",
            "comment": "",
            "depends_on": {},
            "wait_time": "",
        }

        expected = "CREATE ROLE bi_god_role"

        result = self.loader.render_templates(
            template=template,
            definition=definition,
            iac_action="create",
            name=obj_name,
        )
        self.assertEqual(result, expected)


    def test_render_templates_with_invalid_temmplate(self):
        """Test that render_templates raises TemplateFileError for an invalid template fron config."""
        template = """
        missing if statement

        CREATE ROLE {{ name }} 
        {%if comment != "" %}COMMENT = {{ comment }}{% endif %};

        {% elif iac_action.upper() == 'DROP' %}
        DROP ROLE {{ name }};

        {% endif %}
        """

        obj_name = "bi_god_role"

        definition = {
            "name": "bi_god_role",
            "comment": "",
            "depends_on": {},
            "wait_time": "",
        }

        with self.assertRaises(TemplateFileError):
            self.loader.render_templates(
                template=template,
                definition=definition,
                iac_action="create",
                name=obj_name,
            )

    def test_render_templates_with_sql_injection_definition(self):
        """Test that the render_templates method properly sanitizes definition values to prevent SQL injection.

        Ensures that special characters are removed from the rendered SQL and the output matches the expected
        sanitized result.
        """
        template = """
        {% if iac_action.upper() == 'CREATE' %}

        CREATE ROLE {{ name }} 
        {%if comment != "" %}COMMENT = {{ comment }}{% endif %};

        {% elif iac_action.upper() == 'DROP' %}
        DROP ROLE {{ name }};

        {% endif %}
        """

        obj_name = "bi_god_role"

        definition = {
            "name": "bi_god_role; drop all -- haha",
            "comment": "",
            "depends_on": {},
            "wait_time": "",
        }

        # This scrip will fail. The success is in removing the special charachters.
        expected = "CREATE ROLE bi_god_role drop all  haha"

        result = self.loader.render_templates(
            template=template,
            definition=definition,
            iac_action="create",
            name=obj_name,
        )
        self.assertEqual(result, expected)

    def test_render_templates_with_invalid_definition(self):
        """Test that render_templates raises TemplateFileError for an invalid definiton of the resource."""
        template = """
        {% if iac_action.upper() == 'CREATE' %}

        CREATE ROLE {{ name }} 
        {%if comment != "" %}COMMENT = {{ comment }}{% endif %};

        {% elif iac_action.upper() == 'DROP' %}
        DROP ROLE {{ name }};

        {% endif %}
        """

        obj_name = "bi_god_role"

        definition = {
            # missing 'name' variable
            "comment": "",
            "depends_on": {},
            "wait_time": "",
        }

        with self.assertRaises(TemplateFileError):
            self.loader.render_templates(
                template=template,
                definition=definition,
                iac_action="create",
                name=obj_name,
            )

    @patch("tomllib.load")
    @patch.dict("os.environ", {
    "SQLITE_ENGINE_SQLALCHEMY_CONNECT_ARGS_TIMEOUT": "1",
    "SQLITE_ENGINE_SQLALCHEMY_CONNECT_ARGS_CHECK_SAME_THREAD": "False",
    "SQLITE_ENGINE_SQLALCHEMY_CONNECT_ARGS_ISOLATION_LEVEL": "None",
}, clear=True)
    def test_create_db_sys_connection_with_valid_config(self, mock_tomlib_load):
        """Test creating database system connection with sqlalchemy."""
        mock_tomlib_load.return_value = {
            "sqlite":{
                "engine":{
                    "sqlalchemy.url":"sqlite://",
                    "sqlalchemy.connect_args":{
                        "timeout":1,
                        "check_same_thread": False,
                        "isolation_level": None,
                        },
                },
            },
        }

        conn = self.loader.create_db_sys_connection(database_system="sqlite")

        # Assert that the returned object is a SQLAlchemy Connection
        assert isinstance(conn, Connection)

        return conn

    def test_execute_rendered_sql_template(self):
        """Test the execution of SQL."""
        conn:Connection = self.test_create_db_sys_connection_with_valid_config()

        valid_sql = """
            CREATE TABLE IF NOT EXISTS actors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """
        invalid_sql = """
            CREATE IF NOT EXISTS actors (
                id INTEGER PRIMARY KEY
                name TEXT NOT NULL
            )
        """

        try:
            self.loader.execute_rendered_sql_template(
                conn=conn,
                sql=valid_sql,
                operation="Apply",
                depends_on={
                    "role": ["bi_admin_role"],
                },
                wait_time=1,
            )
        except Exception as e:
            self.fail(f"CREATE TABLE failed with exception: {e}")


        # Asert invalid sql
        with self.assertRaises(SQLExecutionError):
            self.loader.execute_rendered_sql_template(
                conn=conn,
                sql=invalid_sql,
                operation="Apply",
            )




if __name__ == "__main__":
    unittest.main()