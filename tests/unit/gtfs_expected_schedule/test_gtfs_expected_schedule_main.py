"""Module for testing main.py in gtfs_expected_schedule lambda."""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from lambdas.gtfs_expected_schedule.main import get_db_connection, handler


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Fixture to mock environment variables."""
    monkeypatch.setenv("ACCOUNT_NUMBER", "123456789012")


class TestGetDbConnection(unittest.TestCase):
    """Class for testing get_db_connection function."""

    @patch("lambdas.gtfs_expected_schedule.main.duckdb.connect")
    def test_get_db_connection(self, mock_connect):
        """Tests successfully initializing DuckDB connection."""
        # Arrange
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        # Act
        result = get_db_connection()

        # Assert
        mock_connect.assert_called_once_with(database=":memory:")
        args, _ = mock_con.execute.call_args
        self.assertIn("CREATE OR REPLACE SECRET", args[0])
        self.assertEqual(result, mock_con)


class TestHandler(unittest.TestCase):
    """Class for testing handler function."""

    @patch("lambdas.gtfs_expected_schedule.main.get_db_connection")
    def test_handler_success(self, mock_get_conn):
        """Tests successful handler execution."""
        # Arrange
        mock_con = MagicMock()
        mock_get_conn.return_value = mock_con
        mock_con.execute.return_value.fetchone.return_value = ["20260101"]

        # Act
        response = handler(event={}, context={})

        # Assert
        assert response == {"status": "success", "effective_date": "20260101"}
        assert mock_con.execute.call_count == 7
        mock_con.execute.assert_any_call("SELECT MIN(start_date) FROM calendar")

    @patch("lambdas.gtfs_expected_schedule.main.get_db_connection")
    def test_handler_no_start_date_raises_error(self, mock_get_conn):
        """Tests handler returns error if no start_date is returned from query."""
        # Arrange
        mock_con = MagicMock()
        mock_get_conn.return_value = mock_con
        mock_con.execute.return_value.fetchone.return_value = [None]

        # Act & Assert
        with pytest.raises(ValueError, match="No start_date found"):
            handler(event={}, context={})

        assert mock_con.execute.call_count == 6
