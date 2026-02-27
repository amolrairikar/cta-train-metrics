"""Module for testing main.py in train_location_fetch lambda."""

import os
import unittest
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
import requests

from lambdas.train_location_fetch.main import fetch_cta_data, write_to_firehose, handler


class TestFetchCtaData(unittest.TestCase):
    """Class for testing fetch_cta_data function."""

    @patch("lambdas.train_location_fetch.main.requests.get")
    def test_fetch_cta_data_success(self, mock_get):
        """Tests successful API request to CTA API to get train locations."""
        # Arrange
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": "test"}
        mock_get.return_value = mock_response

        # Act
        result = fetch_cta_data(line="test", api_key="test_key")

        # Assert
        mock_get.assert_called_once_with(
            url="http://lapi.transitchicago.com/api/1.0/ttpositions.aspx",
            params={
                "rt": "test",
                "key": "test_key",
                "outputType": "JSON",
            },
            timeout=5,
        )
        self.assertEqual(result, {"data": "test"})

    @patch("lambdas.train_location_fetch.main.requests.get")
    @patch("lambdas.train_location_fetch.main.time.sleep")
    def test_fetch_cta_data_http_error_exhaust_retries(self, mock_sleep, mock_get):
        """Tests HTTP error with max retries exhausted."""
        # Arrange
        mock_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError("404 Not Found")
        )

        # Act & Assert
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_cta_data(line="test", api_key="test_key", max_retries=2)

        self.assertEqual(mock_get.call_count, 3)
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

    @patch("lambdas.train_location_fetch.main.requests.get")
    @patch("lambdas.train_location_fetch.main.time.sleep")
    def test_fetch_cta_data_timeout_retry_success(self, mock_sleep, mock_get):
        """Tests timeout followed by successful retry."""
        # Arrange
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": "retry_success"}
        mock_get.side_effect = [requests.exceptions.Timeout("Timeout"), mock_response]

        # Act
        result = fetch_cta_data(line="test", api_key="test_key", max_retries=2)

        # Assert
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once_with(1)
        self.assertEqual(result, {"data": "retry_success"})


class TestWriteToFirehose(unittest.TestCase):
    """Class for testing write_to_firehose function."""

    @patch("lambdas.train_location_fetch.main.boto3.client")
    def test_firehose_write_success(self, mock_client):
        """Tests successfully writing payload to Firehose."""
        # Arrange
        mock_client.return_value = MagicMock()
        mock_client.return_value.put_record.return_value = None

        # Act
        write_to_firehose(payload="test")

        # Assert
        mock_client.return_value.put_record.assert_called_with(
            DeliveryStreamName="cta-train-locations-stream",
            Record={"Data": "test"},
        )

    @patch("lambdas.train_location_fetch.main.boto3.client")
    def test_firehose_write_failure(self, mock_client):
        """Tests failure in writing payload to Firehose."""
        # Arrange
        mock_client.return_value = MagicMock()
        mock_client.return_value.put_record.side_effect = (
            botocore.exceptions.ClientError(
                error_response={
                    "Error": {
                        "Code": "InternalServerError",
                        "Message": "InternalServerError",
                    }
                },
                operation_name="GetParameter",
            )
        )

        # Act + Assert
        with pytest.raises(botocore.exceptions.ClientError):
            write_to_firehose(payload="test")

        mock_client.return_value.put_record.assert_called_with(
            DeliveryStreamName="cta-train-locations-stream",
            Record={"Data": "test"},
        )


class TestLambdaHandler(unittest.TestCase):
    """Class for testing handler function."""

    def setUp(self):
        self.env_patcher = patch.dict(os.environ, {"CTA_API_KEY": "test_key"})
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()

    @patch("lambdas.train_location_fetch.main.write_to_firehose")
    @patch("lambdas.train_location_fetch.main.fetch_cta_data")
    def test_handler_success_test_function(self, mock_fetch, mock_firehose):
        """Tests successful handler execution for test function (with mocked dependencies)."""
        # Arrange
        mock_fetch.return_value = {"line": "test_line", "trains": []}
        event = {"test": "event"}
        context = MagicMock()
        context.function_name = "test-function-test"

        # Act
        result = handler(event=event, context=context)

        # Assert
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 8)
        self.assertFalse(mock_firehose.called)

    @patch("lambdas.train_location_fetch.main.write_to_firehose")
    @patch("lambdas.train_location_fetch.main.fetch_cta_data")
    def test_handler_success_live_function(self, mock_fetch, mock_firehose):
        """Tests successful handler execution for live function (with mocked dependencies)."""
        # Arrange
        mock_fetch.return_value = {"line": "test_line", "trains": []}
        event = {"test": "event"}
        context = MagicMock()
        context.function_name = "live-function"

        # Act
        result = handler(event=event, context=context)

        # Assert
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 8)
        self.assertTrue(mock_firehose.called)

        args, kwargs = mock_firehose.call_args
        sent_payload = kwargs.get("payload") or args[0]
        self.assertIn("timestamp", sent_payload)
        self.assertTrue(sent_payload.endswith("\n"))

    @patch("lambdas.train_location_fetch.main.write_to_firehose")
    @patch("lambdas.train_location_fetch.main.fetch_cta_data")
    def test_handler_partial_failure(self, mock_fetch, mock_firehose):
        """Test a single thread pool executor failing."""
        # Arrange
        mock_fetch.side_effect = [
            {"data": "ok"},
            {"data": "ok"},
            {"data": "ok"},
            {"data": "ok"},
            {"data": "ok"},
            {"data": "ok"},
            {"data": "ok"},
            Exception("API Timeout"),
        ]
        context = MagicMock()
        context.function_name = "live-function"

        # Act
        result = handler(event={}, context=context)

        # Assert
        self.assertEqual(result["count"], 7)
        self.assertEqual(result["status"], "success")
        self.assertTrue(mock_firehose.called)
