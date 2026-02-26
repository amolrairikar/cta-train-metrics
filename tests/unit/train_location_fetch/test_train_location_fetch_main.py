"""Module for testing main.py in train_location_fetch lambda."""

import os
import unittest
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
import requests

from lambdas.train_location_fetch.main import fetch_cta_data, write_to_firehose, handler


class TestFetchCtaLine(unittest.TestCase):
    """Class for testing fetch_cta_line function."""

    @patch("lambdas.train_location_fetch.main.requests.get")
    def test_fetch_cta_line_success(self, mock_get):
        """Tests successful API request to CTA API to get train locations."""
        # Arrange
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Act
        fetch_cta_data(line="test", api_key="test_key")

        # Assert
        mock_get.assert_called_once_with(
            url="http://lapi.transitchicago.com/api/1.0/ttpositions.aspx",
            params={
                "rt": "test",
                "key": "test_key",
                "outputType": "JSON",
            },
            timeout=10,
        )

    @patch("lambdas.train_location_fetch.main.requests.get")
    def test_fetch_cta_line_failure(self, mock_get):
        """Tests HTTP error on API request to CTA API to get train locations."""
        # Arrange
        mock_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError("404 Not Found")
        )

        # Act & Assert
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_cta_data(line="test", api_key="test_key")

        # Assert
        mock_get.assert_called_once_with(
            url="http://lapi.transitchicago.com/api/1.0/ttpositions.aspx",
            params={
                "rt": "test",
                "key": "test_key",
                "outputType": "JSON",
            },
            timeout=10,
        )


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
    def test_handler_success(self, mock_fetch, mock_firehose):
        """Tests successful handler execution."""
        # Arrange
        mock_fetch.return_value = {"line": "test_line", "trains": []}
        event = {"test": "event"}
        context = MagicMock()

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

        # Act
        result = handler(event={}, context=None)

        # Assert
        self.assertEqual(result["count"], 7)
        self.assertEqual(result["status"], "success")
        self.assertTrue(mock_firehose.called)
