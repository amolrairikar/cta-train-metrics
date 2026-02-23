"""Module for testing main.py in gtfs_data_fetch lambda."""

import unittest
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
import requests

from lambdas.gtfs_data_fetch.main import (
    get_last_modified_time,
    update_last_modified_time,
    upload_gtfs_zip_to_s3,
    handler,
)


class TestGetLastModifiedTime(unittest.TestCase):
    """Class for testing get_last_modified_time function."""

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    def test_get_last_modified_time_success(self, mock_client):
        """Tests successfully fetching GTFS data last modified time."""
        # Arrange
        mock_client.return_value = MagicMock()
        mock_client.return_value.get_parameter.return_value = {
            "Parameter": {
                "Name": "test-parameter",
                "Value": "test-value",
            }
        }

        # Act
        last_modified_time = get_last_modified_time()

        # Assert
        self.assertEqual(last_modified_time, "test-value")
        mock_client.return_value.get_parameter.assert_called_with(
            Name="gtfs_last_modified_time",
            WithDecryption=False,
        )

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    def test_get_last_modified_time_failure(self, mock_client):
        """Tests failure with fetching GTFS data last modified time."""
        # Arrange
        mock_client.return_value = MagicMock()
        mock_client.return_value.get_parameter.side_effect = (
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
            get_last_modified_time()


class TestUpdateLastModifiedTime(unittest.TestCase):
    """Class for testing update_last_modified_time function."""

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    def test_update_last_modified_time_success(self, mock_client):
        """Tests successfully updating GTFS data last modified time."""
        # Arrange
        mock_client.return_value = MagicMock()
        mock_client.return_value.put_parameter.return_value = "success"
        mock_last_modified_time = "2026-01-01T23:00:00"

        # Act
        update_last_modified_time(last_modified_time=mock_last_modified_time)

        # Assert
        mock_client.return_value.put_parameter.assert_called_with(
            Name="gtfs_last_modified_time",
            Value=mock_last_modified_time,
            Type="String",
            Overwrite=True,
        )

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    def test_update_last_modified_time_failure(self, mock_client):
        """Tests failure with fetching GTFS data last modified time."""
        # Arrange
        mock_client.return_value = MagicMock()
        mock_client.return_value.put_parameter.side_effect = (
            botocore.exceptions.ClientError(
                error_response={
                    "Error": {
                        "Code": "InternalServerError",
                        "Message": "InternalServerError",
                    }
                },
                operation_name="PutParameter",
            )
        )
        mock_last_modified_time = "2026-01-01T23:00:00"

        # Act + Assert
        with pytest.raises(botocore.exceptions.ClientError):
            update_last_modified_time(last_modified_time=mock_last_modified_time)

        mock_client.return_value.put_parameter.assert_called_with(
            Name="gtfs_last_modified_time",
            Value=mock_last_modified_time,
            Type="String",
            Overwrite=True,
        )


class TestUploadGtfsZipToS3(unittest.TestCase):
    """Class for testing upload_gtfs_zip_to_s3 function."""

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    @patch("lambdas.gtfs_data_fetch.main.zipfile.ZipFile")
    @patch.dict("os.environ", {"ACCOUNT_NUMBER": "123456789"})
    def test_upload_zip_success(self, mock_zip_file, mock_client):
        """Tests successfully uploading GTFS zip to S3."""
        # Arrange
        mock_response = MagicMock()
        mock_response.content = b"mock zip content"

        mock_s3_client = MagicMock()
        mock_client.return_value = mock_s3_client

        mock_file_info = MagicMock()
        mock_file_info.filename = "test_file.txt"
        mock_file_info.is_dir.return_value = False

        mock_zip_ref = MagicMock()
        mock_zip_ref.infolist.return_value = [mock_file_info]
        mock_zip_ref.__enter__.return_value = mock_zip_ref
        mock_zip_ref.open.return_value.__enter__.return_value = b"file content"
        mock_zip_file.return_value = mock_zip_ref

        # Act
        upload_gtfs_zip_to_s3(mock_response)

        # Assert
        mock_s3_client.upload_fileobj.assert_called_once_with(
            Fileobj=mock_zip_ref.open.return_value.__enter__.return_value,
            Bucket="123456789-cta-analytics-project",
            Key="gtfs_data/test_file.txt",
        )

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    @patch("lambdas.gtfs_data_fetch.main.zipfile.ZipFile")
    @patch.dict("os.environ", {"ACCOUNT_NUMBER": "123456789"})
    def test_upload_zip_partial_success(self, mock_zip_file, mock_client):
        """Tests partial success uploading GTFS zip to S3."""
        # Arrange
        mock_response = MagicMock()
        mock_response.content = b"mock zip content"

        mock_s3_client = MagicMock()
        mock_client.return_value = mock_s3_client

        # First file succeeds, second file fails
        mock_s3_client.upload_fileobj.side_effect = [
            None,
            botocore.exceptions.ClientError(
                error_response={
                    "Error": {"Code": "AccessDenied", "Message": "Access Denied"}
                },
                operation_name="PutFileObject",
            ),
        ]

        mock_file_info1 = MagicMock()
        mock_file_info1.filename = "file1.txt"
        mock_file_info1.is_dir.return_value = False

        mock_file_info2 = MagicMock()
        mock_file_info2.filename = "file2.txt"
        mock_file_info2.is_dir.return_value = False

        mock_zip_ref = MagicMock()
        mock_zip_ref.infolist.return_value = [mock_file_info1, mock_file_info2]
        mock_zip_ref.__enter__.return_value = mock_zip_ref
        mock_zip_ref.open.return_value.__enter__.return_value = b"file content"
        mock_zip_file.return_value = mock_zip_ref

        # Act
        upload_gtfs_zip_to_s3(mock_response)

        # Assert
        self.assertEqual(mock_s3_client.upload_fileobj.call_count, 2)
        mock_s3_client.upload_fileobj.assert_any_call(
            Fileobj=mock_zip_ref.open.return_value.__enter__.return_value,
            Bucket="123456789-cta-analytics-project",
            Key="gtfs_data/file1.txt",
        )
        mock_s3_client.upload_fileobj.assert_any_call(
            Fileobj=mock_zip_ref.open.return_value.__enter__.return_value,
            Bucket="123456789-cta-analytics-project",
            Key="gtfs_data/file2.txt",
        )

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    @patch("lambdas.gtfs_data_fetch.main.zipfile.ZipFile")
    @patch.dict("os.environ", {"ACCOUNT_NUMBER": "123456789"})
    def test_upload_zip_skips_directories(self, mock_zip_file, mock_client):
        """Tests that directories within the zip are skipped."""
        # Arrange
        mock_response = MagicMock()
        mock_response.content = b"mock zip content"

        mock_s3_client = MagicMock()
        mock_client.return_value = mock_s3_client

        mock_file_info = MagicMock()
        mock_file_info.filename = "test_directory/"
        mock_file_info.is_dir.return_value = True

        mock_zip_ref = MagicMock()
        mock_zip_ref.infolist.return_value = [mock_file_info]
        mock_zip_ref.__enter__.return_value = mock_zip_ref
        mock_zip_file.return_value = mock_zip_ref

        # Act
        upload_gtfs_zip_to_s3(mock_response)

        # Assert
        mock_s3_client.upload_fileobj.assert_not_called()
        mock_file_info.is_dir.assert_called_once()

    @patch("lambdas.gtfs_data_fetch.main.boto3.client")
    @patch("lambdas.gtfs_data_fetch.main.zipfile.ZipFile")
    @patch.dict("os.environ", {"ACCOUNT_NUMBER": "123456789"})
    def test_upload_zip_no_files_present(self, mock_zip_file, mock_client):
        """Tests no files are uploaded to S3 when the zip is empty."""
        # Arrange
        mock_response = MagicMock()
        mock_response.content = b"mock zip content"

        mock_s3_client = MagicMock()
        mock_client.return_value = mock_s3_client

        mock_zip_ref = MagicMock()
        mock_zip_ref.infolist.return_value = []  # Empty zip
        mock_zip_ref.__enter__.return_value = mock_zip_ref
        mock_zip_file.return_value = mock_zip_ref

        # Act
        upload_gtfs_zip_to_s3(mock_response)

        # Assert
        mock_s3_client.upload_fileobj.assert_not_called()


class TestLambdaHandler(unittest.TestCase):
    """Class for testing handler function."""

    @patch("lambdas.gtfs_data_fetch.main.update_last_modified_time")
    @patch("lambdas.gtfs_data_fetch.main.upload_gtfs_zip_to_s3")
    @patch("lambdas.gtfs_data_fetch.main.get_last_modified_time")
    @patch("lambdas.gtfs_data_fetch.main.requests.get")
    @patch("lambdas.gtfs_data_fetch.main.dotenv.load_dotenv")
    def test_handler_success_update_gtfs_files(
        self,
        mock_load_dotenv,
        mock_requests_get,
        mock_get_last_modified,
        mock_upload,
        mock_update,
    ):
        """Tests successful handler execution when GTFS files are updated."""
        # Arrange
        mock_event = {"test": "event"}
        mock_context = MagicMock()

        mock_response = MagicMock()
        mock_response.headers = {"Last-Modified": "Mon, 23 Feb 2026 15:00:00 GMT"}
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        mock_get_last_modified.return_value = "2026-01-01T12:00:00"

        # Act
        result = handler(mock_event, mock_context)

        # Assert
        mock_load_dotenv.assert_called_once()
        mock_requests_get.assert_called_once_with(
            "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
        )
        mock_get_last_modified.assert_called_once()
        mock_upload.assert_called_once_with(response=mock_response)
        mock_update.assert_called_once_with("2026-02-23T15:00:00")
        self.assertEqual(
            result,
            {
                "status": "updated",
                "message": "GTFS data has been updated and stored in S3.",
            },
        )

    @patch("lambdas.gtfs_data_fetch.main.update_last_modified_time")
    @patch("lambdas.gtfs_data_fetch.main.upload_gtfs_zip_to_s3")
    @patch("lambdas.gtfs_data_fetch.main.get_last_modified_time")
    @patch("lambdas.gtfs_data_fetch.main.requests.get")
    @patch("lambdas.gtfs_data_fetch.main.dotenv.load_dotenv")
    def test_handler_success_no_update(
        self,
        mock_load_dotenv,
        mock_requests_get,
        mock_get_last_modified,
        mock_upload,
        mock_update,
    ):
        """Tests successful handler execution when no update occurs."""
        # Arrange
        mock_event = {"test": "event"}
        mock_context = MagicMock()

        mock_response = MagicMock()
        mock_response.headers = {"Last-Modified": "Mon, 23 Feb 2026 15:00:00 GMT"}
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        mock_get_last_modified.return_value = "2026-02-24T15:00:00"

        # Act
        result = handler(mock_event, mock_context)

        # Assert
        mock_load_dotenv.assert_called_once()
        mock_requests_get.assert_called_once_with(
            "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
        )
        mock_get_last_modified.assert_called_once()
        mock_upload.assert_not_called()
        mock_update.assert_not_called()
        self.assertEqual(
            result,
            {
                "status": "no_update",
                "message": "GTFS data has not been updated since last fetch.",
            },
        )

    @patch("lambdas.gtfs_data_fetch.main.requests.get")
    @patch("lambdas.gtfs_data_fetch.main.dotenv.load_dotenv")
    def test_handler_no_modified_header(self, mock_load_dotenv, mock_requests_get):
        """Tests handler fails when no Last-Modified header is present."""
        # Arrange
        mock_event = {"test": "event"}
        mock_context = MagicMock()

        mock_response = MagicMock()
        mock_response.headers = {}  # No Last-Modified header
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        # Act + Assert
        with pytest.raises(
            ValueError, match="Last-Modified header not found in the response"
        ):
            handler(mock_event, mock_context)

        mock_load_dotenv.assert_called_once()
        mock_requests_get.assert_called_once_with(
            "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
        )

    @patch("lambdas.gtfs_data_fetch.main.requests.get")
    @patch("lambdas.gtfs_data_fetch.main.dotenv.load_dotenv")
    def test_handler_request_error(self, mock_load_dotenv, mock_requests_get):
        """Tests handler raises error if the GET request fails."""
        # Arrange
        mock_event = {"test": "event"}
        mock_context = MagicMock()

        mock_requests_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError("404 Not Found")
        )

        # Act + Assert
        with pytest.raises(requests.exceptions.HTTPError, match="404 Not Found"):
            handler(mock_event, mock_context)

        mock_load_dotenv.assert_called_once()
        mock_requests_get.assert_called_once_with(
            "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
        )
