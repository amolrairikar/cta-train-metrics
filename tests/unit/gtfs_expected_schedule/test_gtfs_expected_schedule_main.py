"""Module for testing main.py in gtfs_expected_schedule lambda."""

import unittest
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from lambdas.gtfs_expected_schedule.main import (
    read_gtfs_data,
    processs_gtfs_data,
    save_to_s3,
    handler,
)


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Fixture to mock environment variables."""
    monkeypatch.setenv("ACCOUNT_NUMBER", "123456789012")


class TestReadGtfsData(unittest.TestCase):
    """Class for testing read_gtfs_data function."""

    @patch("lambdas.gtfs_expected_schedule.main.pd.read_csv")
    @patch("lambdas.gtfs_expected_schedule.main.boto3.client")
    def test_read_gtfs_data_success(self, mock_boto3_client, mock_read_csv):
        """Tests successfully reading GTFS data from S3."""
        # Arrange
        mock_read_csv.return_value = MagicMock()
        mock_boto3_client.return_value = MagicMock()
        mock_boto3_client.return_value.get_object.return_value = MagicMock()

        # Act
        result = read_gtfs_data()

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(mock_read_csv.call_count, 5)
        self.assertEqual(mock_boto3_client.return_value.get_object.call_count, 5)

    @patch("lambdas.gtfs_expected_schedule.main.boto3.client")
    def test_read_gtfs_data_failure(self, mock_boto3_client):
        """Tests failing to read GTFS data from S3."""
        # Arrange
        mock_boto3_client.return_value = MagicMock()
        mock_boto3_client.return_value.get_object.side_effect = (
            botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Object not found"}},
                "GetObject",
            )
        )

        # Act & Assert
        with pytest.raises(botocore.exceptions.ClientError):
            read_gtfs_data()

        self.assertEqual(mock_boto3_client.return_value.get_object.call_count, 1)
        mock_boto3_client.return_value.get_object.assert_called_once_with(
            Bucket="123456789012-cta-analytics-project", Key="gtfs_data/calendar.txt"
        )


class TestProcessGtfsData(unittest.TestCase):
    """Class for testing processs_gtfs_data function."""

    @patch("lambdas.gtfs_expected_schedule.main.pd.merge")
    def test_processs_gtfs_data(
        self,
        mock_merge,
    ):
        """Tests successful processing of GTFS data."""
        # Arrange
        dataframes = {
            "calendar": MagicMock(),
            "routes": MagicMock(),
            "stops": MagicMock(),
            "stop_times": MagicMock(),
            "trips": MagicMock(),
        }

        for df in dataframes.values():
            df.drop.return_value = MagicMock()
            df.query.return_value = MagicMock()
            df.query.return_value.reset_index.return_value = MagicMock()

        mock_merge.return_value = MagicMock()
        mock_merge.return_value.drop.return_value = MagicMock()

        # Act
        result = processs_gtfs_data(dataframes=dataframes)

        # Assert
        self.assertIsNotNone(result)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], MagicMock)
        self.assertIsInstance(result[1], MagicMock)


class TestSaveToS3(unittest.TestCase):
    """Class for testing save_to_s3 function."""

    @patch("lambdas.gtfs_expected_schedule.main.boto3.client")
    def test_save_to_s3_success(self, mock_boto3_client):
        """Tests successful saving of dataframe to S3."""
        # Arrange
        df = MagicMock()
        df.to_csv.return_value = "csv_data"
        mock_boto3_client.return_value.put_object.return_value = None

        # Act
        save_to_s3(df=df, schedule_effective_date="20260101")

        # Assert
        df.to_csv.assert_called_once_with(index=False)
        mock_boto3_client.return_value.put_object.assert_called_once_with(
            Bucket="123456789012-cta-analytics-project",
            Key="gtfs_expected_cta_schedule/20260101.csv",
            Body="csv_data",
        )

    @patch("lambdas.gtfs_expected_schedule.main.boto3.client")
    def test_save_to_s3_failure(self, mock_boto3_client):
        """Tests failure with saving dataframe to S3."""
        # Arrange
        df = MagicMock()
        df.to_csv.return_value = "csv_data"
        mock_boto3_client.return_value.put_object.side_effect = (
            botocore.exceptions.ClientError(
                error_response={
                    "Error": {"Code": "NoSuchKey", "Message": "Object not found"}
                },
                operation_name="PutObject",
            )
        )

        # Act
        with pytest.raises(botocore.exceptions.ClientError):
            save_to_s3(df=df, schedule_effective_date="20260101")

        # Assert
        df.to_csv.assert_called_once_with(index=False)
        mock_boto3_client.return_value.put_object.assert_called_once_with(
            Bucket="123456789012-cta-analytics-project",
            Key="gtfs_expected_cta_schedule/20260101.csv",
            Body="csv_data",
        )


class TestHandler(unittest.TestCase):
    """Class for testing handler function."""

    @patch("lambdas.gtfs_expected_schedule.main.read_gtfs_data")
    @patch("lambdas.gtfs_expected_schedule.main.processs_gtfs_data")
    @patch("lambdas.gtfs_expected_schedule.main.save_to_s3")
    def test_handler_success(
        self, mock_save_to_s3, mock_process_gtfs_data, mock_read_gtfs_data
    ):
        """Tests successful handler execution."""
        # Arrange
        mock_read_gtfs_data.return_value = [MagicMock(), MagicMock()]
        mock_process_gtfs_data.return_value = (MagicMock(), "20260101")
        mock_save_to_s3.return_value = None

        # Act
        handler(event={}, context=None)

        # Assert
        mock_read_gtfs_data.assert_called_once()
        mock_process_gtfs_data.assert_called_once_with(
            dataframes=[
                mock_read_gtfs_data.return_value[0],
                mock_read_gtfs_data.return_value[1],
            ]
        )
        mock_save_to_s3.assert_called_once_with(
            df=mock_process_gtfs_data.return_value[0],
            schedule_effective_date="20260101",
        )
