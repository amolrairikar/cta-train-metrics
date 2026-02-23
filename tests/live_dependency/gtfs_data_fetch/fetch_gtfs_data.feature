Feature: Fetch GTFS data

    Scenario: New GTFS data available
        Given we query GTFS data
        and a new version of the data is available
        When we fetch the data
        Then the data should be saved to S3
        and the last modified parameter should be updated

    Scenario: No new GTFS data available
        Given we query GTFS data
        and a new version of the data is not available
        When we fetch the data
        Then the data should not be saved to S3
        and the last modified parameter should not be updated
