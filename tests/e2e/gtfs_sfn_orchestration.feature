Feature: Run GTFS Lambda orchestrator Step Function

    Scenario: Orchestrator runs both Lambda functions
        Given new GTFS data is available
        And existing GTFS raw data is present in S3
        And existing GTFS processed schedule data is present in S3
        When we trigger the GTFS lambda orchestrator
        Then the orchestrator will have status SUCCEEDED
        And new GTFS raw data files will be created in S3
        And new GTFS processed schedule data will be created in S3

    Scenario: Orchestrator runs first Lambda function and skips to end
        Given new GTFS data is not available
        And existing GTFS raw data is present in S3
        And existing GTFS processed schedule data is present in S3
        When we trigger the GTFS lambda orchestrator
        Then the orchestrator will have status SUCCEEDED
        And new GTFS raw data files will not be created in S3
        And new GTFS processed schedule data will not be created in S3

    Scenario: Orchestrator encounters error and sends error notification
        Given new GTFS data is not available
        And existing GTFS raw data is present in S3
        And existing GTFS processed schedule data is present in S3
        And we are subscribed to orchestrator failure notifications
        And we update the gtfs-data-fetch lambda to remove environment variables
        When we trigger the GTFS lambda orchestrator
        Then the orchestrator will have status SUCCEEDED
        And new GTFS raw data files will not be created in S3
        And new GTFS processed schedule data will not be created in S3
        And an error notification will be sent