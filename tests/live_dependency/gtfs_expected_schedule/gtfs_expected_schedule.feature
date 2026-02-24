Feature: Create expected schedule of CTA train runs from GTFS data

    Scenario: Successfully create expected schedule
        Given GTFS data is available
        When we process the data
        Then the expected schedule should be saved to S3
