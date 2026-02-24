Feature: Run GTFS Lambda orchestrator Step Function

    Scenario: Successfully run GTFS Lambda orchestrator
        Given the orchestrator is available
        When we run the GTFS Lambda orchestrator
        Then the step function should complete successfully