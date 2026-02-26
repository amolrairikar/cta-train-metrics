Feature: Run Lambda function to fetch CTA train locations

    Scenario: Lambda runs successfully
        Given the CTA Train Locations API is available
        When we trigger the cta-get-train-locations lambda function
        Then the function will succeed

    Scenario: Lambda fails while executing
        Given the CTA Train Locations API is available
        And we update the cta-get-train-locations lambda to remove environment variables
        When we trigger the cta-get-train-locations lambda function
        Then the function will fail