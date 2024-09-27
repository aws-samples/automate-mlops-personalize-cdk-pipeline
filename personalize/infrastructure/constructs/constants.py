class Constants:
    AWS_SOLUTIONS_IAM5_REASON = (
        "These are added directly by Stepfunctions when tasks.StepFunctionsStartExecution is "
        "added to the step function, the arn is added with a * automatically at the end of the "
        "state machine which is to be executed like arn:aws:states:us-east-1: "
        ":execution:mlops-personalize-batchinferencejob-sm*, this is also added for a lambda "
        "function as it needs access to create the specified personalize resource")

    AWS_SOLUTIONS_IAM4_REASON = ("Uses AWSLambdaBasicExecutionRole"
                                 "Basic Policy required for"
                                 "Providing write permissions "
                                 "to CloudWatch Logs.")
