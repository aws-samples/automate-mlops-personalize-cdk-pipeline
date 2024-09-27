#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this
#  software and associated documentation files (the "Software"), to deal in the Software
#  without restriction, including without limitation the rights to use, copy, modify,
#  merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#  PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Module for defining the AWS Step Functions state machine and tasks for creating and managing
a Schema in Amazon Personalize.

This module defines two main classes:

1. SchemaFlow: Represents the overall flow for creating and monitoring the status of a
   Schema in Amazon Personalize. It inherits from the BaseFlow class and defines the state
   machine for the Schema flow.

2. SchemaStep: Represents a step in the Schema flow for Amazon Personalize. It inherits
   from the BaseStep class and defines the specific steps for creating, describing, and
   monitoring the status of a Schema.

"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks, Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class SchemaFlow(BaseFlow):
    """
    A class that represents the flow for creating and managing a Schema in Amazon Personalize.

    This class inherits from the BaseFlow class and defines the state machine for creating and
    monitoring the status of a Schema in Amazon Personalize.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the SchemaFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope

        self.schema_step = SchemaStep(self.scope)

        if SchemaFlow.state_machine is None:
            SchemaFlow.state_machine = self.build_flow(self.schema_step)

    def build(self):
        """
        Builds and returns the task for executing the state machine for the Schema flow.

        Returns:
            Task: The task for executing the state machine.
        """
        return self.schema_step.task(SchemaFlow.state_machine)


class SchemaStep(BaseStep):
    """
    A class that represents a step in the Schema flow for AWS Personalize.

    This class inherits from the BaseStep class and defines the specific steps for creating,
    describing, and monitoring the status of a Schema in AWS Personalize.
    """
    SERVICE = "personalize"
    object_type = "Schema"
    STEP_NAME = "SchemaTask"

    def __init__(self, scope):
        """
        Initializes a new instance of the SchemaStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
        """
        super().__init__(scope)
        self.scope = scope

        self.id = SchemaStep.STEP_NAME
        self.create_step_arn_path = "schemaArn"

        self.result_path_describe = "$.DescribeSchema"
        self.result_path_create = "$.CreateSchema"

        self.result_path_error = "$.DescribeSchema.Error"
        self.result_path = f"$.{SchemaStep.STEP_NAME}"

        self.element_exists_check = "Item.schema"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.Schema.SchemaArn",
            "message": "Personalize Schema status change",
            "detail": {
                "Schema.$": f"{self.result_path_describe}.Schema.Schema",
                "Name.$": f"{self.result_path_describe}.Schema.Name"

            },
            "detailType": "Personalize Schema status change",
        }

    def condition_success(self):
        """
        Returns a condition that checks if the Schema creation was successful.

        Returns:
            Condition: The condition that checks if the Schema creation was successful.
        """
        return sfn.Condition.is_not_null(
            self.result_path_describe + "." + SchemaStep.object_type + ".SchemaArn")

    def condition_failure(self):
        """
        Returns a condition that checks if the Schema creation failed.

        Returns:
            Condition: The condition that checks if the Schema creation failed.
        """
        return sfn.Condition.is_null(
            self.result_path_describe + "." + SchemaStep.object_type + ".SchemaArn")

    def describe(self):
        """
        Creates a step that describes the Schema in AWS Personalize.

        Returns:
            Task: The task that describes the Schema.
        """
        payload = {
            "action": "describeSchema",
            "service": SchemaStep.SERVICE,
            "id": "DescribeSchema",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "SchemaArn.$": "States.Format('arn:aws:personalize:{}:{}:schema/{}-{}-{}',"
                               "$.Region,$.AccountID,$.DatasetGroup.serviceConfig.name,"
                               "$.Item.schema.serviceConfig.name,$.Item.schema.schemaVersion)"
            },

            "object_type": SchemaStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize Schemas.

        Args:
            state_machine (StateMachine): The state machine for the Schema flow.

        Returns:
            Task: The task that starts the execution of the state machine.
        """
        if state_machine is None:
            raise Exception("State Machine None, it is not initialized")
        task = tasks.StepFunctionsStartExecution(self.scope, self.id,
                                                 state_machine=state_machine,
                                                 result_path=self.result_path,
                                                 integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                                 input=sfn.TaskInput.from_object({
                                                     "Region": self.region,
                                                     "AccountID": self.account_id,
                                                     "Type": self.object_type,
                                                     "DatasetGroup":
                                                         sfn.JsonPath.object_at("$.DatasetGroup"),
                                                     "Item": sfn.JsonPath.object_at("$.Item")

                                                 }),
                                                 result_selector={
                                                     "SchemaArn.$": "$..Output[?(@.DescribeSchema)].DescribeSchema"
                                                                    ".Schema.SchemaArn",
                                                     "ExecutionArn.$": "$.ExecutionArn",
                                                     "StateMachineStatus.$": "$.Status",
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.Input.DatasetGroup"),
                                                     "Item": sfn.JsonPath.object_at("$.Input.Item")

                                                 },
                                                 output_path=self.result_path
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
