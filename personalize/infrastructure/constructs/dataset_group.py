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

This module defines the DatasetGroupFlow and DatasetGroupStep classes, which are responsible
for managing the lifecycle of Amazon Personalize dataset groups using AWS Step Functions.

The DatasetGroupFlow class orchestrates the creation and management of dataset groups,
while the DatasetGroupStep class encapsulates the logic for individual steps in the process.

"""
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks, Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class DatasetGroupFlow(BaseFlow):
    """
    Manages the lifecycle of an Amazon Personalize dataset group using AWS Step Functions.

    This class orchestrates the creation and management of dataset groups by building
    a Step Functions state machine with the necessary steps.

    Attributes:
        state_machine (sfn.StateMachine): The Step Functions state machine for managing dataset groups.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the DatasetGroupFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.dataset_group_step = DatasetGroupStep(self.scope)

        if DatasetGroupFlow.state_machine is None:
            DatasetGroupFlow.state_machine = self.build_flow(self.dataset_group_step)

    def build(self):
        """
        Builds the Step Functions task for managing dataset groups.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing dataset groups.
        """
        return self.dataset_group_step.task(DatasetGroupFlow.state_machine)


class DatasetGroupStep(BaseStep):
    """
   Encapsulates the logic for individual steps in the dataset group management process.

   This class defines the steps required to create, describe, and manage dataset groups
   in Amazon Personalize.

   """
    SERVICE = "personalize"
    object_type = "DatasetGroup"
    STEP_NAME = "DatasetGroupTask"

    def __init__(self, scope):
        """
       Initializes a new instance of the DatasetGroupStep class.

       Args:
           scope (Construct): The scope in which this construct is created.
       """
        super().__init__(scope)
        self.scope = scope
        self.id = DatasetGroupStep.STEP_NAME
        self.create_step_arn_path = "datasetGroupArn"

        self.element_exists_check = "ServiceConfig.name"

        self.result_path_describe = "$.DescribeDatasetGroup"
        self.result_path_create = "$.CreateDatasetGroup"

        self.result_path_error = "$.DescribeDatasetGroup.Error"
        self.result_path = f"$.{DatasetGroupStep.STEP_NAME}"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.DatasetGroup.DatasetGroupArn",
            "statusPath": f"{self.result_path_describe}.DatasetGroup.Status",
            "message": "Personalize DatasetGroup status change",
            "detail": {
                "Name.$": f"{self.result_path_describe}.DatasetGroup.Name"

            },
            "detailType": "Personalize DatasetGroup status change",
        }

    def condition_success(self):
        """
        Defines the condition for a successful dataset group creation or update.

        Returns:
            sfn.Condition: The condition that checks if the dataset group status is 'ACTIVE'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + DatasetGroupStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
        Defines the condition for a failed dataset group creation or update.

        Returns:
            sfn.Condition: The condition that checks if the dataset group status is 'CREATE FAILED'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + DatasetGroupStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
       Defines the step for describing an existing dataset group.

       Returns:
           sfn.Task: The Step Functions task for describing a dataset group.
       """
        payload = {
            "action": "describeDatasetGroup",
            "service": DatasetGroupStep.SERVICE,
            "id": "DescribeDatasetGroup",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "DatasetGroupArn.$": "States.Format('arn:aws:personalize:{}:{}:dataset-group/{}',$.Region,"
                                     "$.AccountID,$.ServiceConfig.name)",
            },

            "object_type": DatasetGroupStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize DatasetGroup.

        Args:
            state_machine (sfn.StateMachine): The Step Functions state machine for managing dataset groups.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing dataset groups.
        """
        task = tasks.StepFunctionsStartExecution(self.scope, self.id,
                                                 state_machine=state_machine,
                                                 result_path=self.result_path,
                                                 integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                                 input=sfn.TaskInput.from_object({
                                                     "Region": self.region,
                                                     "AccountID": self.account_id,
                                                     "Type": self.object_type,
                                                     "ServiceConfig": sfn.JsonPath.string_at(
                                                         "$.datasetGroup.serviceConfig")

                                                 }),
                                                 result_selector={
                                                     "DatasetGroupArn.$": "$.Output.DescribeDatasetGroup.DatasetGroup"
                                                                          ".DatasetGroupArn",
                                                     "DatasetGroupStatus.$": "$.Output.DescribeDatasetGroup"
                                                                             ".DatasetGroup.Status",
                                                     "ExecutionArn.$": "$.ExecutionArn",
                                                     "Status.$": "$.Status",

                                                 }

                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
