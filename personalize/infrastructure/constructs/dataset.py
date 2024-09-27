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
This module defines classes for creating and managing Amazon Personalize Datasets
using AWS Step Functions and AWS CDK.
"""
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class DatasetFlow(BaseFlow):
    """
    A class representing the flow for creating and managing a Personalize dataset.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
       Initialize the DatasetFlow instance.

       Args:
           scope (Construct): The scope in which this construct is created.
           construct_id (str): The unique identifier for this construct.
       """
        super().__init__(scope, construct_id)
        self.scope = scope

        self.dataset_step = DatasetStep(self.scope)

        if DatasetFlow.state_machine is None:
            DatasetFlow.state_machine = self.build_flow(self.dataset_step)

    def build(self):
        """
        Build and return the Step Functions task for the DatasetFlow.

        Returns:
            Task: The Step Functions task for the DatasetFlow.
        """
        return self.dataset_step.task(DatasetFlow.state_machine)

    def build_definition(self, step):
        """
        Build the definition for the DatasetFlow state machine.

        Args:
            step (DatasetStep): The DatasetStep instance.

        Returns:
            Choice: The Choice state representing the entry point of the state machine.
        """
        create_step = step.create(step.id, step.result_path_create)

        exit_step = step.exit_step()

        describe_step_after_create = step.describe("DescribeDatasetAfterCreate")

        dataset_name_present_condition = sfn.Condition.is_present(step.element_exists_check)

        should_execute_step = sfn.Choice(self.scope, step.object_type + " should execute workflow?").when(
            dataset_name_present_condition,
            create_step).otherwise(
            exit_step)

        create_step.next(
            step.wait("Wait After Create").next(describe_step_after_create))

        describe_step_after_create.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.send_event().next(sfn.Pass(self.scope, step.object_type + " End"))).otherwise(
                step.wait("Wait After Describe").
                next(describe_step_after_create)))

        return should_execute_step


class DatasetStep(BaseStep):
    """
    A class representing a step in the Personalize dataset creation and management process.
    """
    SERVICE = "personalize"
    STEP_NAME = "DatasetTask"
    object_type = "Dataset"

    def __init__(self, scope):
        """
       Initialize the DatasetStep instance.

       Args:
           scope (Construct): The scope in which this construct is created.
       """
        super().__init__(scope)
        self.scope = scope

        self.id = DatasetStep.STEP_NAME
        self.create_step_arn_path = "CreateDataset"

        self.element_exists_check = "$.Item.dataset"
        self.schema_arn_exists_check = "$.SchemaArn[0]"

        self.result_path_describe = "$.DescribeDatasetAfterCreate"
        self.result_path_create = "$.CreateDataset"
        self.result_path_update = "$.UpdateDataset"
        self.result_path_error = "$.DescribeDataset.Error"
        self.result_path = f"$.{DatasetStep.STEP_NAME}"
        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.Dataset.DatasetArn",
            "statusPath": f"{self.result_path_describe}.Dataset.Status",
            "message": "Personalize Dataset status change",
            "detail": {
                "DatasetGroupArn.$": f"{self.result_path_describe}.Dataset.DatasetGroupArn",
                "DatasetType.$": f"{self.result_path_describe}.Dataset.DatasetType",
                "SchemaArn.$": f"{self.result_path_describe}.Dataset.SchemaArn",
                "Name.$": f"{self.result_path_describe}.Dataset.Name"

            },
            "detailType": "Personalize Dataset status change",
        }

    def condition_success(self):
        """
       Define the condition for a successful dataset creation.

       Returns:
           Condition: The condition representing a successful dataset creation.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + DatasetStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
        Define the condition for a failed dataset creation.

        Returns:
            Condition: The condition representing a failed dataset creation.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + DatasetStep.object_type + ".Status",
            "CREATE FAILED")

    def update(self):
        """
       Create a Step Functions step for updating a Personalize dataset.

       Returns:
           Task: The Step Functions step for updating a Personalize dataset.
       """
        payload = {
            "action": "updateDataset",
            "service": DatasetStep.SERVICE,
            "id": "UpdateDataset",
            "iam_resources": ["*"],
            "result_path": self.result_path_update,
            "parameters": {
                "DatasetArn.$": "States.Format('arn:aws:personalize:{}:{}:dataset/{}/{}',$.Region,$.AccountID,"
                                "$.DatasetGroup.serviceConfig.name,$.Item.type)",
                "SchemaArn.$": "$.SchemaArn[0]"
            },
            "object_type": DatasetStep.object_type
        }

        return self.create_call_aws_service_step(payload)

    def describe(self, construct_id):
        """
        Create a Step Functions step for describing a Personalize dataset.

        Args:
            construct_id (str): The unique identifier for the construct.

        Returns:
            Task: The Step Functions task for describing a Personalize dataset.
        """
        payload = {
            "action": "describeDataset",
            "service": DatasetStep.SERVICE,
            "id": construct_id,
            "iam_resources": ["*"],
            "result_path": f"$.{construct_id}",
            "parameters": {
                "DatasetArn.$": "States.Format('arn:aws:personalize:{}:{}:dataset/{}/{}',$.Region,$.AccountID,"
                                "$.DatasetGroup.serviceConfig.name,$.Item.type)"
            },

            "object_type": DatasetStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize Datasets.

       Args:
           state_machine (StateMachine): The Step Functions state machine.

       Returns:
           Task: The Step Functions task step for the DatasetStep.

       Raises:
           Exception: If the state machine is not initialized.
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
                                                     "SchemaArn.$": "$.SchemaArn",
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.DatasetGroup"),
                                                     "Item": sfn.JsonPath.object_at("$.Item")

                                                 }),
                                                 result_selector={
                                                     "Item": sfn.JsonPath.object_at("$.Input.Item"),
                                                     "DatasetStatus.$": "$..Output[?("
                                                                        "@.DescribeDatasetAfterCreate"
                                                                        ")].DescribeDatasetAfterCreate.Dataset"
                                                                        ".Status",
                                                     "UpdateDatasetArn.$": "$..Output[?(@.UpdateDataset)].UpdateDataset"
                                                                           ".DatasetArn",
                                                     "CreateDatasetArn.$": "$..Output[?(@.CreateDataset)].CreateDataset"
                                                                           ".Payload.response.datasetArn",

                                                     "ExecutionArn.$": "$.ExecutionArn",
                                                     "StateMachineStatus.$": "$.Status",
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.Input.DatasetGroup"),
                                                 },
                                                 output_path=self.result_path
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
