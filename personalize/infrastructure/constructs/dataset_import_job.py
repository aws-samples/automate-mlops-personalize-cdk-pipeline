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

This module defines the DatasetImportJobFlow and DatasetImportJobStep classes, which are responsible
for managing the lifecycle of Amazon Personalize dataset import jobs using AWS Step Functions.

The DatasetImportJobFlow class orchestrates the creation and management of dataset import jobs,
while the DatasetImportJobStep class encapsulates the logic for individual steps in the process.

"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks, Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class DatasetImportJobFlow(BaseFlow):
    """
   Manages the lifecycle of an Amazon Personalize dataset import job using AWS Step Functions.

   This class orchestrates the creation and management of dataset import jobs by building
   a Step Functions state machine with the necessary steps.

   Attributes:
       state_machine (sfn.StateMachine): The Step Functions state machine for managing dataset import jobs.
   """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the DatasetImportJobFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope

        self.dataset_import_job_step = DatasetImportJobStep(self.scope)

        if DatasetImportJobFlow.state_machine is None:
            DatasetImportJobFlow.state_machine = self.build_flow(self.dataset_import_job_step)

    def build(self):
        """
        Builds the Step Functions task for managing dataset import jobs.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing dataset import jobs.
        """
        return self.dataset_import_job_step.task(DatasetImportJobFlow.state_machine)

    def build_definition(self, step):
        """
       Builds the Step Functions state machine definition for managing dataset import jobs.

       Args:
           step (DatasetImportJobStep): The DatasetImportJobStep instance.

       Returns:
           sfn.Choice: The Step Functions choice state that determines whether to execute the dataset import job.
       """
        create_step = step.create(step.STEP_NAME, step.result_path_create)
        describe_step = step.describe(f"{step.result_path_create}.Payload.response.datasetImportJobArn")

        element_exists_condition = sfn.Condition.is_present("$.Item.datasetImportJob")
        should_run_dataset_import_job = sfn.Condition.boolean_equals("$.Item.datasetImportJob.createNewJob", True)
        update_dataset_arn_exists_condition = sfn.Condition.is_present("$.UpdateDatasetArn[0]")
        create_dataset_arn_exists_condition = sfn.Condition.is_present("$.CreateDatasetArn[0]")

        or_condition = sfn.Condition.or_(should_run_dataset_import_job, update_dataset_arn_exists_condition,
                                         create_dataset_arn_exists_condition)

        should_execute_step = sfn.Choice(self.scope, step.object_type + " should execute workflow?").when(
            sfn.Condition.and_(element_exists_condition, or_condition),
            create_step).otherwise(
            step.exit_step())

        create_step.next(
            step.wait("Wait After Create").next(describe_step))

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.send_event().next(sfn.Pass(self.scope, step.object_type + " End"))).otherwise(
                step.wait("Wait After Describe").
                next(describe_step)))

        return should_execute_step


class DatasetImportJobStep(BaseStep):
    """
    Encapsulates the logic for individual steps in the dataset import job management process.

    This class defines the steps required to create, describe, and manage dataset import jobs
    in Amazon Personalize.

    """
    STEP_NAME = "DatasetImportTask"
    SERVICE = "personalize"
    object_type = "DatasetImportJob"

    def __init__(self, scope):
        """
        Initializes a new instance of the DatasetImportJobStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
        """
        super().__init__(scope)
        self.scope = scope

        self.id = DatasetImportJobStep.STEP_NAME
        self.create_step_arn_path = "CreateDatasetImportJob"

        self.result_path = f"$.{DatasetImportJobStep.STEP_NAME}"
        self.result_path_describe = "$.DescribeDatasetImportJob"
        self.result_path_create = "$.CreateDatasetImportJob"
        self.result_path_error = "$.DescribeDatasetImportJob.Error"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.DatasetImportJob.DatasetImportJobArn",
            "statusPath": f"{self.result_path_describe}.DatasetImportJob.Status",
            "message": "Personalize DatasetImportJob status change",
            "detail": {
                "DatasetArn.$": f"{self.result_path_describe}.DatasetImportJob.DatasetArn",
                "DataLocation.$": f"{self.result_path_describe}.DatasetImportJob.DataSource.DataLocation",
                "ImportMode.$": f"{self.result_path_describe}.DatasetImportJob.ImportMode",
                "JobName.$": f"{self.result_path_describe}.DatasetImportJob.JobName"

            },
            "detailType": "Personalize DatasetImportJob status change",
        }

    def condition_success(self):
        """
        Defines the condition for a successful dataset import job creation or update.

        Returns:
            sfn.Condition: The condition that checks if the dataset import job status is 'ACTIVE'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + DatasetImportJobStep.object_type + ".Status", "ACTIVE")

    def exit_step(self, message="Step data not present, skipping step"):
        """
        Defines the exit step definition for the state machine flow.

        Returns:
            sfn.Condition: The condition that checks if the dataset import job status is 'CREATE FAILED'.
        """
        return sfn.Succeed(self.scope, f"{self.object_type}  {message}")

    def condition_failure(self):
        """
        Defines the condition for a failed dataset import job creation or update.

        Returns:
            sfn.Condition: The condition that checks if the dataset import job status is 'CREATE FAILED'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + DatasetImportJobStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self, dataset_import_arn):
        """
        Defines the step for describing a dataset import job.

        Args:
            dataset_import_arn (str): The Amazon Resource Name (ARN) of the dataset import job.

        Returns:
            sfn.Task: The Step Functions task for describing a dataset import job.
        """
        payload = {
            "action": "describeDatasetImportJob",
            "service": DatasetImportJobStep.SERVICE,
            "id": "DescribeDatasetImportJob",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "DatasetImportJobArn.$": dataset_import_arn
            },

            "object_type": DatasetImportJobStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize DatasetImportStep.

        Args:
            state_machine (sfn.StateMachine): The Step Functions state machine for managing dataset import jobs.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing dataset import jobs.
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
                                                     "Item": sfn.JsonPath.object_at("$.Item"),
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.DatasetGroup"),
                                                     "UpdateDatasetArn": sfn.JsonPath.object_at('$.UpdateDatasetArn'),
                                                     "CreateDatasetArn": sfn.JsonPath.object_at('$.CreateDatasetArn'),

                                                 }),
                                                 result_selector={
                                                     "DatasetImportJobStatus.$": "$..Output"
                                                                                 "[?(@.DescribeDatasetImportJob)]"
                                                                                 ".DescribeDatasetImportJob"
                                                                                 ".DatasetImportJob.Status",
                                                     "DatasetDescribeImportJobArn.$": "$..Output"
                                                                                      "[?(@.DescribeDatasetImportJob)]"
                                                                                      ".DescribeDatasetImportJob"
                                                                                      ".DatasetImportJob"
                                                                                      ".DatasetImportJobArn",
                                                     "DatasetCreateImportJobArn.$": "$..Output"
                                                                                    f"[?(@.{self.result_path_create})]"
                                                                                    f".{self.result_path_create}"
                                                                                    ".Payload.response"
                                                                                    ".datasetImportJobArn",
                                                     "ImportMode.$": "$..Output"
                                                                     "[?(@.DescribeDatasetImportJob)]"
                                                                     ".DescribeDatasetImportJob"
                                                                     ".DatasetImportJob"
                                                                     ".ImportMode",
                                                     "ExecutionArn.$": "$.ExecutionArn",
                                                     "StateMachineStatus.$": "$.Status",
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.Input.DatasetGroup")

                                                 },
                                                 output_path=self.result_path
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
