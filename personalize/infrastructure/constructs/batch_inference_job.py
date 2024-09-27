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
This module defines classes for creating and managing Amazon Personalize Batch Inference Jobs
using AWS Step Functions and AWS CDK.
"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks, Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class BatchInferenceJobFlow(BaseFlow):
    """
    A class that represents the flow for creating and managing Amazon Personalize Batch Inference Jobs.

    This class inherits from the BaseFlow class and is responsible for building the state machine
    definition for the Batch Inference Job flow.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initialize a new instance of the BatchInferenceJobFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.batch_inference_job_step = BatchInferenceJobStep(self.scope)

        if BatchInferenceJobFlow.state_machine is None:
            BatchInferenceJobFlow.state_machine = self.build_flow(self.batch_inference_job_step)

    def build(self):
        """
        Build the task for the Batch Inference Job flow.

        Returns:
            The task for the Batch Inference Job flow.
        """
        return self.batch_inference_job_step.task(BatchInferenceJobFlow.state_machine)

    def build_definition(self, step):
        """
        Build the state machine definition for the Batch Inference Job flow.

        Args:
            step (BatchInferenceJobStep): The step object representing the Batch Inference Job.

        Returns:
            The state machine definition for the Batch Inference Job flow.
        """
        create_step = step.create(step.id, step.result_path_create)
        exit_step = step.exit_step()

        describe_step = step.describe()

        is_solution_version_present_condition = sfn.Condition.is_present("$.SolutionVersionArn[0]")
        solution_version_arn_exists_condition = sfn.Condition.is_present("$.ServiceConfig.solutionVersionArn")
        new_batch_job_creation_condition = sfn.Condition.boolean_equals("$.CreateBatchInferenceJob", True)

        should_create_new_batch_job = sfn.Choice(self.scope, step.object_type + " create new job?").when(
            sfn.Condition.and_(
                sfn.Condition.or_(is_solution_version_present_condition, solution_version_arn_exists_condition),
                new_batch_job_creation_condition),
            create_step).otherwise(
            exit_step)

        create_step.next(
            step.wait("Wait After Create").next(describe_step))

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.send_event().next(exit_step)).otherwise(
                step.wait("Wait After Describe").
                next(describe_step)))

        return should_create_new_batch_job


class BatchInferenceJobStep(BaseStep):
    """
   A class that represents a step in the Batch Inference Job flow.

   This class inherits from the BaseStep class and is responsible for defining the configuration
   and behavior of the Batch Inference Job step.
   """
    STEP_NAME = "BatchInferenceTask"
    SERVICE = "personalize"
    object_type = "BatchInferenceJob"

    def __init__(self, scope):
        """
        Initialize a new instance of the BatchInferenceJobStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
        """
        super().__init__(scope)
        self.scope = scope
        self.id = BatchInferenceJobStep.STEP_NAME
        self.create_step_arn_path = "batchInferenceJobArn"

        self.result_path = f"$.{BatchInferenceJobStep.STEP_NAME}"
        self.result_path_describe = "$.DescribeBatchInferenceJob"
        self.result_path_create = "$.CreateBatchInferenceJob"
        self.result_path_error = "$.DescribeBatchInferenceJob.Error"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.BatchInferenceJob.BatchInferenceJobArn",
            "statusPath": f"{self.result_path_describe}.BatchInferenceJob.Status",
            "message": "Personalize BatchInferenceJob status change",
            "detail": {
                "SolutionVersionArn.$": f"{self.result_path_describe}.BatchInferenceJob.SolutionVersionArn",
                "JobName.$": f"{self.result_path_describe}.BatchInferenceJob.JobName",
                "JobInput.$": f"{self.result_path_describe}.BatchInferenceJob.JobInput.S3DataSource.Path",
                "JobOutput.$": f"{self.result_path_describe}.BatchInferenceJob.JobOutput.S3DataDestination.Path",

            },
            "detailType": "Personalize BatchInferenceJob status change",
        }

    def condition_success(self):
        """
        Define the condition for a successful Batch Inference Job.

        Returns:
            The condition for a successful Batch Inference Job.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + BatchInferenceJobStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
       Define the condition for checking a failed Batch Inference Job.

       Returns:
           The condition for a failed Batch Inference Job.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + BatchInferenceJobStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
        Create a step to describe the Batch Inference Job.

        Returns:
            The step to describe the Batch Inference Job.
        """
        payload = {
            "action": "describeBatchInferenceJob",
            "service": BatchInferenceJobStep.SERVICE,
            "id": "DescribeBatchInferenceJob",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "BatchInferenceJobArn.$": "$.CreateBatchInferenceJob.Payload.response.batchInferenceJobArn"
            },

            "object_type": BatchInferenceJobStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
        Create a step which executes a child state machine for a batch inference job.

        Args:
            state_machine (sfn.StateMachine): The state machine to which the task belongs.

        Returns:
            The task step for the Batch Inference Job step.

        Raises:
            Exception: If the state machine is not initialized.
        """
        if state_machine is None:
            raise Exception("State Machine None, it is not initialized")

        task = tasks.StepFunctionsStartExecution(self.scope, self.id,
                                                 state_machine=state_machine,
                                                 result_path=self.result_path,
                                                 output_path=self.result_path,
                                                 integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                                 input=sfn.TaskInput.from_object({
                                                     "Region": sfn.JsonPath.string_at("$.Region"),
                                                     "AccountID": sfn.JsonPath.string_at("$.AccountID"),
                                                     "Type": self.object_type,
                                                     "SolutionVersionArn": sfn.JsonPath.string_at(
                                                         "$.SolutionVersionArn"),
                                                     "ServiceConfig": sfn.JsonPath.string_at(
                                                         "$.BatchInferenceJob.serviceConfig"),
                                                     "CreateBatchInferenceJob": sfn.JsonPath.string_at(
                                                         "$.BatchInferenceJob.createBatchInferenceJob"),

                                                 }),
                                                 result_selector={
                                                     "BatchInferenceJobArn.$": "$..Output[?(@.DescribeBatchInferenceJob"
                                                                               ")].DescribeBatchInferenceJob"
                                                                               ".BatchInferenceJob.BatchInferenceJobArn",
                                                 }
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
