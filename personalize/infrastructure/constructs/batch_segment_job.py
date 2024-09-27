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
This module defines classes for creating and managing Amazon Personalize Batch Segment Jobs
using AWS Step Functions and AWS CDK.
"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks, Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class BatchSegmentJobFlow(BaseFlow):
    """
    A class that represents the flow for creating and managing Amazon Personalize Batch Segment Jobs.

    This class inherits from the BaseFlow class and is responsible for building the state machine
    definition for the Batch Segment Job flow.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initialize a new instance of the BatchSegmentJobFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """

        super().__init__(scope, construct_id)
        self.scope = scope
        self.batch_segment_job_step = BatchSegmentJobStep(self.scope)

        if BatchSegmentJobFlow.state_machine is None:
            BatchSegmentJobFlow.state_machine = self.build_flow(self.batch_segment_job_step)

    def build(self):
        """
           Build the task for the Batch Segment Job flow.

           Returns:
               The task for the Batch Segment Job flow.
        """
        return self.batch_segment_job_step.task(BatchSegmentJobFlow.state_machine)

    def build_definition(self, step):
        """
           Build the state machine definition for the Batch Segment Job flow.

           Args:
               step (BatchSegmentJobStep): The step object representing the Batch Segment Job.

           Returns:
               The state machine definition for the Batch Segment Job flow.
       """
        create_step = step.create(step.id, step.result_path_create)
        exit_step = step.exit_step()

        describe_step = step.describe()

        is_solution_version_present_condition = sfn.Condition.is_present("$.SolutionVersionArn[0]")
        solution_version_arn_exists_condition = sfn.Condition.is_present("$.ServiceConfig.solutionVersionArn")
        new_batch_job_creation_condition = sfn.Condition.boolean_equals("$.CreateBatchSegmentJob", True)

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


class BatchSegmentJobStep(BaseStep):
    """
    A class that represents a step in the Batch Segment Job Step.

    This class inherits from the BaseStep class and is responsible for defining the configuration
    and behavior of the Batch Segment Job step.
    """
    STEP_NAME = "BatchSegmentTask"
    SERVICE = "personalize"
    object_type = "BatchSegmentJob"

    def __init__(self, scope):
        """
        Initialize a new instance of the BatchSegmentJobStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
        """
        super().__init__(scope)
        self.scope = scope
        self.id = BatchSegmentJobStep.STEP_NAME
        self.create_step_arn_path = "batchSegmentJobArn"

        self.result_path = f"$.{BatchSegmentJobStep.STEP_NAME}"
        self.result_path_describe = "$.DescribeBatchSegmentJob"
        self.result_path_create = "$.CreateBatchSegmentJob"
        self.result_path_error = "$.DescribeBatchSegmentJob.Error"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.BatchSegmentJob.BatchSegmentJobArn",
            "statusPath": f"{self.result_path_describe}.BatchSegmentJob.Status",
            "message": "Personalize BatchSegmentJob status change",
            "detail": {
                "SolutionVersionArn.$": f"{self.result_path_describe}.BatchSegmentJob.SolutionVersionArn",
                "JobName.$": f"{self.result_path_describe}.BatchSegmentJob.JobName",
                "JobInput.$": f"{self.result_path_describe}.BatchSegmentJob.JobInput.S3DataSource.Path",
                "JobOutput.$": f"{self.result_path_describe}.BatchSegmentJob.JobOutput.S3DataDestination.Path",

            },
            "detailType": "Personalize BatchSegmentJob status change",
        }

    def condition_success(self):
        """
       Define the condition for a successful Batch Segment Job.

       Returns:
           The condition for a successful Batch Segment Job.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + BatchSegmentJobStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
           Define the condition for a failed Batch Segment Job.

           Returns:
               The condition for a failed Batch Segment Job.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + BatchSegmentJobStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
        Create a step to describe the Batch Segment Job.

        Returns:
            The step to describe the Batch Segment Job.
        """
        payload = {
            "action": "describeBatchSegmentJob",
            "service": BatchSegmentJobStep.SERVICE,
            "id": "DescribeBatchSegmentJob",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "BatchSegmentJobArn.$": "$.CreateBatchSegmentJob.Payload.response.batchSegmentJobArn"
            },

            "object_type": BatchSegmentJobStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
        Create a step which executes a child state machine for a batch segment job.

        Args:
            state_machine (sfn.StateMachine): The state machine to which the task belongs.

        Returns:
            The task step for the Batch Segment Job step.

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
                                                     "ServiceConfig": sfn.JsonPath.string_at(
                                                         "$.BatchSegmentJob.serviceConfig"),
                                                     "SolutionVersionArn": sfn.JsonPath.string_at(
                                                         "$.SolutionVersionArn"),
                                                     "CreateBatchSegmentJob": sfn.JsonPath.string_at(
                                                         "$.BatchSegmentJob.createBatchSegmentJob"),

                                                 }),
                                                 result_selector={
                                                     "BatchInferenceJobArn.$": "$..Output[?(@.DescribeBatchSegmentJob"
                                                                               ")].DescribeBatchSegmentJob"
                                                                               ".BatchSegmentJob.BatchSegmentJobArn",
                                                 }
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
