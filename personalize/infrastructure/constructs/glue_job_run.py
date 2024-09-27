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
This module defines the PreprocessingGlueJobFlow and GlueJobStep classes, which are responsible
for managing the execution of AWS Glue jobs for data preprocessing using AWS Step Functions.

The PreprocessingGlueJobFlow class orchestrates the execution of Glue jobs,
while the GlueJobStep class encapsulates the logic for individual steps in the process.

"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class PreprocessingGlueJobFlow(BaseFlow):
    """
    Manages the execution of AWS Glue jobs for data preprocessing using AWS Step Functions.

    This class orchestrates the execution of Glue jobs by building a Step Functions state machine
    with the necessary steps. It uses the GlueJobStep class to define the individual steps
    of the workflow.

    Attributes:
        state_machine (sfn.StateMachine): The Step Functions state machine for executing Glue jobs.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the PreprocessingGlueJobFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.glue_job_step = GlueJobStep(self.scope)

        if PreprocessingGlueJobFlow.state_machine is None:
            PreprocessingGlueJobFlow.state_machine = self.build_flow(self.glue_job_step)

    def build(self):
        """
        Builds the Step Functions task for executing Glue jobs.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for executing Glue jobs.
        """
        return self.glue_job_step.task(PreprocessingGlueJobFlow.state_machine)

    def build_definition(self, step):
        """
        Builds the Step Functions state machine definition for executing Glue jobs.

        Args:
            step (GlueJobStep): The GlueJobStep instance containing the step logic.

        Returns:
            sfn.State: The initial state of the Step Functions state machine.
        """
        run_step = step.run()
        get_step = step.get()

        run_step.next(
            step.wait("Wait After Run").next(get_step))

        new_campaign_run_condition = sfn.Condition.boolean_equals("$.Run[0]", True)
        element_exists_condition = sfn.Condition.is_present(f"$.{step.element_exists_check}")

        should_execute_step = sfn.Choice(self.scope, step.object_type + "-Execute?").when(
            sfn.Condition.and_(element_exists_condition, new_campaign_run_condition),
            run_step).otherwise(
            step.exit_step())

        get_step.next(
            sfn.Choice(self.scope, step.object_type + " Job Still Running?").when(
                step.condition_failure(),
                step.send_event().next(
                    step.fail("Glue Job failed", "Glue Job failed"))).when(
                step.condition_success(),
                step.send_event().next(
                    sfn.Pass(self.scope, step.object_type + " End"))).otherwise(
                step.wait("Wait After Get").
                next(get_step)))

        return should_execute_step


class GlueJobStep(BaseStep):
    """
    Encapsulates the logic for individual steps in the Glue job execution process.

    This class defines the steps required to start, monitor, and manage the execution of
    AWS Glue jobs for data preprocessing.

    """
    SERVICE = "glue"
    STEP = "PreProcessingTask"
    object_type = "GlueJob"

    def __init__(self, scope):
        """
        Initializes a new instance of the GlueJobStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
        """
        super().__init__(scope)
        self.scope = scope
        self.id = GlueJobStep.STEP

        self.result_path = f"$.{GlueJobStep.STEP}"
        self.result_path_get = "$.GetGlueJob"
        self.result_path_run = "$.GlueJobRun"
        self.result_path_error = "$.DescribeGlueJob.Error"

        self.job_name = sfn.JsonPath.string_at("$..[?(@.preprocessing)].preprocessing.jobName")
        self.element_exists_check = "JobName[0]"

        self.put_event_config = {
            "arnPath": f"{self.result_path_get}.JobRun.Id",
            "statusPath": f"{self.result_path_get}.JobRun.JobRunState",
            "message": "Personalize JobRun status change",
            "detail": {
                "JobName.$": f"{self.result_path_get}.JobRun.JobName",
                "LogGroupName.$": f"{self.result_path_get}.JobRun.LogGroupName"

            },
            "detailType": "Personalize JobRun status change",
        }

    def condition_success(self):
        """
       Defines the condition for a successful Glue job execution.

       Returns:
           sfn.Condition: The condition that checks if the job run state is 'SUCCEEDED'.
       """
        return sfn.Condition.string_equals(
            self.result_path_get + ".JobRun" + ".JobRunState", "SUCCEEDED")

    def condition_failure(self):
        """
       Defines the condition for a failed Glue job execution.

       Returns:
           sfn.Condition: The condition that checks if the job run state is 'FAILED'.
       """
        return sfn.Condition.string_equals(
            self.result_path_get + ".JobRun" + ".JobRunState",
            "FAILED")

    def run(self):
        """
        Defines the step for starting a Glue job run.

        Returns:
            sfn.Task: The Step Functions task for starting a Glue job run.
        """
        payload = {
            "action": "startJobRun",
            "service": GlueJobStep.SERVICE,
            "id": "StartJobRun",
            "iam_resources": ["*"],
            "result_path": self.result_path_run,
            "parameters": {
                "JobName.$": "$.JobName[0]"

            },
            "object_type": GlueJobStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def get(self):
        """
       Defines the step for getting the status of a Glue job run.

       Returns:
           sfn.Task: The Step Functions task for getting the status of a Glue job run.
       """
        payload = {
            "action": "getJobRun",
            "service": GlueJobStep.SERVICE,
            "id": "GetJobRun",
            "iam_resources": ["*"],
            "result_path": self.result_path_get,
            "parameters": {
                "JobName.$": "$.JobName[0]",
                "RunId.$": self.result_path_run + ".JobRunId"
            },

            "object_type": GlueJobStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Glue Job Run flow.

        Args:
            state_machine (sfn.StateMachine): The Step Functions state machine for managing Glue Job Run flow.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing Glue Job Run flow.
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
                                                     "JobName": sfn.JsonPath.string_at(
                                                         "$..[?(@.preprocessing)]."
                                                         "preprocessing."
                                                         "jobName"),
                                                     "Run": sfn.JsonPath.string_at(
                                                         "$..[?(@.preprocessing)].preprocessing.run"),
                                                 })
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Glue.Client.exceptions.InternalServiceException",
                                                  "Glue.Client.exceptions.OperationTimeoutException",
                                                  "Glue.Client.exceptions.ResourceNumberLimitExceededException",
                                                  "Glue.Client.exceptions.ConcurrentRunsExceededException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
