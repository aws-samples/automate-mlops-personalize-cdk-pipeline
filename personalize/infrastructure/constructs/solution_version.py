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
a Solution Versions in Amazon Personalize.

This module defines two main classes:

1. SolutionVersionFlow: Represents the overall flow for creating and monitoring the status of a
   Solution Version in Amazon Personalize. It inherits from the BaseFlow class and defines the
   state machine for the Solution Version flow.

2. SolutionVersionStep: Represents a step in the Solution Version flow for Amazon Personalize.
   It inherits from the BaseStep class and defines the specific steps for creating, describing,
   and monitoring the status of a Solution Version.


"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class SolutionVersionFlow(BaseFlow):
    """
    A class that represents the flow for creating and managing a Solution Version in Amazon Personalize.

    This class inherits from the BaseFlow class and defines the state machine for creating and
    monitoring the status of a Solution Version in Amazon Personalize.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the SolutionVersionFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """

        super().__init__(scope, construct_id)
        self.scope = scope
        self.solution_version_step = SolutionVersionStep(self.scope, construct_id)

        if SolutionVersionFlow.state_machine is None:
            SolutionVersionFlow.state_machine = self.build_flow(self.solution_version_step)

    def build(self):
        """
        Builds and returns the task for executing the state machine for the Solution Version flow.

        Returns:
            Task: The task for executing the state machine.
        """
        return self.solution_version_step.task(SolutionVersionFlow.state_machine)

    def build_definition(self, step):
        """
           Builds the definition of the state machine for the Solution Version flow.

           Args:
               step (BaseStep): The base step for the Solution Version flow.

           Returns:
               Choice: The choice state that determines whether to create a new Solution Version.
        """
        create_step = step.create(step.id, step.result_path_create)
        describe_step = step.describe()

        exit_step = step.exit_step("Training mode not specified, skipping generating new version")

        retrain_solution_condition = sfn.Condition.boolean_equals("$.CreateNewSolutionVersion[0]", True)

        # Users Dataset
        dataset_import_job_run_condition_1 = sfn.Condition.is_present("$.DatasetCreateImportJobArn[0][0]")
        # Items Dataset
        dataset_import_job_run_condition_2 = sfn.Condition.is_present("$.DatasetCreateImportJobArn[1][0]")
        # Interactions Dataset
        dataset_import_job_run_condition_3 = sfn.Condition.is_present("$.DatasetCreateImportJobArn[2][0]")

        should_retrain = sfn.Choice(self.scope, step.object_type + " should retrain?").when(
            sfn.Condition.or_(retrain_solution_condition, dataset_import_job_run_condition_1,
                              dataset_import_job_run_condition_2, dataset_import_job_run_condition_3),
            create_step).otherwise(
            exit_step)

        create_step.next(
            step.wait("Wait After Create").next(describe_step))

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Creation failed", "Creation failed"))).when(
                step.condition_success(),
                step.send_event().next(sfn.Pass(self.scope, step.object_type + " End"))).otherwise(
                step.wait("Wait After Describe").
                next(describe_step)))

        return should_retrain


class SolutionVersionStep(BaseStep):
    """
       A class that represents a step in the Solution Version flow for Amazon Personalize.

       This class inherits from the BaseStep class and defines the specific steps for creating,
       describing, and monitoring the status of a Solution Version in Amazon Personalize.
    """
    service = "personalize"
    object_type = "SolutionVersion"
    STEP_NAME = "SolutionVersionTask"

    def __init__(self, scope, suffix):
        """
       Initializes a new instance of the SolutionVersionStep class.

       Args:
           scope (Construct): The scope in which this construct is created.
           suffix (str): A suffix to be added to the step name.
       """
        super().__init__(scope)
        self.scope = scope
        self.id = f"{SolutionVersionStep.STEP_NAME}{suffix}"
        self.create_step_arn_path = "solutionVersionArn"

        self.result_path = f"$.{SolutionVersionStep.STEP_NAME}"
        self.result_path_describe = "$.DescribeSolutionVersion"
        self.result_path_create = "$.CreateSolutionVersion"
        self.result_path_error = "$.DescribeSolutionVersion.Error"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.SolutionVersion.SolutionVersionArn",
            "statusPath": f"{self.result_path_describe}.SolutionVersion.Status",
            "message": "Personalize Solution status change",
            "detail": {
                "DatasetGroupArn.$": f"{self.result_path_describe}.SolutionVersion.DatasetGroupArn",
                "RecipeArn.$": f"{self.result_path_describe}.SolutionVersion.RecipeArn",
                "Name.$": f"{self.result_path_describe}.SolutionVersion.Name",
                "SolutionArn.$": f"{self.result_path_describe}.SolutionVersion.SolutionArn",
                "TrainingHours.$": f"{self.result_path_describe}.SolutionVersion.TrainingHours",
                "TrainingMode.$": f"{self.result_path_describe}.SolutionVersion.TrainingMode"

            },
            "detailType": "Personalize SolutionVersion status change",
        }

    def condition_success(self):
        """
       Returns a condition that checks if the Solution Version is in the ACTIVE state.

       Returns:
           Condition: The condition that checks if the Solution Version is in the ACTIVE state.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + SolutionVersionStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
        Returns a condition that checks if the Solution Version creation failed.

        Returns:
            Condition: The condition that checks if the Solution Version creation failed.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + SolutionVersionStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
        Creates a step that describes the Solution Version in Amazon Personalize.

        Returns:
            Task: The task that describes the Solution Version.
        """
        payload = {
            "action": "describeSolutionVersion",
            "service": SolutionVersionStep.service,
            "id": "DescribeSolutionVersion",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "SolutionVersionArn.$": "$.CreateSolutionVersion.Payload.response.solutionVersionArn"
            },

            "object_type": SolutionVersionStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize Solutions Versions.

        Args:
            state_machine (StateMachine): The state machine for the SolutionVersion flow.

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
                                                     "Region": sfn.JsonPath.string_at("$.Region"),
                                                     "AccountID": sfn.JsonPath.string_at("$.AccountID"),
                                                     "Type": self.object_type,
                                                     "SolutionServiceConfig": sfn.JsonPath.string_at(
                                                         "$.Solution.serviceConfig"),
                                                     "SolutionVersionServiceConfig": sfn.JsonPath.string_at(
                                                         "$.Solution[?(@.solutionVersion)].solutionVersion"
                                                         ".serviceConfig"),
                                                     "CreateNewSolutionVersion": sfn.JsonPath.string_at(
                                                         "$.Solution[?(@.solutionVersion)].solutionVersion"
                                                         ".createNewSolutionVersion"),
                                                     "DatasetCreateImportJobArn": sfn.JsonPath.string_at(
                                                         "$.DatasetCreateImportJobArn"),
                                                 }),

                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
