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
a Solution in Amazon Personalize.

This module defines two main classes:

1. SolutionFlow: Represents the overall flow for creating and monitoring the status of a
   Solution in Amazon Personalize. It inherits from the BaseFlow class and defines the state
   machine for the Solution flow.

2. SolutionStep: Represents a step in the Solution flow for Amazon Personalize. It inherits
   from the BaseStep class and defines the specific steps for creating, describing, and
   monitoring the status of a Solution.


"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks, Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class SolutionFlow(BaseFlow):
    """
       A class that represents the flow for creating and managing a Solution in Amazon Personalize.

       This class inherits from the BaseFlow class and defines the state machine for creating and
       monitoring the status of a Solution in Amazon Personalize.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the SolutionFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.solution_step = SolutionStep(self.scope, construct_id)

        if SolutionFlow.state_machine is None:
            SolutionFlow.state_machine = self.build_flow(self.solution_step)

    def build(self):
        """
           Builds and returns the task for executing the state machine for the Solution flow.

           Returns:
               Task: The task for executing the state machine.
        """
        return self.solution_step.task(SolutionFlow.state_machine)

    def build_definition(self, step):
        """
        Builds the definition of the state machine for the Solution flow.

        Args:
            step (SolutionStep): The SolutionStep for the Solution flow.

        Returns:
            Task: The task that describes the Solution.
        """
        create_step = step.create(step.id, step.result_path_create)

        describe_step = step.describe()
        describe_step.add_catch(create_step, errors=["Personalize.ResourceNotFoundException"],
                                result_path=step.result_path_error
                                )

        create_step.next(
            step.wait("Wait After Create").next(describe_step))

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.condition_created_in_current_execution(step)).otherwise(
                step.wait("Wait After Describe").
                next(describe_step)))

        return describe_step


class SolutionStep(BaseStep):
    """
   A class that represents a step in the Solution flow for Amazon Personalize.

   This class inherits from the BaseStep class and defines the specific steps for creating,
   describing, and monitoring the status of a Solution in Amazon Personalize.
   """
    SERVICE = "personalize"
    object_type = "Solution"
    STEP_NAME = "SolutionTask"

    def __init__(self, scope, suffix):
        """
        Initializes a new instance of the SolutionStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
            suffix (str): A suffix to be added to the step name.
        """
        super().__init__(scope)
        self.scope = scope
        self.id = f"{SolutionStep.STEP_NAME}{suffix}"
        self.create_step_arn_path = "solutionArn"

        self.result_path_describe = "$.DescribeSolution"
        self.result_path_create = "$.CreateSolution"
        self.result_path_error = "$.DescribeSolution.Error"
        self.result_path = f"$.{SolutionStep.STEP_NAME}"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.Solution.SolutionArn",
            "statusPath": f"{self.result_path_describe}.Solution.Status",
            "message": "Personalize Solution status change",
            "detail": {
                "DatasetGroupArn.$": f"{self.result_path_describe}.Solution.DatasetGroupArn",
                "RecipeArn.$": f"{self.result_path_describe}.Solution.RecipeArn",
                "Name.$": f"{self.result_path_describe}.Solution.Name"

            },
            "detailType": "Personalize Solution status change",
        }

    def condition_success(self):
        """
       Returns a condition that checks if the Solution is in the ACTIVE state.

       Returns:
           Condition: The condition that checks if the Solution is in the ACTIVE state.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + SolutionStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
        Returns a condition that checks if the Solution creation failed.

        Returns:
            Condition: The condition that checks if the Solution creation failed.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + SolutionStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
        Creates a step that describes the Solution in Amazon Personalize.

        Returns:
            Task: The task that describes the Solution.
        """
        payload = {
            "action": "describeSolution",
            "service": SolutionStep.SERVICE,
            "id": "DescribeSolution",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "SolutionArn.$": "States.Format('arn:aws:personalize:{}:{}:solution/{}',$.Region,$.AccountID,$.ServiceConfig.name)",
            }
            ,

            "object_type": SolutionStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize Solutions.

        Args:
            state_machine (StateMachine): The state machine for the Solution flow.

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
                                                     "DatasetGroup": sfn.JsonPath.string_at("$.DatasetGroup"),
                                                     "Type": self.object_type,
                                                     "ServiceConfig": sfn.JsonPath.string_at(
                                                         "$.Solution.serviceConfig"),
                                                     "DatasetCreateImportJobArn": sfn.JsonPath.string_at(
                                                         "$.DatasetCreateImportJobArn"),
                                                 })

                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
