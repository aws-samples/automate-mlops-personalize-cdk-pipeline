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
This module defines the FilterFlow and FilterStep classes, which are responsible
for managing the lifecycle of Amazon Personalize filters using AWS Step Functions.

The FilterFlow class orchestrates the creation and management of filters,
while the FilterStep class encapsulates the logic for individual steps in the process.

"""
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class FilterFlow(BaseFlow):
    """
    Manages the lifecycle of an Amazon Personalize filter using AWS Step Functions.

    This class orchestrates the creation and management of filters by building
    a Step Functions state machine with the necessary steps.

    Attributes:
        state_machine (sfn.StateMachine): The Step Functions state machine for managing filters.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the FilterFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.filter_step = FilterStep(self.scope)

        if FilterFlow.state_machine is None:
            FilterFlow.state_machine = self.build_flow(self.filter_step)

    def build(self):
        """
        Builds the Step Functions task for managing filters.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing filters.
        """
        return self.filter_step.task(FilterFlow.state_machine)

    def build_definition(self, step):
        """
        Builds the Step Functions state machine definition for managing filters.

        Args:
            step (FilterStep): The FilterStep instance containing the step logic.

        Returns:
            sfn.State: The initial state of the Step Functions state machine.
        """
        create_step = step.create(step.id, step.result_path_create)

        pass_state_filter_arn = sfn.Pass(self.scope, step.object_type + " extract Filter Arn")

        describe_step = step.describe()
        create_step.add_catch(pass_state_filter_arn, errors=["ResourceAlreadyExistsException"],
                              result_path=step.result_path_error
                              )
        pass_state_filter_arn.next(describe_step)

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

        return create_step


class FilterStep(BaseStep):
    """
    Encapsulates the logic for individual steps in the filter management process.

    This class defines the steps required to create, describe, and manage filters
    in Amazon Personalize.

    """
    SERVICE = "personalize"
    object_type = "Filter"
    STEP_NAME = "FilterTask"

    def __init__(self, scope):
        """
       Initializes a new instance of the FilterStep class.

       Args:
           scope (Construct): The scope in which this construct is created.
       """
        super().__init__(scope)
        self.scope = scope
        self.id = FilterStep.STEP_NAME
        self.create_step_arn_path = "CreateFilter"

        self.result_path_describe = "$.DescribeFilter"
        self.result_path_create = "$.CreateFilter"
        self.result_path_error = "$.DescribeFilter.error"
        self.result_path = f"$.{FilterStep.STEP_NAME}"

        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.Filter.FilterArn",
            "statusPath": f"{self.result_path_describe}.Filter.Status",
            "message": "Personalize Filter status change",
            "detail": {
                "DatasetGroupArn.$": f"{self.result_path_describe}.Filter.DatasetGroupArn",
                "Name.$": f"{self.result_path_describe}.Filter.Name"

            },
            "detailType": "Personalize Filter status change",
        }

    def condition_success(self):
        """
        Defines the condition for a successful filter creation or update.

        Returns:
            sfn.Condition: The condition that checks if the filter status is 'ACTIVE'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + FilterStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
        Defines the condition for a failed filter creation or update.

        Returns:
            sfn.Condition: The condition that checks if the filter status is 'CREATE FAILED'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + FilterStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
        Defines the step for describing an existing filter.

        Returns:
            sfn.Task: The Step Functions task for describing a filter.
        """
        payload = {
            "action": "describeFilter",
            "service": FilterStep.SERVICE,
            "id": "DescribeFilter",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "FilterArn.$": "States.Format('arn:aws:personalize:{}:{}:filter/{}',$.Region,$.AccountID,$.Item.serviceConfig.name)",
            }
            ,

            "object_type": FilterStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize Filters.

        Args:
            state_machine (sfn.StateMachine): The Step Functions state machine for managing filters.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing filters.
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
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.DatasetGroup"),
                                                     "Item": sfn.JsonPath.object_at("$.Item")

                                                 }),
                                                 result_selector={
                                                     "ExecutionArn.$": "$.ExecutionArn",
                                                     "StateMachineStatus.$": "$.Status"

                                                 },
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
