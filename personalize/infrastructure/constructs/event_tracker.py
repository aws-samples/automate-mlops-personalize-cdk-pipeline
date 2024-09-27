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

This module defines the EventTrackerFlow and EvenTrackerStep classes, which are responsible
for managing the lifecycle of Amazon Personalize event trackers using AWS Step Functions.

The EventTrackerFlow class orchestrates the creation and management of event trackers,
while the EvenTrackerStep class encapsulates the logic for individual steps in the process.


"""
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class EventTrackerFlow(BaseFlow):
    """
    Manages the lifecycle of an Amazon Personalize event tracker using AWS Step Functions.

    This class orchestrates the creation and management of event trackers by building
    a Step Functions state machine with the necessary steps.

    Attributes:
        state_machine (sfn.StateMachine): The Step Functions state machine for managing event trackers.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initializes a new instance of the EventTrackerFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.event_tracker_step = EvenTrackerStep(self.scope)

        if EventTrackerFlow.state_machine is None:
            EventTrackerFlow.state_machine = self.build_flow(self.event_tracker_step)

    def build(self):
        """
        Builds the Step Functions task for managing event trackers.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing event trackers.
        """
        return self.event_tracker_step.task(EventTrackerFlow.state_machine)

    def build_definition(self, step):
        """
        Builds the Step Functions state machine definition for managing event trackers.

        Args:
            step (EvenTrackerStep): The EvenTrackerStep instance for managing event trackers.

        Returns:
            sfn.State: The initial state of the Step Functions state machine for managing event trackers.
        """
        list_step = step.list()

        should_execute_step = sfn.Choice(self.scope, step.object_type + " should execute workflow?").when(
            sfn.Condition.is_present(f"$.{step.element_exists_check}"),
            list_step).otherwise(
            step.exit_step())

        pass_state_arn_array_length = sfn.Pass(self.scope, step.object_type + " get ARN array length", parameters={
            "eventTrackerArnArrayLength": sfn.JsonPath.array_length(
                sfn.JsonPath.string_at('$.ListEventTracker.EventTrackers'))},
                                               result_path="$.eventTrackerArnArrayLengthObject")

        pass_state_tracker_arn = sfn.Pass(self.scope, step.object_type + " extract ARN", parameters={
            "EventTrackerArn": sfn.JsonPath.string_at("$.ListEventTracker.EventTrackers[0].EventTrackerArn")},
                                          result_path="$.EventTracker")

        describe_step = step.describe()

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.condition_created_in_current_execution(step)).otherwise(
                step.wait("Wait After Describe").next(describe_step)))

        create_step = step.create(step.id, step.result_path_create)

        choice = sfn.Choice(self.scope, step.object_type + " Value matches?").when(
            sfn.Condition.number_greater_than("$.eventTrackerArnArrayLengthObject.eventTrackerArnArrayLength", 0),
            pass_state_tracker_arn.next(describe_step)).otherwise(
            create_step)

        pass_state_extract_api_arn = sfn.Pass(self.scope, step.object_type + " extract ARN post API call", parameters={
            "EventTrackerArn": sfn.JsonPath.string_at("$.EventTracker.Payload.response.eventTrackerArn")},
                                              result_path="$.EventTracker")

        create_step.next(pass_state_extract_api_arn)

        pass_state_extract_api_arn.next(describe_step)

        list_step.next(pass_state_arn_array_length.next(choice))

        return should_execute_step


class EvenTrackerStep(BaseStep):
    """
    Encapsulates the logic for individual steps in the event tracker management process.

    This class defines the steps required to create, describe, and manage event trackers
    in Amazon Personalize.

    """
    SERVICE = "personalize"
    object_type = "EventTracker"
    STEP_NAME = "EventTrackerTask"

    def __init__(self, scope):
        """
        Initializes a new instance of the EvenTrackerStep class.

        Args:
            scope (Construct): The scope in which this construct is created.
        """
        super().__init__(scope)
        self.scope = scope
        self.id = EvenTrackerStep.STEP_NAME
        self.create_step_arn_path = "EventTracker"

        self.element_exists_check = "ServiceConfig[0]"
        self.result_path_describe = "$.DescribeEventTracker"
        self.result_path_list = "$.ListEventTracker"
        self.result_path_create = "$.EventTracker"
        self.result_path_error = "$.DescribeEventTracker.Error"
        self.result_path = f"$.{EvenTrackerStep.STEP_NAME}"
        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.EventTracker.EventTrackerArn",
            "statusPath": f"{self.result_path_describe}.EventTracker.Status",
            "message": "Personalize EventTracker status change",
            "detail": {
                "TrackingId.$": f"{self.result_path_describe}.EventTracker.TrackingId",
                "DatasetGroupArn.$": f"{self.result_path_describe}.EventTracker.DatasetGroupArn",
                "Name.$": f"{self.result_path_describe}.EventTracker.Name"

            },
            "detailType": "Personalize EventTracker status change",
        }

    def condition_success(self):
        """
       Defines the condition for a successful event tracker creation or update.

       Returns:
           sfn.Condition: The condition that checks if the event tracker status is 'ACTIVE'.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + EvenTrackerStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
        Defines the condition for a failed event tracker creation or update.

        Returns:
            sfn.Condition: The condition that checks if the event tracker status is 'CREATE FAILED'.
        """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + EvenTrackerStep.object_type + ".Status",
            "CREATE FAILED")

    def describe(self):
        """
        Defines the step for describing an existing event tracker.

        Returns:
            sfn.Task: The Step Functions task for describing an event tracker.
        """
        payload = {
            "action": "describeEventTracker",
            "service": EvenTrackerStep.SERVICE,
            "id": "DescribeEventTracker",
            "iam_resources": ["*"],
            "result_path": self.result_path_describe,
            "parameters": {
                "EventTrackerArn.$": "$.EventTracker.EventTrackerArn",
            }
            ,

            "object_type": EvenTrackerStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def list(self):
        """
        Defines the step for listing existing event trackers.

        Returns:
            sfn.Task: The Step Functions task for listing event trackers.
        """
        payload = {
            "action": "listEventTrackers",
            "service": EvenTrackerStep.SERVICE,
            "id": "ListEventTrackers",
            "iam_resources": ["*"],
            "result_path": self.result_path_list,
            "parameters": {
                "DatasetGroupArn.$": "States.Format('arn:aws:personalize:{}:{}:dataset-group/{}',$.Region,"
                                     "$.AccountID,$.DatasetGroup.serviceConfig.name)"
            },
            "object_type": EvenTrackerStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
            Create a step which executes a child state machine for creating a Personalize EventTrackers.

        Args:
            state_machine (sfn.StateMachine): The Step Functions state machine for managing event trackers.

        Returns:
            sfn.StepFunctionsStartExecution: The Step Functions task for managing event trackers.
        """
        if state_machine is None:
            raise Exception("State Machine None, it is not initialized")
        task = tasks.StepFunctionsStartExecution(self.scope, self.id,
                                                 state_machine=state_machine,
                                                 result_path=self.result_path,
                                                 integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                                 input=sfn.TaskInput.from_object({
                                                     "Type": self.object_type,
                                                     "Region": self.region,
                                                     "AccountID": self.account_id,
                                                     "DatasetGroup": sfn.JsonPath.object_at("$.datasetGroup"),
                                                     "ServiceConfig": sfn.JsonPath.string_at(
                                                         "$..[?(@.eventTracker)].eventTracker.serviceConfig")

                                                 }),
                                                 result_selector={
                                                     "ExecutionArn.$": "$.ExecutionArn",
                                                     "StateMachineStatus.$": "$.Status"

                                                 },
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
