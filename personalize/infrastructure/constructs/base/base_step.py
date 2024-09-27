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

"""This module defines a base class for creating steps in an AWS Step Functions state machine for an Amazon
Personalize MLOps pipeline.

The BaseStep class provides a foundation for defining and configuring various steps in a Step Functions state
machine. It includes methods for handling success and failure conditions, exiting steps, sending events to
EventBridge, creating Wait states, and creating Fail states.

The module is designed to be used in conjunction with other classes and modules that define specific steps for
different Personalize resources, such as dataset groups, datasets, filters, and recommenders.

Key components:
- BaseStep class:
    - Initializes the base step with a scope and other necessary attributes.
    - Defines methods for handling success and failure conditions.
    - Provides methods for exiting steps, sending events to EventBridge, creating Wait states, and creating Fail states.
    - Serves as a base class for other step-specific classes to inherit from.

"""

import uuid
from abc import abstractmethod

from aws_cdk import (

    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    aws_iam as iam

)


class BaseStep:
    """
    Base class for defining steps in a Step Functions state machine.
    """
    put_event_config = None
    object_type = None

    def __init__(self, scope):
        """
        Initialize the BaseStep instance.

        Args:
            scope (Construct): The scope in which the resources are defined.
        """
        self.scope = scope
        self.dataset_group_name_path = sfn.JsonPath.string_at("$.datasetGroup.name")
        self.region = scope.region
        self.account_id = scope.account
        self.dataset_group = sfn.JsonPath.string_at("$.datasetGroup")

    @abstractmethod
    def condition_success(self):
        """
       Define the success condition for the step.
       """

    def exit_step(self, message="Step data not present, skipping step"):
        """
        Exit the step with a success state and a message.

        Args: message (str, optional): The message to display when exiting the step. Defaults to "Step data not
        present, skipping step".

        Returns:
            sfn.Succeed: A Succeed state representing the successful exit of the step.
        """
        return sfn.Succeed(self.scope, f"{self.object_type} {message}")

    @abstractmethod
    def condition_failure(self):
        """
        Define the failure condition for the step.
        """

    def send_event(self):
        """
        Send an event to EventBridge.

        Returns:
            tasks.CallAwsService: A CallAwsService task representing the event being sent to EventBridge.
        """
        default_detail_dict = {
            "Arn.$": self.put_event_config["arnPath"],
            "Message": self.put_event_config["message"],
        }

        if "statusPath" in self.put_event_config:
            default_detail_dict["Status.$"] = self.put_event_config["statusPath"]

        additional_detail_dict = self.put_event_config["detail"]

        payload = {
            "action": "putEvents",
            "service": "eventbridge",
            "id": f"PutEvent-{uuid.uuid4()}",
            "iam_resources": ["*"],
            "result_path": "$.PutEvent",
            "parameters": {
                "Entries": [
                    {
                        "Detail": {
                            **default_detail_dict, **additional_detail_dict

                        },
                        "DetailType": self.put_event_config["detailType"],
                        "EventBusName": self.scope.event_bus_name,
                        "Source": "solutions.aws.personalize"
                    }
                ]
            },
            "object_type": self.object_type
        }

        return self.create_call_aws_service_step(payload)

    def wait(self, step_id):
        """
        Create a Wait state in the state machine.

        Args:
            step_id (str): The ID of the step.

        Returns:
            sfn.Wait: A Wait state representing a delay in the state machine execution.
        """
        return sfn.Wait(self.scope,
                        step_id + (
                            ("-" + self.dataset_type) if hasattr(self,
                                                                 "dataset_type") else "") + "-" + self.object_type,
                        time=sfn.WaitTime.duration(Duration.seconds(120))
                        )

    def fail(self, step_id, error):
        """
        Create a Fail state in the state machine.

        Args:
            step_id (str): The ID of the step.
            error (str): The error message to display in the Fail state.

        Returns:
            sfn.Fail: A Fail state representing a failure in the state machine execution.
        """
        return sfn.Fail(self.scope, step_id + (
            ("-" + self.dataset_type) if hasattr(self,
                                                 "dataset_type") else "") + "-" + self.object_type,
                        cause=error,
                        error=error
                        )

    def create(self, step_name, result_path):
        """
        Create a LambdaInvoke task in the state machine.

        Args:
            step_name (str): The name of the step.
            result_path (str): The path to store the result of the Lambda invocation.

        Returns:
            tasks.LambdaInvoke: A LambdaInvoke task representing the invocation of a Lambda function.
        """
        api = tasks.LambdaInvoke(
            self.scope, f"{step_name}Api",
            lambda_function=self.scope.api_lambda,
            result_path=result_path,
            result_selector={
                "Payload": {
                    "response.$": "$.Payload.response"}
            }
        )

        return api

    def create_call_aws_service_step(self, params):
        """
        Create a CallAwsService task in the state machine.

        Args:
            params (dict): A dictionary containing the parameters for the CallAwsService task.

        Returns:
            tasks.CallAwsService: A CallAwsService task representing a call to an AWS service.
        """
        return tasks.CallAwsService(self.scope, params["id"],
                                    service=params["service"],
                                    action=params["action"],
                                    iam_resources=params["iam_resources"],
                                    parameters=params["parameters"],
                                    result_path=params["result_path"],
                                    additional_iam_statements=[iam.PolicyStatement(
                                        actions=["events:PutEvents"],
                                        resources=[self.scope.event_bus_arn])]
                                    )

    def condition_created_in_current_execution(self, step):
        """
        Create a condition to check if a resource was created in the current execution of the state machine.

        Args:
            step (Step): The step for which the condition is being created.

        Returns:
            sfn.Choice: A Choice state representing the condition.
        """
        end_state = sfn.Pass(self.scope, step.object_type + " End")

        extract_arn = sfn.Pass(self.scope, step.object_type + " prepare input",
                               parameters={
                                   "Arn": sfn.JsonPath.object_at(
                                       f"$..[?(@.Payload)].Payload.response.{step.create_step_arn_path}")
                               },
                               result_path=f"$.{step.object_type}"
                               )

        created_condition = sfn.Choice(self.scope, step.object_type + " was created in this execution?").when(
            sfn.Condition.is_present(f"$.{step.object_type}.Arn[0]"),
            step.send_event().next(end_state)).otherwise(
            end_state)

        return extract_arn.next(created_condition)

    @abstractmethod
    def task(self, state_machine):
        """
        Define the task to be executed in the state machine.

        Args:
            state_machine (sfn.StateMachine): The state machine in which the task will be executed.
        """
