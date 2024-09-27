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
This module defines classes for creating and managing Amazon Personalize Campaigns
using AWS Step Functions and AWS CDK.
"""

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration

)

from personalize.infrastructure.constructs.base.base_flow import BaseFlow
from personalize.infrastructure.constructs.base.base_step import BaseStep


class CampaignFlow(BaseFlow):
    """
       A class that represents the flow for creating and managing Amazon Personalize Campaigns.

       This class inherits from the BaseFlow class and is responsible for building the state machine
       definition for the Campaign flow.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initialize a new instance of the CampaignFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.scope = scope
        self.campaign_step = CampaignStep(self.scope)

        if CampaignFlow.state_machine is None:
            CampaignFlow.state_machine = self.build_flow(self.campaign_step)

    def build(self):
        """
        Build the task for the Campaign flow.

        Returns:
            The task for the Campaign flow.
        """
        return self.campaign_step.task(CampaignFlow.state_machine)

    def build_definition(self, step):
        """
        Build the state machine definition for the Campaign flow.

        Args:
            step (CampaignStep): The step object representing the Campaign.

        Returns:
            The state machine definition for the Campaign flow.
        """
        create_step = step.create(step.id, step.result_path_create)

        exit_step = step.exit_step()

        describe_step = step.describe("DescribeCampaignInitial")
        describe_step.add_catch(create_step, errors=["Personalize.ResourceNotFoundException"],
                                result_path=step.result_path_error
                                )
        describe_step_after_create = step.describe("DescribeCampaignAfterCreate")

        new_campaign_creation_condition = sfn.Condition.is_present("$.SolutionVersionArn[0]")
        solution_version_arn_exists_condition = sfn.Condition.is_present("$.ServiceConfig.solutionVersionArn")
        new_campaign_run_condition = sfn.Condition.boolean_equals("$.CreateCampaign", True)

        should_execute_step = sfn.Choice(self.scope, step.object_type + " Execute?").when(
            sfn.Condition.and_(
                sfn.Condition.or_(new_campaign_creation_condition, solution_version_arn_exists_condition),
                new_campaign_run_condition),
            describe_step).otherwise(exit_step)

        campaign_active_condition = sfn.Condition.string_equals("$.DescribeCampaignInitial.Campaign.Status", "ACTIVE")

        is_campaign_active = sfn.Choice(self.scope, step.object_type + "Active?") \
            .when(
            sfn.Condition.and_(
                campaign_active_condition, solution_version_arn_exists_condition),
            step.update("$.ServiceConfig.solutionVersionArn", "UpdateCampaignExistingArn").next(
                describe_step_after_create)) \
            .when(sfn.Condition.and_(campaign_active_condition, new_campaign_creation_condition),
                  step.update("$.SolutionVersionArn[0]", "UpdateCampaignNewArn").next(
                      describe_step_after_create)).otherwise(exit_step)

        describe_step.next(is_campaign_active)

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


class CampaignStep(BaseStep):
    """
    A class that represents a step in the Campaign Step.

    This class inherits from the BaseStep class and is responsible for defining the configuration
    and behavior of the Campaign step.
    """
    SERVICE = "personalize"
    STEP_NAME = "CampaignTask"
    object_type = "Campaign"

    def __init__(self, scope):
        """
       Initialize a new instance of the CampaignStep class.

       Args:
           scope (Construct): The scope in which this construct is created.
       """
        super().__init__(scope)
        self.scope = scope
        self.create_step_arn_path = "campaignArn"

        self.id = CampaignStep.STEP_NAME
        self.result_path = f"$.{CampaignStep.STEP_NAME}"

        self.result_path_describe = "$.DescribeCampaignAfterCreate"
        self.result_path_update = "$.CampaignMetaData"
        self.result_path_create = "$.CreateCampaign"
        self.result_path_error = "$.DescribeCampaign.Error"
        self.put_event_config = {
            "arnPath": f"{self.result_path_describe}.Campaign.CampaignArn",
            "statusPath": f"{self.result_path_describe}.Campaign.Status",
            "message": "Personalize Campaign status change",
            "detail": {
                "SolutionVersionArn.$": f"{self.result_path_describe}.Campaign.SolutionVersionArn",
                "Name.$": f"{self.result_path_describe}.Campaign.Name"

            },
            "detailType": "Personalize Campaign status change",
        }

    def condition_success(self):
        """
       Define the condition for a successful Campaign.

       Returns:
           The condition for a successful Campaign.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + CampaignStep.object_type + ".Status", "ACTIVE")

    def condition_failure(self):
        """
       Define the condition for a failed Campaign.

       Returns:
           The condition for a failed Campaign.
       """
        return sfn.Condition.string_equals(
            self.result_path_describe + "." + CampaignStep.object_type + ".Status",
            "CREATE FAILED")

    def update(self, solution_version_arn, step_id):
        """
       Create a step to update the Campaign with a new solution version.

       Args:
           solution_version_arn (str): The Amazon Resource Name (ARN) of the solution version.
           step_id (str): The identifier for the update step.

       Returns:
           The step to update the Campaign.
       """
        payload = {
            "action": "updateCampaign",
            "service": CampaignStep.SERVICE,
            "id": step_id,
            "iam_resources": ["*"],
            "result_path": self.result_path_update,
            "parameters": {
                "CampaignArn.$": "States.Format('arn:aws:personalize:{}:{}:campaign/{}',$.Region,$.AccountID,"
                                 "$.ServiceConfig.name)",
                "SolutionVersionArn.$": solution_version_arn,
            },
            "object_type": CampaignStep.object_type
        }

        return self.create_call_aws_service_step(payload)

    def describe(self, step_id):
        """
        Create a step to describe the Campaign.

        Args:
            step_id (str): The identifier for the describe step.

        Returns:
            The step to describe the Campaign.
        """
        payload = {
            "action": "describeCampaign",
            "service": CampaignStep.SERVICE,
            "id": step_id,
            "iam_resources": ["*"],
            "result_path": f"$.{step_id}",
            "parameters": {
                "CampaignArn.$": "States.Format('arn:aws:personalize:{}:{}:campaign/{}',$.Region,$.AccountID,"
                                 "$.ServiceConfig.name)",
            },

            "object_type": CampaignStep.object_type
        }

        return super().create_call_aws_service_step(payload)

    def task(self, state_machine):
        """
        Create a step which executes a child state machine for creating a Personalize campaign.

        Args:
            state_machine (sfn.StateMachine): The state machine to which the task belongs.

        Returns:
            The task step for the Campaign step.

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
                                                     "Type": self.object_type,
                                                     "AccountID": sfn.JsonPath.string_at("$.AccountID"),
                                                     "SolutionVersionArn": sfn.JsonPath.string_at(
                                                         "$.SolutionVersionArn"),
                                                     "ServiceConfig": sfn.JsonPath.string_at(
                                                         "$.Campaign.serviceConfig"),
                                                     "CreateCampaign": sfn.JsonPath.string_at(
                                                         "$.Campaign.createCampaign"),

                                                 }),
                                                 result_selector={
                                                     "CampaignArn.$": "$..Output[?(@.DescribeCampaignAfterCreate"
                                                                      ")].DescribeCampaignAfterCreate"
                                                                      ".Campaign.CampaignArn",
                                                 }
                                                 )
        task.add_retry(backoff_rate=1.05, errors=["Personalize.Client.exceptions.LimitExceededException",
                                                  "Personalize.Client.exceptions.ResourceInUseException"],
                       interval=Duration.seconds(5), max_attempts=5)

        return task
