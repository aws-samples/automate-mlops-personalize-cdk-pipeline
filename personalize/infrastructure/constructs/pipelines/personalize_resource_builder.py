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
This module contains the PersonalizeResourceBuilder class, which is responsible for creating various
Amazon Personalize resources and orchestrating their interactions using AWS Step Functions.
"""
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    aws_lambda as _lambda, Stack

)
from constructs import Construct
from cdk_nag import NagSuppressions, NagPackSuppression

from personalize.infrastructure.constructs.constants import Constants
from personalize.infrastructure.constructs.base.batch_inference_job_map import BatchInferenceJobMap
from personalize.infrastructure.constructs.base.batch_segment_job_map import BatchSegmentJobMap
from personalize.infrastructure.constructs.base.campaign_map import CampaignMap
from personalize.infrastructure.constructs.base.dataset_map import DatasetMap
from personalize.infrastructure.constructs.base.parallel_flow import ParallelFlow
from personalize.infrastructure.constructs.base.recommender_map import RecommenderMap
from personalize.infrastructure.constructs.base.solution_map import SolutionMap
from personalize.infrastructure.constructs.dataset_group import DatasetGroupFlow
from personalize.infrastructure.constructs.event_tracker import EventTrackerFlow
from personalize.infrastructure.constructs.filter import FilterFlow


class PersonalizeResourceBuilder(Construct):
    """
   A construct that builds Amazon Personalize resources and orchestrates their interactions.

   This class should not be instantiated directly. Instead, create a subclass that extends
   PersonalizeResourceBuilder.
   """

    def __new__(cls, *args, **kwargs):
        """
        Prevent direct instantiation of the PersonalizeResourceBuilder class.

        Raises:
            TypeError: If the class being instantiated is PersonalizeResourceBuilder.
        """
        if cls is PersonalizeResourceBuilder:
            raise TypeError(f"Only children of {cls.__name__} may be instantiated")
        return object.__new__(cls)

    def __init__(self, scope: Construct, construct_id: str):
        """
        Initialize a new instance of the PersonalizeResourceBuilder.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        super().__init__(scope, construct_id)
        self.suffix = construct_id

    def create_dataset_fragment(self, scope):
        """
        Create a dataset fragment in form of a Step function Map state for the Personalize pipeline, which could
        create steps for schema, dataset and datasetImport tasks.

        Args:
            scope (Construct): The scope in which the dataset fragment should be created.

        Returns:
            The dataset fragment.
        """
        dataset_map = DatasetMap(scope).build()
        self.add_nag_suppression(scope, "CreateDatasetStateMachine")
        self.add_nag_suppression(scope, "CreateSchemaStateMachine")
        self.add_nag_suppression(scope, "CreateDatasetImportJobStateMachine")

        return dataset_map

    def create_inference_task_fragment(self, scope, solutions_config):
        """
        Create an inference task fragment for the Personalize pipeline. This step is part of the Step Functions
        workflow and responsible for creating the applicable inference steps such as creating campaigns,
        batch inference or batch segment jobs. These steps are configured to execute in parallel.

        Args:
            scope (Construct): The scope in which the inference task fragment should be created.
            solutions_config (dict): The configuration for the solutions.

        Returns:
            The inference task fragment, or a Pass task if no inference options are specified.
        """

        if "inference_options" not in solutions_config:
            return sfn.Pass(scope, "Solutions End")

        branch_map = {}
        inference_options = solutions_config["inference_options"]
        if "campaigns" in inference_options:
            campaign_map = CampaignMap(scope, self.suffix)
            branch_map["campaigns"] = campaign_map
            self.add_nag_suppression(scope, "CreateCampaignStateMachine")

        if "batchInferenceJobs" in inference_options:
            batch_inference_job_map = BatchInferenceJobMap(scope, self.suffix)
            branch_map["batchInferenceJobs"] = batch_inference_job_map
            self.add_nag_suppression(scope, "CreateBatchInferenceJobStateMachine")

        if "batchSegmentJobs" in inference_options:
            batch_segment_job_map = BatchSegmentJobMap(scope, self.suffix)
            branch_map["batchSegmentJobs"] = batch_segment_job_map
            self.add_nag_suppression(scope, "CreateBatchSegmentJobStateMachine")

        branches = list(branch_map.values())

        if len(branches) <= 0:
            return sfn.Pass(scope, "Solutions End")

        parallel_flow = ParallelFlow(scope, branches, "InferenceTasks")

        return parallel_flow.build()

    def create_solution_task_fragment(self, scope, recommendation_config):
        """
        Create a solution task fragment for the Personalize pipeline. This includes the steps foe creating solutions
        and recommender steps in the Step functions workflow. These steps are configured to be executed as parallel
        steps. This method also uses the create_inference_task_fragment method to create corresponding inference
        tasks for any solutions that have been configured while deploying the cdk app.

        Args:
            scope (Construct): The scope in which the solution task fragment should be created.
            recommendation_config (list): The configuration for the recommendations.

        Returns:
            The solution task fragment, or a Pass task if no solutions or recommenders are specified.
        """

        solutions_config = next((config for config in recommendation_config if
                                 isinstance(config, dict) and config.get(
                                     "type") == "solutions"), None)

        recommender_config = next((config for config in recommendation_config if
                                   isinstance(config, dict) and config.get(
                                       "type") == "recommenders"), None)

        branches = []

        if solutions_config:
            inference_tasks = self.create_inference_task_fragment(scope, solutions_config)

            solution_map = SolutionMap(scope, inference_tasks)
            branches.append(solution_map)
            self.add_nag_suppression(scope, "CreateSolutionStateMachine")
            self.add_nag_suppression(scope, "CreateSolutionVersionStateMachine")

        if recommender_config:
            recommender_map = RecommenderMap(scope, self.suffix)
            branches.append(recommender_map)
            self.add_nag_suppression(scope, "CreateRecommenderStateMachine")

        if len(branches) <= 0:
            return sfn.Pass(scope, "Pipeline End")

        parallel_flow = ParallelFlow(scope, branches, "Solution and Recommenders")

        return parallel_flow.build()

    def create_filter_fragment(self, scope):
        """
        Create a filter fragment for the Personalize pipeline. THis creates a Map state which creates a flow that can
        create multiple filters based on the configuration that was passed.

       Args:
           scope (Construct): The scope in which the filter fragment should be created.

       Returns:
           The filter fragment.
       """

        filter_map = sfn.Map(scope, "Filters",
                             max_concurrency=1,
                             items_path=sfn.JsonPath.string_at("$.filters"),
                             item_selector={
                                 "Item": sfn.JsonPath.string_at("$$.Map.Item.Value"),
                                 "DatasetGroup": sfn.JsonPath.object_at("$.datasetGroup"),
                                 "Region": scope.region,
                                 "AccountID": scope.account

                             },
                             result_path="$.FilterMapOutput"
                             )

        filter_map.item_processor(FilterFlow(scope, self.suffix).build())

        self.add_nag_suppression(scope, "CreateFilterStateMachine")

        return filter_map

    def create_dataset_group_fragment(self, scope):
        """
        Create a dataset group fragment for the Personalize pipeline. This is responsible for creating DatasetGroups

        Args:
            scope (Construct): The scope in which the dataset group fragment should be created.

        Returns:
            The dataset group fragment.
        """

        dataset_group_flow = DatasetGroupFlow(scope, self.suffix).build()

        self.add_nag_suppression(scope, "CreateDatasetGroupStateMachine")

        return dataset_group_flow

    def create_preprocessing_fragment(self, scope, config):
        """
        Create a preprocessing fragment for the Personalize pipeline. This is responsible for creating any
        preprocessing task step if configured while deploying the CDK app.

        Args:
            scope (Construct): The scope in which the preprocessing fragment should be created.
            config (dict): The configuration for the preprocessing job.

        Returns:
            The preprocessing fragment.
        """

        job_class = config["job_class"]

        job = job_class(scope, self.suffix).build()

        self.add_nag_suppression(scope, "CreateGlueJobStateMachine")

        return job

    def create_event_tracker_fragment(self, scope):
        """
       Create an event tracker fragment for the Personalize pipeline.

       Args:
           scope (Construct): The scope in which the event tracker fragment should be created.

       Returns:
           The event tracker fragment.
       """

        event_tracker_flow = EventTrackerFlow(scope, self.suffix).build()
        self.add_nag_suppression(scope, "CreateEventTrackerStateMachine")

        return event_tracker_flow

    def create_personalize_api_lambda(self, scope):
        """
        Create a Lambda function as a wrapper for calling the Personalize API. Tis step is used in every state
        machine which needs to make a personalize api call

        Args:
            scope (Construct): The scope in which the Lambda function should be created.

        Returns:
            The Lambda function.
        """

        api_lambda = _lambda.Function(
            self, 'apilambda',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='api.lambda_handler',
            code=_lambda.Code.from_asset('personalize/infrastructure/lambda',
                                         bundling={
                                             'image': _lambda.Runtime.PYTHON_3_12.bundling_image,
                                             'command': [
                                                 'bash', '-c',
                                                 'pip install -r requirements.txt -t /asset-output && cp api.py '
                                                 '/asset-output'
                                             ],
                                         }
                                         )
        )

        api_lambda.add_to_role_policy(
            iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                actions=['personalize:CreateDatasetGroup',
                                         'personalize:CreateDataset', 'personalize:UpdateDataset',
                                         'personalize:TagResource',
                                         'personalize:CreateDatasetImportJob', 'personalize:CreateFilter',
                                         'personalize:CreateSchema', 'personalize:CreateEventTracker',
                                         'personalize:CreateSolution', 'personalize:CreateSolutionVersion',
                                         'personalize:CreateCampaign', 'personalize:CreateBatchInferenceJob',
                                         'personalize:CreateRecommender',
                                         'personalize:CreateBatchSegmentJob', 'personalize:DescribeDataset',
                                         'personalize:DescribeSchema'],
                                resources=["arn:aws:personalize:*:*:schema/*",
                                           "arn:aws:personalize:*:*:dataset/*/*",
                                           "arn:aws:personalize:*:*:dataset-group/*",
                                           "arn:aws:personalize:*:*:dataset-import-job/*",
                                           "arn:aws:personalize:*:*:filter/*",
                                           "arn:aws:personalize:*:*:event-tracker/*",
                                           "arn:aws:personalize:*:*:solution/*",
                                           "arn:aws:personalize:*:*:solution-version/*",
                                           "arn:aws:personalize:*:*:campaign/*",
                                           "arn:aws:personalize:*:*:batch-inference-job/*",
                                           "arn:aws:personalize:*:*:batch-segment-job/*",
                                           "arn:aws:personalize:*:*:recommender/*",
                                           "arn:aws:personalize:*:*:recipe/*"]))

        api_lambda.add_to_role_policy(
            iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                actions=['iam:PassRole'],
                                resources=[f"arn:aws:iam::{scope.account}:role/*"],
                                conditions={'StringEquals': {'iam:PassedToService': 'personalize.amazonaws.com'}}
                                ))

        return api_lambda

    @staticmethod
    def create_put_event_task(scope):
        """
        Create a task to put an event in Amazon EventBridge.

        Args:
            scope (Construct): The scope in which the task should be created.

        Returns:
            The task to put an event in Amazon EventBridge.
        """
        return tasks.CallAwsService(scope, "PutEvent-Failure-Notification",
                                    service="eventbridge",
                                    action="putEvents",
                                    iam_resources=[scope.event_bus_arn],
                                    parameters={
                                        "Entries": [
                                            {
                                                "Detail": {
                                                    "ErrorMessage": "ErrorMessage",
                                                    "message": "Personalize Pipeline completion status change"

                                                },
                                                "DetailType": "Personalize MLOps pipeline completed with errors",
                                                "EventBusName": scope.event_bus_name,
                                                "Source": "solutions.aws.personalize"
                                            }
                                        ]
                                    },
                                    result_path="$.PutEvent",
                                    additional_iam_statements=[iam.PolicyStatement(
                                        actions=["events:PutEvents"],
                                        resources=[scope.event_bus_arn])]
                                    )

    @staticmethod
    def add_nag_suppression(scope: Construct, resource_path_prefix: str,
                            reason: str = Constants.AWS_SOLUTIONS_IAM5_REASON,
                            nag_pack_id: str = "AwsSolutions-IAM5") -> None:
        NagSuppressions.add_resource_suppressions_by_path(
            scope,
            f'/{scope.stack_name}/{resource_path_prefix}/Role/DefaultPolicy/Resource',
            [NagPackSuppression(id=nag_pack_id, reason=reason)],
            True
        )
