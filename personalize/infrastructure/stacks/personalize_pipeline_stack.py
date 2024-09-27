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
This module defines a CDK stack for deploying an Amazon Personalize MLOps pipeline.

The stack includes the following components:
- A Personalize MLOps pipeline with pre-processing, filters, event tracker, and recommendation configurations
- Suppression rules for CDK Nag to ignore specific IAM policy warnings

The PersonalizePipelineStack class is the main entry point for defining and deploying the stack.
"""
from aws_cdk import (
    Stack
)
from cdk_nag import NagSuppressions, NagPackSuppression
from constructs import Construct

from personalize.infrastructure.constructs.constants import Constants
from personalize.infrastructure.constructs.glue_job_run import PreprocessingGlueJobFlow
from personalize.infrastructure.constructs.pipelines.personalize_mlops_pipeline import \
    PersonalizeMlOpsPipeline


class PersonalizePipelineStack(Stack):
    """
    This class defines a CDK stack for deploying an Amazon Personalize MLOps pipeline.

    The stack includes the following components:
    - A Personalize MLOps pipeline with pre-processing, filters, event tracker, and recommendation configurations
    - Suppression rules for CDK Nag to ignore specific IAM policy warnings

    Attributes:
        scope (Construct): The scope in which this stack is defined.
        construct_id (str): The unique identifier for this stack.
        **kwargs: Additional keyword arguments for the stack.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """
        Initializes the PersonalizePipelineStack.

        Args:
            scope (Construct): The scope in which this stack is defined.
            construct_id (str): The unique identifier for this stack.
            **kwargs: Additional keyword arguments for the stack.
        """
        super().__init__(scope, construct_id, **kwargs)

        PersonalizeMlOpsPipeline(
            self, 'PersonalizePipelineSolution',
            # pre_processing_config={
            #     "job_class": PreprocessingGlueJobFlow
            # },
            # enable_filters=True,
            # enable_event_tracker=True,
            recommendation_config=[
                {
                    "type": "solutions",
                    "inference_options": ["campaigns"]
                },
                {
                    "type": "recommenders"
                }
            ]

        )

        self.cdk_nag_suppressions()

    def cdk_nag_suppressions(self):
        """
        Adds suppression rules for CDK Nag to ignore specific IAM policy warnings.

        This method defines a reason for suppressing the CDK Nag warning and
        adds suppression rules for various resource paths in the stack.
        """

        NagSuppressions.add_resource_suppressions_by_path(self,
                                                          f'/{self.stack_name}/PersonalizePipelineSolution'
                                                          f'/StateMachine/Role/DefaultPolicy/Resource',
                                                          [
                                                              NagPackSuppression(id="AwsSolutions-IAM5",
                                                                                 reason=Constants.AWS_SOLUTIONS_IAM5_REASON)],
                                                          True)

        NagSuppressions.add_resource_suppressions_by_path(self,
                                                          f'/{self.stack_name}/PersonalizePipelineSolution/apilambda'
                                                          f'/ServiceRole/DefaultPolicy/Resource',
                                                          [
                                                              NagPackSuppression(id="AwsSolutions-IAM5",
                                                                                 reason=Constants.AWS_SOLUTIONS_IAM5_REASON)],
                                                          True)

        NagSuppressions.add_resource_suppressions_by_path(self,
                                                          f'/{self.stack_name}/PersonalizePipelineSolution/apilambda'
                                                          f'/ServiceRole/Resource',
                                                          [
                                                              NagPackSuppression(id="AwsSolutions-IAM4",
                                                                                 reason=Constants.AWS_SOLUTIONS_IAM4_REASON)],
                                                          True)
