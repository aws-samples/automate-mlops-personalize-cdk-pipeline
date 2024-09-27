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
Module for creating a solution map in AWS Personalize.

This module defines a SolutionMap class that creates a map state in AWS Step Functions
to orchestrate the creation of multiple solutions and solution versions in AWS Personalize.
"""

from aws_cdk import (
    aws_stepfunctions as sfn,

)
from aws_cdk.aws_stepfunctions import JsonPath

from personalize.infrastructure.constructs.solution import SolutionFlow
from personalize.infrastructure.constructs.solution_version import SolutionVersionFlow


class SolutionMap:
    """
    Class representing a solution map in AWS Personalize.

    This class creates a map state in AWS Step Functions to create multiple solutions and
    solution versions in AWS Personalize concurrently. The solution and solution version
    creation steps are defined in the SolutionFlow and SolutionVersionFlow constructs,
    respectively.
    """
    state_machine = None

    def __init__(self, scope, inference_tasks):
        """
        Initialize a SolutionMap instance.

        Args: scope (Construct): The AWS CDK construct scope for the SolutionMap. inference_tasks (List[sfn.Chain]):
        A list of inference tasks (such as campaigns or batch inference tasks) to be executed after the solution
        version creation.
        """
        self.inference_tasks = inference_tasks
        self.scope = scope
        self.solution_flow = SolutionFlow(scope, "ForSolutionFlow")
        self.solution_version_flow = SolutionVersionFlow(scope, "ForSolutionFlow")

    def build(self):
        """
        Build the solution map in AWS Step Functions.

        Returns:
            sfn.item_processor: The AWS Step Functions item_processor representing the solution map.
        """
        solution_map = sfn.Map(self.scope, "SolutionFlowMap",
                               max_concurrency=10,
                               items_path="$.solutions",
                               item_selector={
                                   "Region": self.scope.region,
                                   "AccountID": self.scope.account,
                                   "Solution": sfn.JsonPath.string_at("$$.Map.Item.Value"),
                                   "DatasetGroup": sfn.JsonPath.object_at("$.datasetGroup"),
                                   "DatasetCreateImportJobArn": sfn.JsonPath.object_at(
                                       "$..DatasetMapOutput"
                                       "[?(@.DatasetCreateImportJobArn)]"
                                       ".DatasetCreateImportJobArn")
                               },
                               result_path=JsonPath.DISCARD
                               )

        return solution_map.item_processor(self.solution_flow.build().next(
            self.solution_version_flow.build().next(self.inference_tasks)))
