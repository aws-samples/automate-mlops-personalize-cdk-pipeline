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
Module for creating a recommender map in AWS Personalize.

This module defines a RecommenderMap class that creates a map state in AWS Step Functions
to orchestrate the creation of multiple recommenders in AWS Personalize.
"""

from aws_cdk import (
    aws_stepfunctions as sfn,

)
from aws_cdk.aws_stepfunctions import JsonPath

from personalize.infrastructure.constructs.recommender import RecommenderFlow


class RecommenderMap:
    """
    Class representing a recommender map in AWS Personalize.

    This class creates a map state in AWS Step Functions to create multiple recommenders
    in AWS Personalize concurrently. The recommender creation process is defined in the
    RecommenderFlow construct.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initialize a RecommenderMap instance.

        Args:
            scope (Construct): The AWS CDK construct scope for the RecommenderMap.
            construct_id (str): The ID of the RecommenderMap construct.
        """
        self.scope = scope
        self.recommender_flow = RecommenderFlow(scope, construct_id)

        # self.branch = Branch([, , ])

    def build(self):
        """
        Build the recommender map in AWS Step Functions.

        Returns:
            sfn.item_processor: The AWS Step Functions item_processor representing the recommender map.
        """
        recommender_map = sfn.Map(self.scope, "RecommenderMap",
                                  max_concurrency=10,
                                  items_path="$.recommenders",
                                  item_selector={
                                      "Recommender": sfn.JsonPath.string_at("$$.Map.Item.Value"),
                                      "DatasetGroup.$": "$.datasetGroup",
                                      "Region": self.scope.region,
                                      "AccountID": self.scope.account,

                                  },
                                  result_path=JsonPath.DISCARD
                                  )

        return recommender_map.item_processor(self.recommender_flow.build())
