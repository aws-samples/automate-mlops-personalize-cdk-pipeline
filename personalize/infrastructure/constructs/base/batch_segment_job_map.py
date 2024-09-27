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
Module for defining a Step Functions Map state for batch segment jobs.

This module contains a class `BatchSegmentJobMap` that creates a Map state in a Step Functions state machine
to iterate over a list of batch segment jobs and execute a specified flow for each job.
"""
from aws_cdk import (
    aws_stepfunctions as sfn,

)
from aws_cdk.aws_stepfunctions import JsonPath

from personalize.infrastructure.constructs.batch_segment_job import BatchSegmentJobFlow


class BatchSegmentJobMap:
    """
       Class to define a Step Functions Map state for batch segment jobs.

       This class creates a Map state in a Step Functions state machine to iterate over a list of batch segment jobs
       and execute a specified flow for each job.
    """
    state_machine = None

    def __init__(self, scope, construct_id):
        """
        Initialize the BatchSegmentJobMap instance.

        Args:
            scope (Construct): The scope in which the resources are defined.
            construct_id (str): The unique identifier for the construct.
        """
        self.scope = scope
        self.batch_segment_job_flow = BatchSegmentJobFlow(scope, construct_id)

    def build(self):
        """
        Build the Map state for batch segment jobs.

        Returns:
            sfn.item_processor: The item_processor for the Map state, which executes the specified flow for each batch segment job.
        """
        batch_segment_job_map = sfn.Map(self.scope, "BatchSegmentJobMap",
                                        max_concurrency=10,
                                        items_path="$.Solution.batchSegmentJobs",
                                        item_selector={
                                            "BatchSegmentJob": sfn.JsonPath.string_at("$$.Map.Item.Value"),
                                            "Region": self.scope.region,
                                            "AccountID": self.scope.account,
                                            "SolutionVersionArn": sfn.JsonPath.string_at(
                                                "$.SolutionVersionTask.Output[?("
                                                "@.CreateSolutionVersion)].CreateSolutionVersion"
                                                ".Payload.response.solutionVersionArn"),

                                        },
                                        result_path=JsonPath.DISCARD
                                        )

        return batch_segment_job_map.item_processor(self.batch_segment_job_flow.build())
