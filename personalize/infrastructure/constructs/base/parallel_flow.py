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
Module for creating a parallel flow in AWS Step Functions.

This module defines a ParallelFlow class that creates a parallel state in AWS Step Functions,
allowing multiple branches to execute concurrently.
"""

from aws_cdk import (
    aws_stepfunctions as sfn

)
from aws_cdk.aws_stepfunctions import JsonPath


class ParallelFlow:
    """
    Class representing a parallel flow in AWS Step Functions.

    This class creates a parallel state in AWS Step Functions, which allows multiple branches
    to execute concurrently. The branches are defined as separate flows, and the ParallelFlow
    class orchestrates their execution in parallel.
    """

    def __init__(self, scope, branches, step_name):
        """
            Initialize a ParallelFlow instance.

            Args:
                scope (Construct): The AWS CDK construct scope for the ParallelFlow.
                branches (list): A list of branch flows to execute in parallel.
                step_name (str): The name of the parallel step in AWS Step Functions.
            """
        self.branches = branches
        self.scope = scope
        self.step_name = step_name
        self.result_path = JsonPath.DISCARD

    def build(self):
        """
        Build the parallel flow in AWS Step Functions.

        Returns:
            sfn.Parallel: The AWS Step Functions parallel state representing the parallel flow.
        """
        parallel = sfn.Parallel(self.scope, self.step_name, result_path=self.result_path)

        for branch in self.branches:
            parallel.branch(branch.build())

        return parallel
