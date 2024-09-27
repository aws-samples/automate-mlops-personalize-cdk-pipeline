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
Module for creating and managing datasets in AWS Personalize.

This module defines a DatasetMap class that orchestrates the creation and import of datasets
using AWS Step Functions. It utilizes the SchemaFlow, DatasetFlow, and DatasetImportJobFlow
constructs to handle the individual steps of the process.

The DatasetMap class provides a way to create and import multiple datasets concurrently,
with a maximum concurrency of 3.
"""
from aws_cdk import (
    aws_stepfunctions as sfn,

)

from personalize.infrastructure.constructs.dataset import DatasetFlow
from personalize.infrastructure.constructs.dataset_import_job import DatasetImportJobFlow
from personalize.infrastructure.constructs.schema import SchemaFlow


class DatasetMap:
    """
       A class representing a dataset map for creating and importing datasets.

       Attributes:
           state_machine (sfn.StateMachine): The AWS Step Functions state machine for the dataset map.
    """
    state_machine = None

    def __init__(self, scope):
        """
        Initialize a DatasetMap instance.

        Args:
            scope (Construct): The AWS CDK construct scope for the DatasetMap.
        """
        self.scope = scope
        self.schema_flow = SchemaFlow(scope, "SchemaFlow")
        self.dataset_flow = DatasetFlow(scope, "DatasetFlow")
        self.dataset_import_flow = DatasetImportJobFlow(scope, "DatasetImportJobFlow")

    def build(self):
        """
        Build the dataset map state machine.

        Returns:
            sfn.Chain: The AWS Step Functions chain representing the dataset map state machine.
        """
        dataset_map = sfn.Map(self.scope, "Create and Import Datasets",
                              max_concurrency=3,
                              items_path="$.datasetGroup.datasets",
                              item_selector={
                                  "Item": sfn.JsonPath.string_at("$$.Map.Item.Value"),
                                  "DatasetGroup.$": "$.datasetGroup",
                                  "Region": self.scope.region,
                                  "AccountID": self.scope.account,

                              },
                              result_path="$.DatasetMapOutput"
                              )

        return dataset_map.item_processor(self.schema_flow.build().next(
            self.dataset_flow.build().next(self.dataset_import_flow.build())))
