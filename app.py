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
This module defines the entry point for deploying an Amazon Personalize MLOps pipeline using AWS CDK.

The module imports the necessary AWS CDK constructs and the PersonalizePipelineStack from the
personalize.infrastructure.stacks.personalize_pipeline_stack module. It then creates a CDK app,
adds the AwsSolutionsChecks aspect, and instantiates the PersonalizePipelineStack with the specified
environment variables. Finally, it synthesizes the CDK app.
"""

import os

import aws_cdk as cdk
from cdk_nag import (AwsSolutionsChecks)

from personalize.infrastructure.stacks.personalize_pipeline_stack import \
    PersonalizePipelineStack

app = cdk.App()

cdk.Aspects.of(app).add(AwsSolutionsChecks())

PersonalizePipelineStack(app, "personalize-pipeline-solution",
                         env=cdk.Environment(
                             account=os.environ["CDK_DEFAULT_ACCOUNT"],
                             region=os.environ["CDK_DEFAULT_REGION"]))
app.synth()
