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
This module defines a personalized machine learning operations (MLOps) pipeline
using the AWS CDK. The pipeline includes various components such as preprocessing,
dataset groups, datasets, filters, event trackers, solutions, and inference tasks.
The pipeline is defined using AWS Step Functions and other AWS services.

The main class in this module is PersonalizeMlOpsPipeline, which inherits from
the PersonalizeResourceBuilder class. This class is responsible for creating and
configuring the various components of the pipeline, including:

- Preprocessing steps
- Dataset groups
- Datasets
- Filters
- Event trackers
- Solutions
- Inference tasks

The module also creates an AWS EventBus and an AWS LogGroup for logging purposes.
"""
import uuid
from functools import reduce

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_events as events,
    aws_logs as logs

)
from constructs import Construct

from personalize.infrastructure.constructs.pipelines.personalize_resource_builder import \
    PersonalizeResourceBuilder


class PersonalizeMlOpsPipeline(PersonalizeResourceBuilder):
    """
   This class represents a personalized machine learning operations (MLOps) pipeline.
   It inherits from the PersonalizeResourceBuilder class and is responsible for creating
   and configuring various components of the pipeline, such as preprocessing, dataset
   groups, datasets, filters, event trackers, solutions, and inference tasks.
   """

    def __init__(self, scope: Construct, construct_id: str, pre_processing_config=None, enable_filters=False,
                 enable_event_tracker=False,
                 recommendation_config=None):
        """
        Initializes a new instance of the PersonalizeMlOpsPipeline class.

        Args: scope (Construct): The scope in which this construct is defined. construct_id (str): The unique
        identifier for this construct. pre_processing_config (Optional[Any]): Configuration for preprocessing steps.
        enable_filters (bool): Whether to enable filters in the pipeline. enable_event_tracker (bool): Whether to
        enable an event tracker in the pipeline. recommendation_config (Optional[List]): Configuration for
        recommendation tasks such as when you want to create solutions and/or recommenders
        """
        super().__init__(scope, construct_id)

        if recommendation_config is None:
            recommendation_config = []
        self.prefix = construct_id

        scope.api_lambda = self.create_personalize_api_lambda(scope)

        bus = events.EventBus(self, "bus",
                              event_bus_name="mlops-event-bus"
                              )

        scope.event_bus_arn = bus.event_bus_arn
        scope.event_bus_name = bus.event_bus_name

        steps = []

        if pre_processing_config is not None:
            pre_processing_step = self.create_preprocessing_fragment(scope, pre_processing_config)
            steps.append(pre_processing_step)

        dataset_group_step = self.create_dataset_group_fragment(scope)
        steps.append(dataset_group_step)

        # Create a dataset parallel step in the state machine

        dataset_map_step = self.create_dataset_fragment(scope)

        steps.append(dataset_map_step)

        # Create a filter and event tracker steps

        if enable_filters:
            filter_step = self.create_filter_fragment(scope)
            steps.append(filter_step)

        if enable_event_tracker:
            event_tracker_step = self.create_event_tracker_fragment(scope)
            steps.append(event_tracker_step)

        # Create solution steps

        solution_tasks = self.create_solution_task_fragment(scope, recommendation_config)
        steps.append(solution_tasks)

        # Create inference tasks

        pipeline = reduce(lambda x, y: x.next(y), steps)

        parallel = sfn.Parallel(scope, "ManagedExecution")

        parallel.branch(pipeline)

        send_failure_notification = self.create_put_event_task(scope)
        parallel.add_catch(send_failure_notification)

        fail_state = sfn.Fail(
            self, "FailState",
            cause_path="$.Cause",
            error_path="$.Error"
        )

        send_failure_notification.next(fail_state)

        log_group = logs.LogGroup(self, "mlops-personalize-state-machine",
                                  log_group_name=f"/aws/vendedlogs/states/mlops-personalize-sm-{uuid.uuid4()}")

        sfn.StateMachine(
            self, "StateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(parallel),
            # definition=parallel,
            state_machine_name=self.prefix,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL
            ),
            tracing_enabled=True
        )
