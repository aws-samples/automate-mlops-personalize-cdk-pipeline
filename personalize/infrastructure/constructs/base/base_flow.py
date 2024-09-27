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

"""This module defines a base class for building AWS Step Function state machines for various steps in an Amazon
Personalize MLOps pipeline.

The BaseFlow class provides methods to build Step Function state machine definitions for different steps,
such as creating, describing, and waiting for Personalize resources. It also includes a method to handle
domain-specific dataset group creation.

The module uses the AWS CDK libraries for Step Functions and CloudWatch Logs to create and configure the state
machines and log groups.

Key components:
- BaseFlow class:
    - Initializes the base flow with a scope and construct ID.
    - Builds Step Function state machine definitions for different steps.
    - Handles domain-specific dataset group creation.
    - Creates and configures Step Function state machines with logging and tracing.
"""
import uuid

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_logs as logs

)


class BaseFlow:
    """
    Base class for building AWS Step Function state machines for various steps in an Amazon Personalize MLOps pipeline.

    This class provides methods to create and configure Step Function state machines for different steps, such as
    creating, describing, and waiting for Personalize resources. It also includes a method to handle domain-specific
    dataset group creation.

    """
    state_machine_list = []

    def __init__(self, scope, construct_id):
        """
        Initialize the BaseFlow class.

        Args:
            scope (Construct): The scope in which this construct is created.
            construct_id (str): The unique identifier for this construct.
        """
        self.scope = scope
        self.id = construct_id

    def build_flow(self, step):
        """
           Build the Step Function state machine definition for a given step.

           Args:
               step (Step): The step object containing the necessary information to build the state machine.

           Returns:
               StateMachine: The AWS Step Function state machine.
       """
        state_machine_name = f"mlops-personalize-{step.object_type.lower()}-sm"

        definition = self.build_definition(step)

        log_group = logs.LogGroup(self.scope, f"{state_machine_name}-log-group",
                                  log_group_name=f"/aws/vendedlogs/states/{state_machine_name}-log-group-{uuid.uuid4()}")

        sm = sfn.StateMachine(
            self.scope, "Create" + step.object_type + "StateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            #definition=definition,
            state_machine_name=state_machine_name,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL
            ),
            tracing_enabled=True
        )

        # sm.add_to_role_policy(iam.PolicyStatement(
        #     actions=["iam:PassRole"],
        #     resources=["*"]))

        return sm

    def build_definition(self, step):
        """
        Build the Step Function state machine definition for a given step.

        Args:
            step (Step): The step object containing the necessary information to build the state machine definition.

        Returns:
            Definition: State machine definition.
        """
        create_step = step.create(step.STEP_NAME, step.result_path_create)

        describe_step = step.describe()
        describe_step.add_catch(create_step, errors=["Personalize.ResourceNotFoundException"],
                                result_path=step.result_path_error
                                )

        should_execute_step = sfn.Choice(self.scope, step.object_type + " should execute workflow?").when(
            sfn.Condition.is_present(f"$.{step.element_exists_check}"),
            describe_step).otherwise(
            step.exit_step())

        create_step.next(
            step.wait("Wait After Create").next(describe_step))

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.condition_created_in_current_execution(step)).otherwise(
                step.wait("Wait After Describe").
                next(describe_step)))

        return should_execute_step

    def build_definition_with_domain_check(self, step):
        """
        Build the Step Function state machine definition for a given step, also checks if domain is specified in the
        input and if yes creates a domain specific datasetgroup.

        Args:
            step (Step): The step object containing the necessary information to build the state machine definition.

        Returns:
             Definition: State machine definition.
        """
        create_step = step.create()
        domain_create_step = step.create(domain=True)
        wait_step = step.wait("Wait After Create")

        describe_step = step.describe()

        should_execute_step = sfn.Choice(self.scope, step.object_type + " should execute workflow?").when(
            sfn.Condition.is_present(f"$.{step.element_exists_check}"),
            describe_step).otherwise(
            step.exit_step())

        get_domain_state_array_length = sfn.Pass(self.scope, step.object_type + " prepare input",
                                                 parameters={
                                                     "domainArrayLength": sfn.JsonPath.array_length(
                                                         sfn.JsonPath.string_at('$.Domain'))},
                                                 result_path="$.domainArrayObject")

        choice = sfn.Choice(self.scope, step.object_type + " domain exists?").when(
            sfn.Condition.number_greater_than("$.domainArrayObject.domainArrayLength", 0),
            domain_create_step).otherwise(
            create_step)

        get_domain_state_array_length.next(choice)

        describe_step.add_catch(get_domain_state_array_length, errors=["Personalize.ResourceNotFoundException"],
                                result_path=step.result_path_error
                                )

        create_step.next(wait_step)
        domain_create_step.next(wait_step)
        wait_step.next(describe_step)

        describe_step.next(
            sfn.Choice(self.scope, step.object_type + " Active?").when(
                step.condition_failure(),
                step.send_event().next(step.fail("Failure", "Failure"))).when(
                step.condition_success(),
                step.send_event().next(sfn.Pass(self.scope, step.object_type + " End"))).otherwise(
                step.send_event().next(step.wait("Wait After Describe")).
                next(describe_step)))

        return should_execute_step
