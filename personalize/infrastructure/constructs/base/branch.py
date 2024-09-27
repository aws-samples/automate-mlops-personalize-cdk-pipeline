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
Module for defining a Branch in a Step Functions state machine.

This module contains a class `Branch` that represents a sequence of steps in a Step Functions state machine.
The `Branch` class provides a way to define and build a branch of steps that can be used in the state machine.
"""


class Branch:
    """
    Class representing a sequence of steps in a Step Functions state machine.

    This class allows you to define a branch of steps that can be executed in a specific order.
    The `build` method constructs the branch definition by chaining the steps together.
    """

    def __init__(self, branch_steps):
        """
        Initialize the Branch instance.

        Args:
            branch_steps (list): A list of step objects that make up the branch.
        """
        self.branch_steps = branch_steps

    def build(self):
        """
       Build the branch definition by chaining the steps together.

       Returns:
           The branch definition as a Step Functions state.
       """
        branch_definition = ""
        if len(self.branch_steps) > 0:
            first_step = self.branch_steps[0].build()
            branch_definition = first_step
            for branch_step in self.branch_steps[1:]:
                first_step = first_step.next(branch_step.build())
                branch_definition = first_step
        return branch_definition
