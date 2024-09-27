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
Module for interacting with the Amazon Personalize service using AWS Lambda functions.

This module defines several functions that interact with the Amazon Personalize service
to create and manage various resources, such as dataset groups, schemas, datasets, and
dataset import jobs.

The module uses the boto3 library to interact with the Amazon Personalize API.
"""

import json
import secrets
import string
from collections import Counter

import boto3

personalize = boto3.client('personalize')


def lambda_handler(event, context):
    """
    AWS Lambda handler function.

    This function is the entry point for the AWS Lambda function. It receives an event
    and a context object, and routes the event to the appropriate handler function based
    on the event type.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (obj): The context object for the Lambda function.

    Returns:
        dict: A dictionary containing the response and status of the operation.
    """
    try:
        print("api-event", event)
        event_type = f"{event['Type'].lower()}"

        handler = HANDLERS.get(event_type)

        if handler:
            response = handler(event)
            return {
                "response": response,
                "status": "SUCCEEDED",
            }
        else:
            return {
                "response": "Invalid event type",
                "status": "FAILED",
            }

    except Exception as ex:
        raise ex


def create_dataset_group(event):
    """
    Creates a new dataset group in Amazon Personalize.

    Args:
        event (dict): The event data containing the service configuration for the dataset group.

    Returns:
        dict: The response from the Amazon Personalize API for creating the dataset group.

    A dataset group in Amazon Personalize is a collection of related datasets (such as user data,
    item data, and interaction data) that are used to train a machine learning model for
    recommendations. This function creates a new dataset group based on the provided service
    configuration.

    The function expects the `event` parameter to be a dictionary containing the service
    configuration for the dataset group under the 'ServiceConfig' key. It then calls the
    `create_dataset_group` method of the Amazon Personalize client, passing in the service
    configuration as arguments.

    The response from the Amazon Personalize API for creating the dataset group is returned by
    this function.
    """

    return personalize.create_dataset_group(**event['ServiceConfig'])


def create_schema(event):
    """
        Creates a new schema in Amazon Personalize.

        Args:
            event (dict): The event data containing the schema configuration, dataset group information,
                          and other necessary details.

        Returns:
            dict: The response from the Amazon Personalize API for creating the schema.

        A schema in Amazon Personalize defines the structure of a dataset, including the names and data
        types of the fields. This function creates a new schema based on the provided schema
        configuration and dataset group information.

        The function first extracts the schema configuration and dataset group information from the
        `event` data. It then constructs the dataset ARN (Amazon Resource Name) based on the AWS region,
        account ID, dataset group name, and dataset type.

        If the dataset group has a domain specified, the function includes the domain in the schema
        creation request.

        The function then converts the schema dictionary to a JSON string and checks if the new schema
        is different from the original schema for the dataset. If the schemas are the same, it raises
        an exception.

        If the new schema is different, the function creates a new configuration dictionary with the
        schema as a string and generates a unique name for the schema based on the dataset group name,
        schema name, and schema version.

        Finally, the function calls the `create_schema` method of the Amazon Personalize client, passing
        in the new configuration and domain (if applicable) as arguments.

        The response from the Amazon Personalize API for creating the schema is returned by this
        function.
        """

    item = event["Item"]
    service_config = item['schema']['serviceConfig']

    dataset_arn = f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:" \
                  f"dataset/{event['DatasetGroup']['serviceConfig']['name']}/{item['type'].upper()}"

    domain = {}
    if 'domain' in event['DatasetGroup']['serviceConfig']:
        domain = {"domain": event['DatasetGroup']['serviceConfig']['domain']}

    # Keep reference to schema dict
    schema = service_config.pop('schema')

    # Convert schema to string
    schema_str = json.dumps(schema)

    if _is_original_schema_equal_to_new_schema(dataset_arn, schema):
        raise Exception("The original schema and new schema are the same")

    # Create new dict with all original keys + schema as string
    new_config = service_config.copy()
    new_config['schema'] = schema_str

    schema_name = f"{event['DatasetGroup']['serviceConfig']['name']}-{new_config['name']}-{item['schema']['schemaVersion']}"
    new_config['name'] = schema_name

    return personalize.create_schema(**new_config, **domain)


def create_update_dataset(event):
    """
        Creates or updates a dataset in Amazon Personalize.

        Args:
            event (dict): The event data containing the dataset configuration, schema information,
                          dataset group information, and other necessary details.

        Returns:
            dict: The response from the Amazon Personalize API for creating or updating the dataset.

        A dataset in Amazon Personalize is a collection of data that is used to train a machine learning
        model for recommendations. This function either creates a new dataset or updates an existing
        dataset based on the provided configuration and schema information.

        The function first constructs the dataset ARN (Amazon Resource Name) based on the AWS region,
        account ID, dataset group name, and dataset type.

        It then attempts to retrieve information about the existing dataset using the `describe_dataset`
        method of the Amazon Personalize client. If the dataset does not exist, it creates a new dataset
        by calling the `create_dataset` method with the provided dataset configuration, schema ARN,
        dataset group ARN, and dataset type.

        If the dataset already exists, the function checks if the new schema is different from the
        original schema for the dataset by calling the `_is_original_schema_equal_to_new_schema`
        helper function. If the schemas are the same, it returns the existing dataset information.

        If the new schema is different, the function calls the `update_dataset` method of the Amazon
        Personalize client, passing in the new schema ARN and dataset ARN as arguments to update the
        dataset with the new schema.

        The response from the Amazon Personalize API for creating or updating the dataset is returned
        by this function.
        """

    item = event["Item"]
    schema_service_config = item['schema']['serviceConfig']

    dataset_arn = f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:" \
                  f"dataset/{event['DatasetGroup']['serviceConfig']['name']}/{item['type'].upper()}"

    try:
        response = personalize.describe_dataset(
            datasetArn=dataset_arn
        )

    except personalize.exceptions.ResourceNotFoundException:
        return personalize.create_dataset(
            **event['Item']["dataset"]["serviceConfig"], **{
                "schemaArn": event['SchemaArn'][0],
                "datasetGroupArn": f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:dataset-group/"
                                   f"{event['DatasetGroup']['serviceConfig']['name']}",
                "datasetType": event['Item']['type']
            }
        )
    schema = schema_service_config['schema']

    if _is_original_schema_equal_to_new_schema(dataset_arn, schema):

        return {
            'name': response['dataset']['name'],
            'datasetArn': response['dataset']['datasetArn'],
            'datasetGroupArn': response['dataset']['datasetGroupArn'],
            'datasetType': response['dataset']['datasetType'],
            'schemaArn': response['dataset']['schemaArn'],
            'status': response['dataset']['status'],
        }
    else:
        return personalize.update_dataset(
            **{
                "schemaArn": event['SchemaArn'][0],
                "datasetArn": dataset_arn
            }
        )


def create_dataset_import_job(event):
    """
    Creates a new dataset import job in Amazon Personalize.

    Args:
        event (dict): The event data containing the dataset import job configuration, dataset group
                      information, and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the dataset import job.

    A dataset import job in Amazon Personalize is used to import data into a dataset from a specified
    data source. This function creates a new dataset import job based on the provided configuration
    and dataset group information.

    The function first retrieves the dataset import job configuration from the `event` data. It then
    generates a unique job name by appending a random string to the provided job name in the
    configuration.

    The function constructs the dataset ARN (Amazon Resource Name) based on the AWS region, account
    ID, dataset group name, and dataset type.

    Finally, the function calls the `create_dataset_import_job` method of the Amazon Personalize
    client, passing in the updated dataset import job configuration and the constructed dataset ARN
    as arguments.

    The response from the Amazon Personalize API for creating the dataset import job is returned by
    this function.
    """

    service_config = event['Item']["datasetImportJob"]["serviceConfig"]

    service_config['jobName'] = f"{service_config['jobName']}-{_generate_secure_random_string(12)}"

    return personalize.create_dataset_import_job(
        **service_config,
        **{"datasetArn":
               f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:dataset"
               f"/{event['DatasetGroup']['serviceConfig']['name']}"
               f"/{event['Item']['type']}"}
    )


def create_filter(event):
    """
    Creates a new filter in Amazon Personalize.

    Args:
        event (dict): The event data containing the filter configuration, dataset group information,
                      and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the filter.

    A filter in Amazon Personalize is used to exclude specific data from being imported into a dataset.
    This function creates a new filter based on the provided configuration and dataset group information.

    The function constructs the datasetGroupArn by combining the AWS region, account ID, and dataset group
    name from the event data. It then calls the create_filter method of the Amazon Personalize client,
    passing in the filter configuration and the constructed datasetGroupArn as arguments.

    The response from the Amazon Personalize API for creating the filter is returned by this function.
    """
    return personalize.create_filter(
        **event['Item']['serviceConfig'],
        **{"datasetGroupArn": f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:dataset-group/"
                              f"{event['DatasetGroup']['serviceConfig']['name']}",

           }
    )


def create_event_tracker(event):
    """
   Creates a new event tracker in Amazon Personalize.

   Args:
       event (dict): The event data containing the event tracker configuration, dataset group information,
                     and other necessary details.

   Returns:
       dict: The response from the Amazon Personalize API for creating the event tracker.

   An event tracker in Amazon Personalize is used to record user interactions with items, such as clicks,
   purchases, or other events. This function creates a new event tracker based on the provided configuration
   and dataset group information.

   The function constructs the datasetGroupArn by combining the AWS region, account ID, and dataset group
   name from the event data. It then calls the create_event_tracker method of the Amazon Personalize client,
   passing in the event tracker configuration and the constructed datasetGroupArn as arguments.

   The response from the Amazon Personalize API for creating the event tracker is returned by this function.
   """
    return personalize.create_event_tracker(
        **event['ServiceConfig'][0],
        **{"datasetGroupArn": f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:dataset-group/"
                              f"{event['DatasetGroup']['serviceConfig']['name']}"}
    )


def create_solution(event):
    """
   Creates a new solution in Amazon Personalize.

   Args:
       event (dict): The event data containing the solution configuration, dataset group information,
                     and other necessary details.

   Returns:
       dict: The response from the Amazon Personalize API for creating the solution.

   A solution in Amazon Personalize is a combination of a recipe, customized parameters, and one or more
   solution versions (trained models). This function creates a new solution based on the provided configuration
   and dataset group information.

   The function constructs the datasetGroupArn by combining the AWS region, account ID, and dataset group
   name from the event data. It then calls the create_solution method of the Amazon Personalize client,
   passing in the solution configuration and the constructed datasetGroupArn as arguments.

   The response from the Amazon Personalize API for creating the solution is returned by this function.
   """
    return personalize.create_solution(
        **event['ServiceConfig'],
        **{"datasetGroupArn": f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:dataset-group/"
                              f"{event['DatasetGroup']['serviceConfig']['name']}"}
    )


def create_solution_version(event):
    """
    Creates a new solution version in Amazon Personalize.

    Args:
        event (dict): The event data containing the solution version configuration, solution information,
                      and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the solution version.

    A solution version in Amazon Personalize represents a trained model that can be deployed for generating
    recommendations. This function creates a new solution version based on the provided configuration
    and solution information.

    The function constructs the solutionArn by combining the AWS region, account ID, and solution name
    from the event data. It then calls the create_solution_version method of the Amazon Personalize client,
    passing in the solution version configuration and the constructed solutionArn as arguments.

    The response from the Amazon Personalize API for creating the solution version is returned by this function.
    """
    return personalize.create_solution_version(
        **event['SolutionVersionServiceConfig'][0],
        **{"solutionArn": f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:solution/"
                          f"{event['SolutionServiceConfig']['name']}"}
    )


def create_campaign(event):
    """
    Creates a new campaign in Amazon Personalize.

    Args:
        event (dict): The event data containing the campaign configuration, solution version information,
                      and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the campaign.

    A campaign in Amazon Personalize is a deployment of a solution version that can be used to generate
    recommendations. This function creates a new campaign based on the provided configuration and
    solution version information.

    The function checks if the solutionVersionArn is present in the campaign configuration. If not, it
    constructs a new service_config dictionary by combining the campaign configuration and the
    solutionVersionArn from the event data. Otherwise, it uses the campaign configuration as is.

    It then calls the create_campaign method of the Amazon Personalize client, passing in the service_config
    as an argument.

    The response from the Amazon Personalize API for creating the campaign is returned by this function.
    """
    if "solutionVersionArn" not in event['ServiceConfig']:
        service_config = {
            **event['ServiceConfig'],
            **{"solutionVersionArn": event['SolutionVersionArn'][0]}
        }
    else:
        service_config = event['ServiceConfig']

    return personalize.create_campaign(
        **service_config
    )


def create_batch_inference_job(event):
    """
    Creates a new batch inference job in Amazon Personalize.

    Args:
        event (dict): The event data containing the batch inference job configuration, solution version information,
                      and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the batch inference job.

    A batch inference job in Amazon Personalize is used to generate recommendations for a large number of users
    or items. This function creates a new batch inference job based on the provided configuration and
    solution version information.

    The function constructs a unique job name by appending a random string to the provided job name. It then
    checks if the solutionVersionArn is present in the batch inference job configuration. If not, it constructs
    a new service_config dictionary by combining the batch inference job configuration and the solutionVersionArn
    from the event data. Otherwise, it uses the batch inference job configuration as is.

    It then calls the create_batch_inference_job method of the Amazon Personalize client, passing in the
    service_config as an argument.

    The response from the Amazon Personalize API for creating the batch inference job is returned by this function.
    """
    service_config = event["ServiceConfig"]

    service_config['jobName'] = f"{service_config['jobName']}-{_generate_secure_random_string(12)}"

    if "solutionVersionArn" not in service_config:
        service_config = {
            **service_config,
            **{"solutionVersionArn": event['SolutionVersionArn'][0]}
        }
    else:
        service_config = service_config

    return personalize.create_batch_inference_job(
        **service_config
    )


def create_batch_segment_job(event):
    """
    Creates a new batch segment job in Amazon Personalize.

    Args:
        event (dict): The event data containing the service configuration for the batch segment job,
                      the solution version ARN, and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the batch segment job.

    A batch segment job in Amazon Personalize is used to generate recommendations or user segments
    based on a trained solution version. This function creates a new batch segment job based on the
    provided service configuration and solution version ARN.

    The function first retrieves the service configuration from the event data. It then generates a
    unique job name by appending a random string to the provided job name in the service configuration.

    If the service configuration does not include a solution version ARN, the function adds the
    solution version ARN from the event data to the service configuration.

    Finally, the function calls the create_batch_segment_job method of the Amazon Personalize client,
    passing in the updated service configuration as arguments.

    The response from the Amazon Personalize API for creating the batch segment job is returned by
    this function.
    """
    service_config = event["ServiceConfig"]

    service_config['jobName'] = f"{service_config['jobName']}-{_generate_secure_random_string(12)}"

    if "solutionVersionArn" not in service_config:
        service_config = {
            **service_config,
            **{"solutionVersionArn": event['SolutionVersionArn'][0]}
        }
    else:
        service_config = service_config

    return personalize.create_batch_segment_job(
        **service_config
    )


def create_recommender(event):
    """
    Creates a new recommender in Amazon Personalize.

    Args:
        event (dict): The event data containing the service configuration for the recommender,
                      dataset group information, and other necessary details.

    Returns:
        dict: The response from the Amazon Personalize API for creating the recommender.

    A recommender in Amazon Personalize is a machine learning model that generates recommendations
    based on the data in a dataset group. This function creates a new recommender based on the
    provided service configuration and dataset group information.

    The function constructs the datasetGroupArn by combining the AWS region, account ID, and dataset
    group name from the event data. It then calls the create_recommender method of the Amazon
    Personalize client, passing in the service configuration and the constructed datasetGroupArn as
    arguments.

    The response from the Amazon Personalize API for creating the recommender is returned by this
    function.
    """
    return personalize.create_recommender(
        **event["ServiceConfig"],
        **{"datasetGroupArn": f"arn:aws:personalize:{event['Region']}:{event['AccountID']}:dataset-group/"
                              f"{event['DatasetGroup']['serviceConfig']['name']}"}
    )


def _generate_secure_random_string(length):
    """
    Generates a cryptographically secure random string of the specified length.
    uses https://peps.python.org/pep-0506/ to generate cryptographically strong pseudo-random numbers

    Args:
        length (int): The desired length of the random string.

    Returns:
        str: A random string of the specified length.
    """

    characters = string.ascii_lowercase + string.digits
    secure_random_string = ''.join(secrets.choice(characters) for _ in range(length))
    return secure_random_string


def _convert_type(field_type):
    """
    Converts a field type to a tuple if it is a list.

    Args:
        field_type (list or other): The field type to be converted.

    Returns:
        tuple or original type: The converted field type as a tuple if it was a list, or the original type otherwise.
    """
    if isinstance(field_type, list):
        return tuple(field_type)
    else:
        return field_type


def _compare_schemas(schema1, schema2):
    """
    Compares two schemas to determine if they are equal. Check if the schemas have the same type, name, namespace,
    and version

    Args:
        schema1 (dict): The first schema to be compared.
        schema2 (dict): The second schema to be compared.

    Returns:
        bool: True if the schemas are equal, False otherwise.
    """
    #

    keys = set(schema1.keys()) | set(schema2.keys())

    for key in keys:
        if schema1.get(key) != schema2.get(key):
            return False

    # Compare the fields
    fields1 = schema1.get("fields", [])
    fields2 = schema2.get("fields", [])

    if len(fields1) != len(fields2):
        return False

    # Create a Counter to count the occurrences of field types and categorical flags
    field_counter1 = Counter(
        (field["name"], _convert_type(field["type"]), field.get("categorical", False), field.get("textual", False))
        for field in fields1)
    field_counter2 = Counter(
        (field["name"], _convert_type(field["type"]), field.get("categorical", False), field.get("textual", False))
        for field in fields2)

    return field_counter1 == field_counter2


def _is_original_schema_equal_to_new_schema(dataset_arn, new_schema):
    """
   Checks if the original schema for a dataset is equal to a new schema.

   Args:
       dataset_arn (str): The Amazon Resource Name (ARN) of the dataset.
       new_schema (dict): The new schema to be compared.

   Returns:
       bool: True if the original schema is equal to the new schema, False otherwise.
   """
    try:
        response = personalize.describe_dataset(
            datasetArn=dataset_arn
        )

    except personalize.exceptions.ResourceNotFoundException:
        return False

    original_schema = ""

    if 'dataset' in response:
        schema_arn = response["dataset"]["schemaArn"]

        schema_response = personalize.describe_schema(
            schemaArn=schema_arn
        )

        if "schema" in schema_response:
            original_schema = schema_response["schema"]["schema"]

    if original_schema != "":
        return _compare_schemas(json.loads(original_schema), new_schema)
    else:
        return False


"""
HANDLERS: A dictionary that maps event types to their corresponding handler functions.

This dictionary is used to route incoming events to the appropriate handler function based on the
event type. The keys in the dictionary represent the event types, and the values are the names of
the functions that should be called to handle events of that type.

Keys:
    "datasetgroup" (str): The event type for creating a dataset group in Amazon Personalize.
    "schema" (str): The event type for creating a schema in Amazon Personalize.
    "dataset" (str): The event type for creating or updating a dataset in Amazon Personalize.
    "datasetimportjob" (str): The event type for creating a dataset import job in Amazon Personalize.
    "filter" (str): The event type for creating a filter in Amazon Personalize.
    "eventtracker" (str): The event type for creating an event tracker in Amazon Personalize.
    "solution" (str): The event type for creating a solution in Amazon Personalize.
    "solutionversion" (str): The event type for creating a solution version in Amazon Personalize.
    "campaign" (str): The event type for creating a campaign in Amazon Personalize.
    "batchinferencejob" (str): The event type for creating a batch inference job in Amazon Personalize.
    "batchsegmentjob" (str): The event type for creating a batch segment job in Amazon Personalize.
    "recommender" (str): The event type for creating a recommender in Amazon Personalize.

Values:
    create_dataset_group (function): The function to handle events of type "datasetgroup".
    create_schema (function): The function to handle events of type "schema".
    create_update_dataset (function): The function to handle events of type "dataset".
    create_dataset_import_job (function): The function to handle events of type "datasetimportjob".
    create_filter (function): The function to handle events of type "filter".
    create_event_tracker (function): The function to handle events of type "eventtracker".
    create_solution (function): The function to handle events of type "solution".
    create_solution_version (function): The function to handle events of type "solutionversion".
    create_campaign (function): The function to handle events of type "campaign".
    create_batch_inference_job (function): The function to handle events of type "batchinferencejob".
    create_batch_segment_job (function): The function to handle events of type "batchsegmentjob".
    create_recommender (function): The function to handle events of type "recommender".

Example usage:
    event_type = event["Type"].lower()
    handler = HANDLERS.get(event_type)

    if handler:
        response = handler(event)
        # Process the response as needed
    else:
        # Handle invalid event type
"""
HANDLERS = {
    "datasetgroup": create_dataset_group,
    "schema": create_schema,
    "dataset": create_update_dataset,
    "datasetimportjob": create_dataset_import_job,
    "filter": create_filter,
    "eventtracker": create_event_tracker,
    "solution": create_solution,
    "solutionversion": create_solution_version,
    "campaign": create_campaign,
    "batchinferencejob": create_batch_inference_job,
    "batchsegmentjob": create_batch_segment_job,
    "recommender": create_recommender,

}
