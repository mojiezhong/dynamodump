


#!/usr/bin/env python2
"""
    Simple backup and restore script for Amazon DynamoDB using boto to work similarly to mysqldump.

    Suitable for DynamoDB usages of smaller data volume which do not warrant the usage of AWS
    Data Pipeline for backup/restores/empty.

    dynamodump supports local DynamoDB instances as well (tested with dynalite).
"""

import argparse
import fnmatch
import json
import logging
import os
import shutil
import threading
import Queue
import datetime
import errno
import sys
import time
import re
import zipfile
import tarfile
import urllib2

import boto.dynamodb2.layer1
from boto.dynamodb2.exceptions import ProvisionedThroughputExceededException
import botocore
import boto3


print "Start geneerating data...."

JSON_INDENT = 2
AWS_SLEEP_INTERVAL = 10  # seconds
LOCAL_SLEEP_INTERVAL = 1  # seconds
BATCH_WRITE_SLEEP_INTERVAL = 0.15  # seconds
MAX_BATCH_WRITE = 25  # DynamoDB limit
SCHEMA_FILE = "schema.json"
DATA_DIR = "data"
MAX_RETRY = 6
LOCAL_REGION = "local"
LOG_LEVEL = "INFO"
DATA_DUMP = "dump"
RESTORE_WRITE_CAPACITY = 25
THREAD_START_DELAY = 1  # seconds
CURRENT_WORKING_DIR = os.getcwd()
DEFAULT_PREFIX_SEPARATOR = "-"
MAX_NUMBER_BACKUP_WORKERS = 25
METADATA_URL = "http://169.254.169.254/latest/meta-data/"

def _get_aws_client(profile, region, service):
    """
    Build connection to some AWS service.
    """

    if region:
        aws_region = region
    else:
        aws_region = os.getenv("AWS_DEFAULT_REGION")

    # Fallback to querying metadata for region
    if not aws_region:
        try:
            azone = urllib2.urlopen(METADATA_URL + "placement/availability-zone",
                                    data=None, timeout=5).read().decode()
            aws_region = azone[:-1]
        except urllib2.URLError:
            logging.exception("Timed out connecting to metadata service.\n\n")
            sys.exit(1)
        except urllib2.HTTPError as e:
            logging.exception("Error determining region used for AWS client.  Typo in code?\n\n" +
                              str(e))
            sys.exit(1)

    if profile:
        session = boto3.Session(profile_name=profile)
        client = session.client(service, region_name=aws_region)
    else:
        client = boto3.client(service, region_name=aws_region)
    return client




def get_table_name_by_tag(profile, region, tag):
    """
    Using provided connection to dynamodb and tag, get all tables that have provided tag

    Profile provided and, if needed, used to build connection to STS.
    """

    matching_tables = []
    all_tables = []
    sts = _get_aws_client(profile, region, "sts")
    dynamo = _get_aws_client(profile, region, "dynamodb")
    account_number = sts.get_caller_identity().get("Account")
    paginator = dynamo.get_paginator("list_tables")
    tag_key = tag.split("=")[0]
    tag_value = tag.split("=")[1]

    get_all_tables = paginator.paginate()
    for page in get_all_tables:
        for table in page["TableNames"]:
            all_tables.append(table)
            logging.debug("Found table " + table)

    for table in all_tables:
        table_arn = "arn:aws:dynamodb:{}:{}:table/{}".format(region, account_number, table)
        table_tags = dynamo.list_tags_of_resource(
            ResourceArn=table_arn
        )
        for found_tag in table_tags["Tags"]:
            if found_tag["Key"] == tag_key:
                logging.debug("Checking table " + table + " tag " + found_tag["Key"])
                if found_tag["Value"] == tag_value:
                    matching_tables.append(table)
                    logging.info("Matched table " + table)

    return matching_tables

def main():

    global args

    parser = argparse.ArgumentParse(description="Data generating to DynamoDB")