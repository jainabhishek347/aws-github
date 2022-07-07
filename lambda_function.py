from __future__ import print_function

import json

print('Loading function')


def lambda_handler(event, context):
    print("Hello World test in aws" + event)
    return event['key1']
