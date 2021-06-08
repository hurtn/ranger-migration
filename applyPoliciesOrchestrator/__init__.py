# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt


# SAMPLE ONLY
import logging
import json
from collections import namedtuple

import azure.functions as func

import azure.durable_functions.orchestrator as o

def orchestrator_function(context: o.DurableOrchestrationContext):

    payload: str = context.get_input()

    parallel_tasks = []

    for payloadItem in payload:
        parallel_tasks.append(context.call_activity("applyPolicyActivity",payloadItem))
                                                                            
    result = yield context.task_all(parallel_tasks)

main = o.Orchestrator.create(orchestrator_function)