import azure.durable_functions.orchestrator as o

def orchestrator_function(context: o.DurableOrchestrationContext):

    payload: str = context.get_input()

    parallel_tasks = []

    for payloadItem in payload:
        parallel_tasks.append(context.call_activity("applyPolicyActivity",payloadItem))
    
    result = context.task_all(parallel_tasks)

main = o.Orchestrator.create(orchestrator_function)