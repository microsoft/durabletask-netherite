import logging
import json

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    result1 = yield context.call_activity('HelloCityNetherite', "Tokyo")
    result2 = yield context.call_activity('HelloCityNetherite', "Seattle")
    result3 = yield context.call_activity('HelloCityNetherite', "London")
    return [result1, result2, result3]

main = df.Orchestrator.create(orchestrator_function)