import * as df from "durable-functions"

const orchestrator = df.orchestrator(function* (context) {
    const outputs = []

    if (context.df.isReplaying === false) {
        context.log(`Calling first Activity`)
    }
    else {
        context.log(`Replay first Activity`)
    }
    outputs.push(yield context.df.callActivity("HelloCityNetherite", "Tokyo"))

    context.log(`Calling second activity`)
    outputs.push(yield context.df.callActivity("HelloCityNetherite", "Seattle"))

    context.log(`Calling third activity`)
    outputs.push(yield context.df.callActivity("HelloCityNetherite", "London"))

    return outputs
})

export default orchestrator
