# Standalone DF client

This sample demonstrates how to create a standalone durable client.

This client is constructed in a function app that is separate from the function app that runs the task hub. 

It is meant to be used in conjunction with the HelloDF sample which defines the orchestration that is being executed.

## How to run it

1. Set the environment variable EventHubsConnection to contain a connection string for an event hubs namespace. You cannot use emulation for creating a standalone client.

2. Start the HelloDF sample app so it runs the task hub. Keep this running (and watch for error messages in the log).

3. In a second window, start this app (StandaloneDFClient). Keep it running (and watch for error messages in the log).

4. Issue an HTTP request to the standalone client, like `curl http://localhost:7135/test`. You should receive (possibly after a few seconds) a return message saying 'client successfully started the instance'.