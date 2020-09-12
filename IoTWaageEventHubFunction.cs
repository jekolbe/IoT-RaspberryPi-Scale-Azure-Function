using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace IoTPrototyp.IoTWaageEventHubFunction
{
    public static class IoTWaageEventHubFunction
    {
        [FunctionName("IoTWaageEventHubFunction")]
        public static async Task Run([EventHubTrigger("iothub-ehub-iot-waage-4481725-55a5cfcbfa", Connection = "iot-waage")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    Dictionary<string, string> messageObj = JsonConvert.DeserializeObject<Dictionary<string, string>>(messageBody);
                    string device = messageObj.TryGetValue("device", out device) ? device : "unknown";

                    var connection = new HubConnectionBuilder()
                        .WithUrl("https://iotwebapp-prototypwaage.azurewebsites.net/messageHub")
                        .Build();

                    connection.StartAsync().Wait();

                    connection.InvokeAsync("SendMessage", device, messageBody).Wait();

                    connection.DisposeAsync().Wait();

                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
