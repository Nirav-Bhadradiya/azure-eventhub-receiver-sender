using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Net;
using Microsoft.Extensions.Configuration;

namespace EventHubsSender
{
    class Program
    {
        // number of events to be sent to the event hub
        private const int numOfEvents = 300;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.
        static EventHubProducerClient producerClient;

        static async Task Main()
        {
            var config = new ConfigurationBuilder()
            .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .AddUserSecrets<Program>()
            .Build();
#if DEBUG
            //create proxy to make eventhub calls from local machine
            ICredentials credentials = CredentialCache.DefaultCredentials;
            IWebProxy proxy = new WebProxy(config.GetSection("ProxyHost").Value, 9000)
            {
                Credentials = credentials
            };
            //use AMQP WEbscokets transport to avoid connection issue from local machine, 
            // AMQPWebSocket uses TCP/443 to connect to eventhub instead port 5671,5672
            EventHubConnectionOptions connectionOptions = new EventHubConnectionOptions
            {
                TransportType = EventHubsTransportType.AmqpWebSockets,
                Proxy = proxy
            };
#endif

#if DEBUG
            EventHubConnection connection = new EventHubConnection(config.GetConnectionString("EventHub"),config.GetSection("EventHubName").Value, connectionOptions);
#else
            EventHubConnection connection = new EventHubConnection(config.GetConnectionString("EventHub"),config.GetSection("EventHubName").Value);
#endif
            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connection);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= numOfEvents; i++)
            {
                var eventBody = new BinaryData("Hello, Event Hubs!");
                var eventData = new EventData(eventBody);
                eventData.Properties.Add("EventType", "com.microsoft.samples.hello-event");
                eventData.Properties.Add("priority", 1);
                eventData.Properties.Add("score", 9.0);
                eventData.Properties.Add("MessageId", i);

                if (!eventBatch.TryAdd(eventData))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {numOfEvents} events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }
    }
}
