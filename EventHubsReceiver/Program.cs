using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using System.Net;
using Microsoft.Extensions.Configuration;
using LogAnalytics.Client;

namespace EventHubsReceiver
{
    class Program
    {
        static BlobContainerClient storageClient;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.        
        static EventProcessorClient processor;
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

            EventProcessorClientOptions eventProcessorClientOptions = new EventProcessorClientOptions
            {
                ConnectionOptions = connectionOptions
            };
#endif

#if DEBUG
            EventHubConnection connection = new EventHubConnection(config.GetConnectionString("EventHub"), config.GetSection("EventHubName").Value, connectionOptions);
#else
            EventHubConnection connection = new EventHubConnection(config.GetConnectionString("EventHub"),config.GetSection("EventHubName").Value);
#endif

            // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
            // Create a blob container client that the event processor will use 
            storageClient = new BlobContainerClient(config.GetConnectionString("StorageAccount"), config.GetSection("BlobContainerName").Value);

#if DEBUG
            // Create an event processor client to process events in the event hub
            processor = new EventProcessorClient(storageClient, consumerGroup, config.GetConnectionString("EventHub"), config.GetSection("EventHubName").Value, eventProcessorClientOptions);
#else
            processor = new EventProcessorClient(storageClient, consumerGroup, config.GetConnectionString("EventHub"), config.GetSection("EventHubName").Value);
#endif
            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 30 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(6000));

            // Stop the processing
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0} With Properties : {1}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()), string.Join(Environment.NewLine, eventArgs.Data.Properties));

            //push to Log Analytics
            //LogAnalyticsClient logAnalytics = new LogAnalyticsClient(config.GetSection("EventHubName").Value, config.GetSection("EventHubName").Value);

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
