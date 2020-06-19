﻿using System;
using System.Text;
using System.Threading.Tasks;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace EventHubsReceiverConApp
{
  class Program
  {
    private const string ehubNamespaceConnectionString = "Endpoint=sb://rkeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=KqdGAnOr/4P9L8JkDa5VXNljm4qglW1VBQaSoGudQ+k=";
    private const string eventHubName = "myeventhub";
    private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=rkeventhubstorage;AccountKey=Dyrl2Cpgc0v3zjAFgWPlygZE7yvVUPq6XqHONbaal8KD9jNuMbJKGWgmjzz/5uj6+WJkM22IjR+OnQd96+0atA==;EndpointSuffix=core.windows.net";
    private const string blobContainerName = "rkcontainer";
    static async Task Main(string[] args)
    {
      // Read from the default consumer group: $Default
      string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

      // Create a blob container client that the event processor will use 
      BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

      // Create an event processor client to process events in the event hub
      EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

      // Register handlers for processing events and handling errors
      processor.ProcessEventAsync += ProcessEventHandler;
      processor.ProcessErrorAsync += ProcessErrorHandler;

      // Start the processing
      await processor.StartProcessingAsync();

      // Wait for 10 seconds for the events to be processed
      await Task.Delay(TimeSpan.FromSeconds(10));

      // Stop the processing
      await processor.StopProcessingAsync();
    }

    static Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
      // Write the body of the event to the console window
      Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
      return Task.CompletedTask;
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
