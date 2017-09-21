namespace DocumentDB.Samples.Queries

{

    using Microsoft.Azure.Documents;

    using Microsoft.Azure.Documents.Client;

    using Microsoft.Azure.Documents.Linq;

    using Newtonsoft.Json;

    using Newtonsoft.Json.Serialization;

    using System;

    using System.Collections.Generic;

    using System.Configuration;

    using System.Threading.Tasks;

    using Newtonsoft.Json.Converters;
    using System.Threading;



    //------------------------------------------------------------------------------------------------

    // This sample demonstrates how 

    //------------------------------------------------------------------------------------------------



    public class Program

    {

        private static DocumentClient client;

        // Assign an id for your database & collection 
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["database"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["collection"];
        private static readonly string endpointUrl = ConfigurationManager.AppSettings["endpoint"];
        private static readonly string authorizationKey = ConfigurationManager.AppSettings["authKey"];

        public static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            try
            {
                //Get a Document client
                using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey,
                    //for documentDB
                    new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
                {
                    await RunDemoAsync(DatabaseName, CollectionName);
                }
            }
            catch (Exception e)
            {
                LogException(e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }


        private static async Task RunDemoAsync(string databaseId, string collectionId)

        {
            
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            Console.WriteLine("Reading all changes from the beginning");
            //Get the checkpoint
            Dictionary<string, string> checkpoints = await GetChanges(client, collectionUri, new Dictionary<string, string>());

            do
            {
                checkpoints = await GetChanges(client, collectionUri, checkpoints);
                Console.Write(".");
                Thread.Sleep(1000);
            } while (true);

        }



        private static async Task<Dictionary<string, string>> GetChanges(
                                                DocumentClient client,
                                                Uri collectionUri,
                                                Dictionary<string, string> checkpoints)
        {
            int numChangesRead = 0;
            string pkRangesResponseContinuation = null;
            List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();
            do
            {
                FeedResponse<PartitionKeyRange> pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(
                                                    collectionUri,
                                                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation });
                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);


            foreach (PartitionKeyRange pkRange in partitionKeyRanges)
            {
                string continuation = null;
                checkpoints.TryGetValue(pkRange.Id, out continuation);
                IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                    collectionUri,
                    new ChangeFeedOptions

                    {
                        PartitionKeyRangeId = pkRange.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = -1,
                        // Set reading time: only show change feed results modified since StartTime
                        StartTime = DateTime.Now - TimeSpan.FromSeconds(30)
                    });
                while (query.HasMoreResults)
                {
                    FeedResponse<dynamic> readChangesResponse = query.ExecuteNextAsync<dynamic>().Result;
                    foreach (dynamic changedDocument in readChangesResponse)
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("\tRead document {0} from the change feed.", changedDocument.id); //For Mongo it is "id" and for document it is "Id"

                        numChangesRead++;
                    }
                    Console.ForegroundColor = ConsoleColor.White;
                    checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                }
            }
    
            return checkpoints;
        }


        
        private static void LogException(Exception e)
        {

            ConsoleColor color = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Exception baseException = e.GetBaseException();

            if (e is DocumentClientException)
            {
                DocumentClientException de = (DocumentClientException)e;
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            else
            {
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }

            Console.ForegroundColor = color;

        }


        

    }

}