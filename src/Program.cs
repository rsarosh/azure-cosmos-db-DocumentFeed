namespace DocumentDB.Samples.Queries

{

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Threading;
    using System.Threading.Tasks;

    
    public class Program

    {
        private static DocumentClient client;
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
                using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey,
                    new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
                {
                    await RunFeedProcessor(DatabaseName, CollectionName);
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

        
        private static async Task RunFeedProcessor(string databaseId, string collectionId)
        {
               
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            Console.WriteLine("Reading all changes from the beginning");
            Dictionary<string, string> checkpoints = new Dictionary<string, string>();
            //Keep polling for the changes
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
            //Starts with Null
            string pkRangesResponseContinuation = null;

            List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();
            do
            {
                //Get the paritionkeyRange
                FeedResponse<PartitionKeyRange> pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(
                                                    collectionUri,
                                                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation });
                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);

            //Read the data for every partition
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
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("\tdocument: {0} \n\r", changedDocument); 
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