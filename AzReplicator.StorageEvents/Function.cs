using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.EventGrid;
using Microsoft.Rest;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AzReplicator.AWS.StorageEvents
{
    public class Function
    {
        #region Data Members

        private IAmazonS3 S3Client { get; set; }
        private readonly string EventGridTopicKey = System.Environment.GetEnvironmentVariable("EventGridTopicKey");
        private readonly string EventGridTopicTopicHostname = System.Environment.GetEnvironmentVariable("EventGridTopicHostName");

        #endregion

        #region Constructors

        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            context.Logger.LogLine("Geppetto.StorageEvents.Function Invoked");

            var s3Event = evnt.Records?[0].S3;
            if(s3Event == null)
            {
                return null;
            }

            try
            {
                var gridEvents = GetEvents(evnt);
                if (gridEvents != null && gridEvents.Count > 0)
                {
                    await PublishEvents(gridEvents);
                }

                return "success";
            }
            catch(Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        #endregion

        #region Private Methods

        private IList<EventGridEvent> GetEvents(S3Event evnt)
        {
            var events = new List<EventGridEvent>();

            foreach (var eventNotification in evnt.Records)
            {
                var s3Event = eventNotification.S3;
                var file = new AzReplicator.AWS.Models.FileEvent()
                {
                    Name = s3Event.Object.Key,
                    Url = GeneratePreSignedUrl(s3Event.Bucket.Name, s3Event.Object.Key, this.S3Client)
                };

                events.Add(new EventGridEvent()
                {
                    Id = Guid.NewGuid().ToString(),
                    Data = file,
                    EventTime = DateTime.UtcNow,
                    EventType = "Geppetto.NewFile",
                    Subject = s3Event.Object.Key,
                    DataVersion = "1.0"
                });
            }

            return events;
        }

        private static string GeneratePreSignedUrl(string bucketName, string objectKey, IAmazonS3 s3Client)
        {
            var urlString = "";
            try
            {
                var request1 = new GetPreSignedUrlRequest
                {
                    BucketName = bucketName,
                    Key = objectKey,
                    Expires = DateTime.Now.AddMinutes(60)
                };
                urlString = s3Client.GetPreSignedURL(request1);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            return urlString;
        }

        private async Task PublishEvents(IList<EventGridEvent> events)
        {
            ServiceClientCredentials credentials = new TopicCredentials(this.EventGridTopicKey);
            var client = new EventGridClient(credentials);
            await client.PublishEventsAsync(this.EventGridTopicTopicHostname, events);
        }

        #endregion
    }
}
