/*
 * Copyright (c) 2023 Sony Semiconductor Solutions Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * -----------------
 *
 * Copyright (c) 2022 Daisuke Nakahara
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
*/

using System.Linq;
using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using Azure.Data.Tables;
using System.Runtime.Caching;
using static PeopleCountingFunctions.Models;

namespace PeopleCountingFunctions
{
    public static class Peoplecounting_processor
    {
        private const string Signalr_Hub = "telemetryHub";
        private const string CosmosDbdatabaseName = "SmartCameras";
        private const string CosmosDbCollectionName = "InferenceResult";
        private const int HumanClass = 0;
        private const int FilterWidth = 5;     // the size of moving average filter [s]
        static MemoryCache memoryCacheCount = new MemoryCache("memoryCacheCount");


        [FunctionName("Peoplecounting_processor")]
        public static async Task RunAsync(
            [CosmosDBTrigger(
            databaseName: CosmosDbdatabaseName,
            collectionName: CosmosDbCollectionName,
            ConnectionStringSetting = "AzureCosmosDBConnection",
            CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> input,
            [CosmosDB(
            databaseName: CosmosDbdatabaseName,
            collectionName: CosmosDbCollectionName,
            ConnectionStringSetting = "AzureCosmosDBConnection")] DocumentClient client,
            [Table("notifications", Connection = "AzureWebJobsAzureTableConnection")] TableClient tableClientNotifications,
            [SignalR(HubName = Signalr_Hub, ConnectionStringSetting = "AzureSignalRConnectionString")] IAsyncCollector<SignalRMessage> signalRMessage,
            ILogger log)
        {

            log.LogInformation($"Triggers {input.Count()}");

            foreach (Document doc in input)
            {
                var jo = JObject.Parse(doc.ToString());

                // Initialize SignalR Data
                SIGNALR_DATA signalrData = new SIGNALR_DATA
                {
                    eventId = doc.Id,
                    eventType = "CosmosDB Trigger",
                    eventSource = "CosmosDB",
                    eventTime = doc.Timestamp.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                    hasImage = false,
                    inference = new List<INFERENCE_RESULT>(),
                    count = 0
                };

                if (jo.ContainsKey("DeviceID"))
                {
                    signalrData.deviceId = (string)jo["DeviceID"];
                }

                if (jo.ContainsKey("Image"))
                {
                    signalrData.hasImage = (bool)jo["Image"];
                }

                if (jo.ContainsKey("Inferences"))
                {

                    var inferenceResult = new INFERENCE_RESULT()
                    {
                        inferenceResults = new List<INFERENCE_ITEM>(),
                        T = null
                    };
                    var inferences = (JObject)jo["Inferences"];
                    foreach (var item in inferences)
                    {
                        if (item.Key == "T")
                        {
                            inferenceResult.T = item.Value.ToString();
                        }
                        else
                        {
                            foreach (JObject inference in item.Value)
                            {
                                var inferenceItem = new INFERENCE_ITEM();

                                inferenceItem.C = (uint)inference["C"];
                                inferenceItem.P = (double)inference["P"];
                                inferenceItem.Left = (int)inference["Left"];
                                inferenceItem.Right = (int)inference["Right"];
                                inferenceItem.Top = (int)inference["Top"];
                                inferenceItem.Bottom = (int)inference["Bottom"];

                                inferenceResult.inferenceResults.Add(inferenceItem);
                            }
                        }
                    }
                    signalrData.inference.Add(inferenceResult);
                }

                //// Application process
                // Calc time range
                var end = ((DateTimeOffset)doc.Timestamp).ToUnixTimeSeconds();
                var start = ((DateTimeOffset)doc.Timestamp.AddSeconds(-FilterWidth)).ToUnixTimeSeconds();

                // Query
                Uri collectionUrl = UriFactory.CreateDocumentCollectionUri(CosmosDbdatabaseName, CosmosDbCollectionName);
                var option = new FeedOptions { EnableCrossPartitionQuery = true };
                var query = client.CreateDocumentQuery<COSMOSDB_DATA>(collectionUrl, option)
                .Where(p => p.DeviceID == signalrData.deviceId)
                .Where(p => p._ts >= start)
                .Where(p => p._ts <= end)
                .OrderByDescending(p => p._ts)
                .AsDocumentQuery();

                // Calc change of count
                int? count_previos = null;
                int? count_current = null;
                var l_countInference = new List<int>();
                foreach (var result in await query.ExecuteNextAsync())
                {
                    var countInference = 0;
                    var res = JObject.Parse(result.ToString());

                    if (res.ContainsKey("Inferences"))
                    {
                        var inferences = (JObject)res["Inferences"];
                        foreach (var item in inferences)
                        {
                            if (item.Key == "T")
                                continue;
                            else
                            {
                                foreach (JObject inference in item.Value)
                                {
                                    if ((uint)inference["C"] == HumanClass)
                                        countInference++;
                                }
                            }
                        }
                    }
                    l_countInference.Add(countInference);
                }

                // calc count
                count_current = l_countInference
                .GroupBy(x => x)
                .OrderByDescending(x => x.Count())
                .ThenBy(x => x.Key)
                .Select(x => (int?)x.Key)
                .FirstOrDefault();

                if (memoryCacheCount.GetCount() == 0)
                {
                    memoryCacheCount.Set("previos", count_current, new CacheItemPolicy()
                    {
                        AbsoluteExpiration = DateTimeOffset.Now.AddSeconds(300),
                    });
                    continue;
                }
                else
                {
                    count_previos = (int)memoryCacheCount["previos"];
                    memoryCacheCount.Set("previos", count_current, new CacheItemPolicy()
                    {
                        AbsoluteExpiration = DateTimeOffset.Now.AddSeconds(300),
                    });
                }

                // Send data to SignalR Hub
                signalrData.count = count_current;
                var serializedSignalrdata = JsonConvert.SerializeObject(signalrData);
                await signalRMessage.AddAsync(new SignalRMessage
                {
                    Target = "PeopleCounting",
                    Arguments = new[] { serializedSignalrdata }
                });

                // Do notification
                var diff = count_current - count_previos;
                if (diff == 0)
                    continue;
                log.LogInformation($"Count was changed from {count_previos} to {count_current}");

                // Initialize SignalR Data
                SIGNALR_DATA signalrData_notification = new SIGNALR_DATA
                {
                    eventId = doc.Id,
                    eventType = "CosmosDB Trigger",
                    eventSource = "CosmosDB",
                    eventTime = doc.Timestamp.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                    hasImage = false,
                    data = ""
                };

                var queryResults = tableClientNotifications.QueryAsync<NotificationEntity>(x => x.PartitionKey == signalrData.deviceId);
                await foreach (var entity in queryResults)
                {
                    var condition = diff > 0 ? "or more" : "or less";
                    if (entity.Condition != condition)
                        continue;

                    if (((count_previos < entity.Threshold) && (entity.Threshold <= count_current)) || ((count_current <= entity.Threshold) && (entity.Threshold < count_previos)))
                    {
                        log.LogInformation(entity.Title);

                        // Call notification app apis here


                        // Send data to SignalR Hub
                        string notification = JsonConvert.SerializeObject(entity);
                        log.LogInformation(notification);
                        signalrData_notification.data = notification;
                        var serializedSignalrdata_notification = JsonConvert.SerializeObject(signalrData_notification);
                        await signalRMessage.AddAsync(new SignalRMessage
                        {
                            Target = "Notification",
                            Arguments = new[] { serializedSignalrdata_notification }
                        });
                    }
                }
                break;
            }
        }
    }
}
