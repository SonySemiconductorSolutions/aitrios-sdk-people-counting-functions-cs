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
 * copies or substantial portions of the  * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using static PeopleCountingFunctionA.Models;

using Google.FlatBuffers;
using SmartCamera;

namespace PeopleCountingFunctionA
{
    public class Telemetry_Processor
    {
        private static ILogger _logger = null;

        [FunctionName("Telemetry_Processor")]
        public async Task Run([EventHubTrigger("sonysmartcamera", Connection = "AzureEventhubRootManageSharedAccessKey")] EventData[] eventData,
        [CosmosDB(
        databaseName: "SmartCameras",
        collectionName: "InferenceResult",
        ConnectionStringSetting = "AzureCosmosDBConnectionString")] IAsyncCollector<dynamic> InferenceItem,
        ILogger logger)
        {
            var exceptions = new List<Exception>();
            _logger = logger;

            foreach (EventData ed in eventData)
            {
                try
                {
                    // only today
                    DateTime now = DateTime.Now;
                    if (now.Date > ed.EnqueuedTime.Date)
                        continue;

                    // deserialize data
                    Dictionary<string, object> jsonEventBody = JsonConvert.DeserializeObject<Dictionary<string, object>>(ed.EventBody.ToString());
                    if (jsonEventBody.ContainsKey("backdoor-EA_Main/placeholder"))
                    {
                        EVENTBODY telemetryMessage = JsonConvert.DeserializeObject<EVENTBODY>(jsonEventBody["backdoor-EA_Main/placeholder"].ToString());
                        string deviceId = telemetryMessage.DeviceID;
                        string modelId = telemetryMessage.ModelID;
                        _logger.LogInformation($"Telemetry Message : {ed.EventBody.ToString()}");

                        // Initialize COSMOSDB Data
                        var cosmosdbData = new COSMOSDB_DATA
                        {
                            DeviceID = deviceId,
                            ModelID = modelId,
                            Image = telemetryMessage.Image,
                            Inferences = new INFERENCE_RESULT(),
                            project_id = telemetryMessage.project_id,
                        };

                        List<INFERENCE_ITEM> deserializedItems = Deserialize(telemetryMessage.Inferences[0].O);
                        INFERENCE_RESULT inferenceResult = new INFERENCE_RESULT
                        {
                            T = telemetryMessage.Inferences[0].T,
                            inferenceResults = deserializedItems
                        };
                        cosmosdbData.Inferences = inferenceResult;

                        if (cosmosdbData.Inferences != null)
                        {
                            var data = JsonConvert.SerializeObject(cosmosdbData);
                            _logger.LogInformation($"Deserialized Message: {data}");

                            // store in cosmosdb
                            await InferenceItem.AddAsync(data);
                        }
                        cosmosdbData = null;
                    }
                    else if (jsonEventBody.ContainsKey("backdoor-EA_Main/Heartbeat"))
                    {
                        continue;
                    }
                    else
                    {
                        _logger.LogInformation("Unsupported Message Source");
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private static List<INFERENCE_ITEM> Deserialize(string inferenceData)
        {
            byte[] buf = Convert.FromBase64String(inferenceData);
            ObjectDetectionTop objectDetectionTop = ObjectDetectionTop.GetRootAsObjectDetectionTop(new ByteBuffer(buf));
            ObjectDetectionData objectData = objectDetectionTop.Perception ?? new ObjectDetectionData();
            int resNum = objectData.ObjectDetectionListLength;
            _logger.LogInformation($"NumOfDetections: {resNum.ToString()}");

            List<INFERENCE_ITEM> inferenceResults = new List<INFERENCE_ITEM>();
            for (int i = 0; i < resNum; i++)
            {
                GeneralObject objectList = objectData.ObjectDetectionList(i) ?? new GeneralObject();
                BoundingBox unionType = objectList.BoundingBoxType;
                if (unionType == BoundingBox.BoundingBox2d)
                {
                    var bbox2d = objectList.BoundingBox<BoundingBox2d>().Value;
                    INFERENCE_ITEM data = new INFERENCE_ITEM();
                    data.C = objectList.ClassId;
                    data.P = objectList.Score;
                    data.Left = bbox2d.Left;
                    data.Top = bbox2d.Top;
                    data.Right = bbox2d.Right;
                    data.Bottom = bbox2d.Bottom;

                    inferenceResults.Add(data);
                }
            }
            return inferenceResults;
        }
    }
}