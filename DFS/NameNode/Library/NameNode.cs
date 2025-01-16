namespace BareDFS.NameNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    [Serializable]
    public class ReDistributeDataRequest
    {
        public string DataNodeUri { get; set; }
    }

    [Serializable]
    public class NameNode
    {
        private NameNodeInstance nameNodeInstance { get; set; }

        public NameNode(NameNodeInstance nameNodeInstance)
        {
            this.nameNodeInstance = nameNodeInstance;
        }

        public bool GetBlockSize(object request, ref ulong reply)
        {
            if (request != null)
            {
                reply = nameNodeInstance.BlockSize;
                return true;
            }
            return false;
        }

        public bool GetData(NameNodeReadRequest request, ref List<NameNodeMetaData> reply)
        {
            var fileBlocks = nameNodeInstance.FileNameToBlocks[request.FileName];
            foreach (var block in fileBlocks)
            {
                var blockAddresses = new List<NodeAddress>();
                var targetDataNodeIds = nameNodeInstance.BlockToDataNodeIds[block];
                foreach (var dataNodeId in targetDataNodeIds)
                {
                    blockAddresses.Add(nameNodeInstance.IdToDataNodes[dataNodeId]);
                }
                reply.Add(new NameNodeMetaData { BlockId = block, BlockAddresses = blockAddresses });
            }
            return true;
        }

        public bool PutData(NameNodeWriteRequest request, ref List<NameNodeMetaData> reply)
        {
            nameNodeInstance.FileNameToBlocks[request.FileName] = new List<string>();
            var numberOfBlocksToAllocate = (ulong)Math.Ceiling((double)request.FileSize / nameNodeInstance.BlockSize);
            reply = AllocateBlocks(request.FileName, numberOfBlocksToAllocate);
            Console.WriteLine($"WriteData: {numberOfBlocksToAllocate} blocks allocated for file name {request.FileName}.");
            return true;
        }

        public void BootStrap()
        {
            DiscoverDataNodes();
            Task.Run(() => HeartbeatToDataNodes());
        }

        private List<NameNodeMetaData> AllocateBlocks(string fileName, ulong numberOfBlocks)
        {
            var metadata = new List<NameNodeMetaData>();
            var dataNodesAvailableCount = (ulong)nameNodeInstance.IdToDataNodes.Count;

            for (ulong i = 0; i < numberOfBlocks; i++)
            {
                // Generate a new block id and assign it to the file
                var blockId = Guid.NewGuid().ToString();
                nameNodeInstance.FileNameToBlocks[fileName].Add(blockId);

                var blockAddresses = new List<NodeAddress>();

                // Replicate as many blocks as the available datanodes or replication factior, whichever is smaller
                var replicationFactor = nameNodeInstance.ReplicationFactor > dataNodesAvailableCount ? dataNodesAvailableCount : nameNodeInstance.ReplicationFactor;

                // Get the datanode ids to which the block is to be replicated
                nameNodeInstance.BlockToDataNodeIds[blockId] = NameNodeUtil.AssignDataNodes(
                    nameNodeInstance.IdToDataNodes.Keys.ToList(),
                    replicationFactor);
                var targetDataNodeIds = nameNodeInstance.BlockToDataNodeIds[blockId];

                foreach (var dataNodeId in targetDataNodeIds)
                {
                    blockAddresses.Add(nameNodeInstance.IdToDataNodes[dataNodeId]);
                }

                metadata.Add(new NameNodeMetaData { BlockId = blockId, BlockAddresses = blockAddresses });
            }

            return metadata;
        }

        private void HeartbeatToDataNodes()
        {
            var heartbeatRequest = new NodeAddress { Host = "localhost", ServicePort = nameNodeInstance.Port };
            while (true)
            {
                Thread.Sleep(10000);
                foreach (var dataNode in nameNodeInstance.IdToDataNodes)
                {
                    var nodeAddres = dataNode.Value;
                    try
                    {
                        using (var client = new TcpClient())
                        {
                            client.Connect(nodeAddres.Host, nodeAddres.ServicePort);
                            var response = false;
                            CallService(client, Services.Heartbeat.ToString(), heartbeatRequest, ref response);
                            if (!response)
                                Console.WriteLine($"No heartbeat response from DataNode {nodeAddres.Host}:{nodeAddres.ServicePort}");
                        }
                    }
                    catch
                    {
                        Console.WriteLine($"No heartbeat received from DataNode {nodeAddres.Host}:{nodeAddres.ServicePort}");
                        bool reply = false;
                        ReDistributeData(nodeAddres, ref reply);
                        nameNodeInstance.IdToDataNodes.Remove(dataNode.Key);
                    }
                }
            }
        }

        private void DiscoverDataNodes()
        {
            var listOfDataNodes = nameNodeInstance.IdToDataNodes;
            string host = "localhost";
            var pingRequest = new NodeAddress { Host = host, ServicePort = nameNodeInstance.Port };

            if (listOfDataNodes.Count == 0)
            {
                Console.WriteLine("No DataNodes specified, discovering ...");
                int serverPort = 7000;

                while (serverPort < 7050)
                {
                    string dataNodeUri = $"{host}:{serverPort}";
                    try
                    {
                        using (var client = new TcpClient())
                        {
                            client.Connect(host, serverPort);
                            Console.WriteLine($"Pinging DataNode {dataNodeUri}");
                            var response = false;
                            CallService(client, Services.Ping.ToString(), pingRequest, ref response);

                            if (response == true)
                            {
                                Console.WriteLine($"Ack received from {dataNodeUri}");
                                listOfDataNodes.Add(Guid.NewGuid(), new NodeAddress { Host = host, ServicePort = (ushort)serverPort });
                            }
                            else
                                Console.WriteLine($"No ack received from {dataNodeUri}");
                        }
                    }
                    catch (Exception ex)
                    {
                        // Continue to next port
                    }
                    serverPort++;
                }
            }
            else
            {
                Console.WriteLine($"Pinging DataNode(s) specified in the input ...");
                foreach (var dataNode in listOfDataNodes)
                {
                    string dataNodeUri = $"{dataNode.Value.Host}:{dataNode.Value.ServicePort}";
                    try
                    {
                        using (var client = new TcpClient())
                        {
                            client.Connect(dataNode.Value.Host, dataNode.Value.ServicePort);
                            Console.WriteLine($"Pinging DataNode {dataNodeUri}");
                            bool response = false;
                            CallService(client, Services.Ping.ToString(), pingRequest, ref response);

                            if (response == true)
                                Console.WriteLine($"Ack received from {dataNodeUri}");
                            else
                            {
                                Console.WriteLine($"No ack received from {dataNodeUri}");
                                listOfDataNodes.Remove(dataNode.Key);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Exception in receiving ack from {dataNodeUri}");
                        Console.WriteLine($"Exception: {ex.Message}");
                        listOfDataNodes.Remove(dataNode.Key);
                    }
                    Console.WriteLine($"DataNode ID: {dataNode.Key} at {dataNode.Value.Host}:{dataNode.Value.ServicePort}\n");
                }
            }

            nameNodeInstance.IdToDataNodes = listOfDataNodes;
        }

        public bool ReDistributeData(NodeAddress deadDataNode, ref bool reply)
        {
            Console.WriteLine($"DataNode {deadDataNode.Host}:{deadDataNode.ServicePort} is dead, trying to redistribute data");
            Guid deadDataNodeId;

            // Get the dead datanode id
            foreach (var dataNode in nameNodeInstance.IdToDataNodes)
            {
                var dnAddress = dataNode.Value;
                if (dnAddress.Host == deadDataNode.Host && dnAddress.ServicePort == deadDataNode.ServicePort)
                {
                    deadDataNodeId = dataNode.Key;
                    break;
                }
            }

            nameNodeInstance.IdToDataNodes.Remove(deadDataNodeId);

            // Get the block ids which were assigned to the dead datanode
            var underReplicatedBlocksList = new List<UnderReplicatedBlocks>();
            foreach (var blockId in nameNodeInstance.BlockToDataNodeIds.Keys.ToList())
            {
                var dnIds = nameNodeInstance.BlockToDataNodeIds[blockId];
                for (int i = 0; i < dnIds.Count; i++)
                {
                    if (dnIds[i] == deadDataNodeId)
                    {
                        var healthyDataNodeId = nameNodeInstance.BlockToDataNodeIds[blockId][(i + 1) % dnIds.Count];
                        underReplicatedBlocksList.Add(new UnderReplicatedBlocks { BlockId = blockId, HealthyDataNodeId = healthyDataNodeId });
                        nameNodeInstance.BlockToDataNodeIds[blockId].Remove(deadDataNodeId);
                        break;
                    }
                }
            }

            if (nameNodeInstance.IdToDataNodes.Count < (int)nameNodeInstance.ReplicationFactor)
            {
                Console.WriteLine("Replication not possible due to unavailability of sufficient DataNode(s)");
                return false;
            }

            // Replicate the under-replicated blocks to the available DataNodes
            var availableNodes = nameNodeInstance.IdToDataNodes.Keys.ToList();

            foreach (var blockToReplicate in underReplicatedBlocksList)
            {
                var blockId = blockToReplicate.HealthyDataNodeId;
                var healthyDataNodeAddress = nameNodeInstance.IdToDataNodes[blockToReplicate.HealthyDataNodeId];
                using (var dataNodeInstance = new TcpClient(healthyDataNodeAddress.Host, healthyDataNodeAddress.ServicePort))
                {
                    // Get the block contents from the healthy DataNode
                    var getRequest = new DataNodeReadRequest { BlockId = blockToReplicate.BlockId };
                    var getReply = new DataNodeData();
                    CallService(dataNodeInstance, Services.GetBlock.ToString(), getRequest, ref getReply);
                    var blockContents = getReply.Data;

                    ulong replication = nameNodeInstance.ReplicationFactor - (ulong)nameNodeInstance.BlockToDataNodeIds[blockToReplicate.BlockId].Count;
                    var targetDataNodeIds = NameNodeUtil.AssignDataNodes(
                        availableNodes,
                        replication,
                        new List<Guid> { blockToReplicate.HealthyDataNodeId });
                    nameNodeInstance.BlockToDataNodeIds[blockToReplicate.BlockId] = targetDataNodeIds;
                    var startingDataNode = nameNodeInstance.IdToDataNodes[targetDataNodeIds[0]];
                    var remainingDataNodes = new List<NodeAddress>();

                    foreach (var id in targetDataNodeIds.Skip(1))
                    {
                        remainingDataNodes.Add(nameNodeInstance.IdToDataNodes[id]);
                    }

                    // Replicate the block to the target DataNodes
                    using (var targetDataNodeInstance = new TcpClient(startingDataNode.Host, startingDataNode.ServicePort))
                    {
                        var putRequest = new DataNodeWriteRequest
                        {
                            BlockId = blockToReplicate.BlockId,
                            Data = blockContents,
                            ReplicationNodes = remainingDataNodes
                        };

                        var putReply = "";
                        CallService(targetDataNodeInstance, Services.PutBlock.ToString(), putRequest, ref putReply);
                    }

                    Console.WriteLine($"Block {blockToReplicate.BlockId} replication completed for {string.Join(", ", targetDataNodeIds)}");
                }
            }

            return true;
        }

        public static void CallService<TRequest, TReply>(TcpClient client, string serviceMethod, TRequest request, ref TReply reply)
        {
            var jsonRequest = JsonConvert.SerializeObject(new RpcRequest(serviceMethod, request));
            Console.WriteLine($"NameNode sending: {jsonRequest}");
            byte[] bytesToSend = Encoding.UTF8.GetBytes(jsonRequest);
            client.Client.Send(bytesToSend);

            byte[] buffer = new byte[10240];    // 10KB
            int bytesRead = client.GetStream().Read(buffer, 0, buffer.Length);
            if (bytesRead > 0)
            {
                reply = JsonConvert.DeserializeObject<TReply>(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                Console.WriteLine("NameNode Received: " + reply);
            }
        }
    }
}