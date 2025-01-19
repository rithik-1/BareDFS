namespace BareDFS.DataNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.IO;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading.Tasks;

    [Serializable]
    public class DataNode
    {
        public DataNodeInstance Instance { get; set; }

        public DataNode(DataNodeInstance instance)
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            Instance = instance;
        }

        public bool Ping(object data)
        {
            if (data != null)
            {
                //Console.WriteLine($"[DataNode - {Instance.ServicePort}] Received ping from NameNode. NameNode: {data}");
                return true;
            }

            return false;
        }

        public bool Heartbeat(object data)
        {
            if (data != null)
            {
                //Console.WriteLine($"[DataNode - {Instance.ServicePort}] Received heartbeat from NameNode.");
                return true;
            }
            return false;
        }

        private bool ForwardForReplication(DataNodeWriteRequest request, DataNodeWriteResponse reply)
        {
            var blockId = request.BlockId;
            var blockAddresses = request.ReplicationNodes;

            if (blockAddresses.Count == 0)
            {
                return true;
            }

            Console.WriteLine($"[DataNode - {Instance.ServicePort}] Forwarding data for replication.");
            Console.WriteLine($"[DataNode - {Instance.ServicePort}] Replication Nodes Count: {blockAddresses.Count}");

            var startingDataNode = blockAddresses[0];
            var remainingDataNodes = new List<NodeAddress>();
            if (blockAddresses.Count > 1)
                remainingDataNodes = blockAddresses.Skip(1).ToList();

            using (var dataNodeClient = new TcpClient(startingDataNode.Host, int.Parse(startingDataNode.ServicePort.ToString())))
            {
                try
                {
                    var stream = dataNodeClient.GetStream();
                    var dataNodeWriteRequest = new DataNodeWriteRequest
                    {
                        BlockId = blockId,
                        Data = request.Data,
                        ReplicationNodes = remainingDataNodes
                    };

                    Call(dataNodeClient, Services.PutBlock.ToString(), dataNodeWriteRequest, ref reply);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[DataNode - {Instance.ServicePort}] Failed to forward data for replication.");
                    Console.WriteLine($"[DataNode - {Instance.ServicePort}] Error: {e.Message}\n");
                    reply.Status = false;
                    return false;
                }
            }

            return true;
        }

        public DataNodeWriteResponse PutData(string dataDirectory, DataNodeWriteRequest request)
        {
            var filePath = Path.Combine(dataDirectory, request.BlockId.ToString());
            try
            {
                File.WriteAllText(filePath, request.Data.ToString(), Encoding.UTF8);
                var reply = new DataNodeWriteResponse { Status = true };
                Task.Run(() => ForwardForReplication(request, reply));
                return reply;
            }
            catch (Exception e)
            {
                Console.WriteLine($"[DataNode - {Instance.ServicePort}] Failed to write data to disk");
                Console.WriteLine($"[DataNode - {Instance.ServicePort}] Error: {e.Message}\n");
                return new DataNodeWriteResponse { Status = false };
            }
        }

        public DataNodeReadResponse GetData(string dataDirectory, DataNodeReadRequest request)
        {
            var filePath = Path.Combine(dataDirectory, request.BlockId.ToString());
            try
            {
                var data = File.ReadAllText(filePath, Encoding.UTF8);
                return new DataNodeReadResponse { Status = true, Data = data };
            }
            catch (Exception e)
            {
                Console.WriteLine($"[DataNode - {Instance.ServicePort}] Failed to read data from disk");
                Console.WriteLine($"[DataNode - {Instance.ServicePort}] Error: {e.Message}\n");
                return new DataNodeReadResponse { Status = false, Data = "", Error = e.Message };
            }
        }

        private void Call<TRequest, TResponse>(TcpClient client, string serviceMethod, TRequest request, ref TResponse reply)
        {
            var jsonRequest = JsonConvert.SerializeObject(new RpcRequest(serviceMethod, request));
            Console.WriteLine($"[DataNode - {Instance.ServicePort}] Sending: {jsonRequest}");
            byte[] bytesToSend = Encoding.UTF8.GetBytes(jsonRequest);
            client.Client.Send(bytesToSend);

            byte[] buffer = new byte[10240];   // 10KB
            int bytesRead = client.GetStream().Read(buffer, 0, buffer.Length);
            if (bytesRead > 0)
            {
                reply = JsonConvert.DeserializeObject<TResponse>(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                Console.WriteLine($"DataNode - {Instance.ServicePort} Client Received: {reply}");
            }
        }
    }
}