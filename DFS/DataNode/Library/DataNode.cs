namespace BareDFS.DataNode.Library
{
    using BareDFS.Common;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.IO;
    using System.Net.Sockets;
    using System.Text;

    [Serializable]
    public static class DataNode
    {
        public static bool Ping(object data)
        {
            if (data != null)
            {
                Console.WriteLine($"Received ping from NameNode. NameNode: {data}");
                return true;
            }

            return false;
        }

        public static bool Heartbeat(object data)
        {
            if (data != null)
            {
                Console.WriteLine($"Received heartbeat from NameNode.");
                return true;
            }
            return false;
        }

        private static bool ForwardForReplication(DataNodeWriteRequest request, ref DataNodeWriteResponse reply)
        {
            var blockId = request.BlockId;
            var blockAddresses = request.ReplicationNodes;

            if (blockAddresses.Count == 0)
            {
                return true;
            }

            var startingDataNode = blockAddresses[0];
            var remainingDataNodes = new List<NodeAddress>();
            if (blockAddresses.Count > 1)
                remainingDataNodes = blockAddresses.Skip(1).ToList();

            using (var client = new TcpClient(startingDataNode.Host, int.Parse(startingDataNode.ServicePort.ToString())))
            {
                var stream = client.GetStream();
                //var formatter = new BinaryFormatter();
                var payloadRequest = new DataNodeWriteRequest
                {
                    BlockId = blockId,
                    Data = request.Data,
                    ReplicationNodes = remainingDataNodes
                };

                //formatter.Serialize(stream, "Service.PutData");
                //formatter.Serialize(stream, payloadRequest);
                //reply = (DataNodeWriteStatus)formatter.Deserialize(stream);
            }

            return true;
        }

        public static DataNodeWriteResponse PutData(string dataDirectory, DataNodeWriteRequest request)
        {
            var filePath = Path.Combine(dataDirectory, request.BlockId.ToString());
            try
            {
                File.WriteAllText(filePath, request.Data.ToString(), Encoding.UTF8);
                var reply = new DataNodeWriteResponse { Status = true };
                //var status = ForwardForReplication(request, ref reply);
                return reply;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to write data to disk");
                Console.WriteLine($"Error: {e.Message}\n");
                return new DataNodeWriteResponse { Status = false };
            }
        }

        public static DataNodeReadResponse GetData(string dataDirectory, DataNodeReadRequest request)
        {
            var filePath = Path.Combine(dataDirectory, request.BlockId.ToString());
            try
            {
                var data = File.ReadAllText(filePath, Encoding.UTF8);
                return new DataNodeReadResponse { Status = true, Data = data };
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to read data from disk");
                Console.WriteLine($"Error: {e.Message}\n");
                return new DataNodeReadResponse { Status = false, Data = "", Error = e.Message };
            }
        }
    }
}