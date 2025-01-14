namespace BareDFS.NameNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    public class NameNodeHandler
    {
        private NameNodeInstance nameNodeInstance { get; set; }
        private NameNode nameNode { get; set; }

        public NameNodeHandler(ushort serverPort, ulong blockSize, ulong replicationFactor, List<string> listOfDataNodes)
        {
            nameNodeInstance = new NameNodeInstance(
                serverPort,
                blockSize,
                replicationFactor,
                listOfDataNodes);

            nameNode = new NameNode(nameNodeInstance);
        }

        public void StartNameNodeServer()
        {
            nameNode.BootStrap();

            var server = new TcpListener(IPAddress.Any, nameNodeInstance.Port);

            try
            {
                Console.WriteLine($"Starting NameNode Server ...");
                server.Start();
                AcceptClients(server);

                Console.WriteLine("NameNode daemon started.\n");
                nameNodeInstance.ReportStatus();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private async Task AcceptClients(TcpListener listener)
        {
            try
            {
                while (true)
                {
                    var client = await listener.AcceptTcpClientAsync();
                    Console.WriteLine("Client connected.");
                    Task.Run(() => HandleClient(client));
                }
            }
            catch (ObjectDisposedException ex)
            {
                Console.WriteLine($"Listener has been stopped: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error accepting client: {ex.Message}\n");
                throw;
            }
        }

        private void HandleClient(TcpClient client)
        {
            try
            {
                using (var stream = client.GetStream())
                using (var reader = new System.IO.StreamReader(stream))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    var request = JsonSerializer.Create().Deserialize<RpcRequest>(jsonReader);
                    var response = ExecuteOperation(request);
                    using (var writer = new System.IO.StreamWriter(stream))
                    using (var jsonWriter = new JsonTextWriter(writer))
                    {
                        JsonSerializer.Create().Serialize(jsonWriter, response);
                    }
                    //formatter.Serialize(stream, response);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling client: {ex.Message}");
            }
            finally
            {
                client.Close();
            }
        }

        private object ExecuteOperation(RpcRequest request)
        {
            string operation = request.Operation;
            var data = request.Data;

            switch (operation)
            {
                case "GetData":
                    List<NameNodeMetaData> getDataReply = new List<NameNodeMetaData>();
                    if (nameNode.GetData((NameNodeReadRequest)data, ref getDataReply) == true)
                    {
                        return getDataReply;
                    }
                    else
                    {
                        return new Exception("GetData failed.");
                    }
                case "WriteData":
                    List<NameNodeMetaData> writeDataReply = new List<NameNodeMetaData>();
                    if (nameNode.PutData((NameNodeWriteRequest)data, ref writeDataReply) == true)
                    {
                        return writeDataReply;
                    }
                    else
                    {
                        return new Exception("WriteData failed.");
                    }
                case "GetBlockSize":
                    ulong blockSize = 0;
                    if (nameNode.GetBlockSize(data, ref blockSize) == true)
                    {
                        return blockSize;
                    }
                    else
                    {
                        return new Exception("GetBlockSize failed.");
                    }
                default:
                    Console.WriteLine($"Invalid Operation: {operation}");
                    return new NotImplementedException("Invalid Operation on DataNode.");
            }
        }
    }
}