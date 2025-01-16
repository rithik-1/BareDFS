namespace BareDFS.NameNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.IO;
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
                using (var networkStream = client.GetStream())
                {
                    var response = new object();
                    var reader = new StreamReader(networkStream);
                    var timeout = Task.Delay(5000);
                    var readTask = reader.ReadLineAsync();
                    if (Task.WhenAny(timeout, readTask).Result == readTask)
                    {
                        Console.WriteLine($"NameNode Received: {readTask.Result}");
                        response = ExecuteOperation(JsonConvert.DeserializeObject<RpcRequest>(readTask.Result));
                    }
                    else
                    {
                        throw new TimeoutException($"The operation read from stream has timed out.");
                    }

                    var writer = new StreamWriter(networkStream);
                    var jsonRequest = JsonConvert.SerializeObject(response);
                    writer.WriteLine(jsonRequest);
                    writer.Flush();
                    Console.WriteLine($"Sent from NameNode: {response}");
                    Thread.Sleep(10000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling client: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("NameNode Closing Connection.");
                client.Close();
            }
        }

        private object ExecuteOperation(RpcRequest request)
        {
            string operation = request.Operation;
            var data = request.Data.ToString();

            switch (operation)
            {
                case "GetData":
                    List<NameNodeMetaData> getDataReply = new List<NameNodeMetaData>();
                    if (nameNode.GetData(JsonConvert.DeserializeObject<NameNodeReadRequest>(data), ref getDataReply) == true)
                    {
                        return getDataReply;
                    }
                    else
                    {
                        return new Exception("GetData failed.");
                    }
                case "WriteData":
                    List<NameNodeMetaData> writeDataReply = new List<NameNodeMetaData>();
                    if (nameNode.PutData(JsonConvert.DeserializeObject<NameNodeWriteRequest>(data), ref writeDataReply) == true)
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