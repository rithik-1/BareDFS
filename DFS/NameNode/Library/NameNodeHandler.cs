namespace BareDFS.NameNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Sockets;
    using System.Text;
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
                Console.WriteLine($"[NameNode] Starting NameNode Server ...");
                server.Start();
                AcceptClients(server);

                Console.WriteLine("[NameNode] NameNode daemon started.\n");
                nameNodeInstance.ReportStatus();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NameNode] Error: {ex.Message}");
            }
        }

        private async Task AcceptClients(TcpListener listener)
        {
            try
            {
                while (true)
                {
                    var client = await listener.AcceptTcpClientAsync();
                    Console.WriteLine("[NameNode] Client connected.");
                    Task.Run(() => HandleClient(client));
                }
            }
            catch (ObjectDisposedException ex)
            {
                Console.WriteLine($"[NameNode] Listener has been stopped: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NameNode] Error accepting client: {ex.Message}\n");
                throw;
            }
        }

        private void HandleClient(TcpClient client)
        {
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[10240];    // 10KB
            int bytesRead;

            try
            {
                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[NameNode] Received: {message}");
                    var response = ExecuteOperation(JsonConvert.DeserializeObject<RpcRequest>(message));

                    Console.WriteLine($"[NameNode] Sending: {response}");
                    byte[] send = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(response));
                    stream.Write(send, 0, send.Length);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[NameNode] Error handling client: {ex.Message}");
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
                        return new Exception("[NameNode] GetData failed.");
                    }
                case "WriteData":
                    List<NameNodeMetaData> writeDataReply = new List<NameNodeMetaData>();
                    if (nameNode.PutData(JsonConvert.DeserializeObject<NameNodeWriteRequest>(data), ref writeDataReply) == true)
                    {
                        return writeDataReply;
                    }
                    else
                    {
                        return new Exception("[NameNode] WriteData failed.");
                    }
                case "GetBlockSize":
                    ulong blockSize = 0;
                    if (nameNode.GetBlockSize(data, ref blockSize) == true)
                    {
                        return blockSize;
                    }
                    else
                    {
                        return new Exception("[NameNode] GetBlockSize failed.");
                    }
                default:
                    Console.WriteLine($"[NameNode] Invalid Operation: {operation}");
                    return new NotImplementedException("[NameNode] Invalid Operation on DataNode.");
            }
        }
    }
}