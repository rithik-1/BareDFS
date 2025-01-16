namespace BareDFS.DataNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading.Tasks;

    [Serializable]
    public class DataNodeHandler
    {
        private DataNodeInstance dataNodeInstance { get; set; }

        public DataNodeHandler(string dataDirectory, ushort servicePort)
        {
            dataNodeInstance = new DataNodeInstance(dataDirectory, servicePort);
        }

        public void StartDataNodeServer()
        {
            Console.WriteLine($"Data storage location is {dataNodeInstance.DataDirectory}");

            TcpListener listener = new TcpListener(IPAddress.Any, dataNodeInstance.ServicePort);

            try
            {
                listener.Start();
                Console.WriteLine($"DataNode port is {dataNodeInstance.ServicePort}");
                AcceptClients(listener);

                Console.WriteLine($"DataNode daemon started on port: {dataNodeInstance.ServicePort}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
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
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[10240];    // 10KB
            int bytesRead;

            try
            {
                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"DataNode Received: {message}");
                    var response = ExecuteOperation(JsonConvert.DeserializeObject<RpcRequest>(message));

                    Console.WriteLine($"DataNode Sending: {response}");
                    byte[] send = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(response));
                    stream.Write(send, 0, send.Length);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DataNode] Error handling client: {ex.Message}");
            }
        }

        private object ExecuteOperation(RpcRequest request)
        {
            string operation = request.Operation;
            var data = request.Data.ToString();

            switch (operation)
            {
                case "GetBlock":
                    return DataNode.GetData(dataNodeInstance.DataDirectory, JsonConvert.DeserializeObject<DataNodeReadRequest>(data));
                    break;
                case "PutBlock":
                    return DataNode.PutData(dataNodeInstance.DataDirectory, JsonConvert.DeserializeObject<DataNodeWriteRequest>(data));
                    break;
                case "Ping":
                    return DataNode.Ping(data);
                    break;
                case "Heartbeat":
                    return DataNode.Heartbeat(data);
                    break;
                default:
                    Console.WriteLine($"Invalid Operation: {operation}");
                    return new NotImplementedException("Invalid Operation on DataNode.");
            }
        }
    }
}