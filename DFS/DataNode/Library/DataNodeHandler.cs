namespace BareDFS.DataNode.Library
{
    using BareDFS.Common;
    using Newtonsoft.Json;
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
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
                        response = ExecuteOperation(JsonConvert.DeserializeObject<RpcRequest>(readTask.Result));
                        Console.WriteLine($"DataNode Received: {readTask.Result}");
                    }
                    else
                    {
                        throw new TimeoutException($"The operation read from stream has timed out.");
                    }

                    var writer = new StreamWriter(networkStream);
                    var jsonRequest = JsonConvert.SerializeObject(response);
                    writer.WriteLine(jsonRequest);
                    writer.Flush();
                    Console.WriteLine($"Sent from DataNode: {response}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DataNode] Error handling client: {ex.Message}");
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
                case "GetBlock":
                    return DataNode.GetData(dataNodeInstance.DataDirectory, (DataNodeReadRequest)data);
                    break;
                case "PutBlock":
                    return DataNode.PutData(dataNodeInstance.DataDirectory, (DataNodeWriteRequest)data);
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