namespace BareDFS.DataNode.Library
{
    using BareDFS.Common;
        using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Runtime.Serialization;
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

                Task.Run(() => AcceptClients(listener));

                Console.WriteLine($"DataNode daemon started on port: {dataNodeInstance.ServicePort}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                listener?.Stop();
            }
        }

        private async Task AcceptClients(TcpListener listener)
        {
            while (true)
            {
                var client = await listener.AcceptTcpClientAsync();
                Task.Run(() => HandleClient(client));
            }
        }

        private void HandleClient(TcpClient client)
        {
            using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream))
            using (var jsonReader = new JsonTextReader(reader))
            {
                var request = JsonSerializer.Create().Deserialize<RpcRequest>(jsonReader);
                var response = ExecuteOperation(request);
                //formatter.Serialize(stream, response);
            }

            client.Close();
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
                default:
                    Console.WriteLine($"Invalid Operation: {operation}");
                    return new NotImplementedException("Invalid Operation on DataNode.");
            }
        }
    }
}