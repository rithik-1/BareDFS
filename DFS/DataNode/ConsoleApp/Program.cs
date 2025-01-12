namespace DFS.DataNode.Client
{
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;

    public class DataNode
    {
        public static void InitializeDataNodeUtil(string dataNodeAddress, string storagePath)
        {
            string[] hostPort = dataNodeAddress.Split(':');
            if (hostPort.Length != 2)
            {
                throw new ArgumentException("Invalid dataNodeAddress format");
            }

            string host = hostPort[0];
            int port = int.Parse(hostPort[1]);

            Console.WriteLine($"DataNode to connect to is {dataNodeAddress}");

            TcpListener listener = new TcpListener(IPAddress.Parse(host), port);
            listener.Start();

            while (true)
            {
                TcpClient client = listener.AcceptTcpClient();
                NetworkStream stream = client.GetStream();

                IFormatter formatter = new BinaryFormatter();
                DataNodeGetRequest request = (DataNodeGetRequest)formatter.Deserialize(stream);

                string filePath = Path.Combine(storagePath, request.BlockId);
                string data = File.ReadAllText(filePath);

                DataNodeData reply = new DataNodeData { Data = data };
                formatter.Serialize(stream, reply);

                client.Close();
            }
        }
    }
}