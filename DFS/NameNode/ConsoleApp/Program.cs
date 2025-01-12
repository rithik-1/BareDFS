namespace DFS.NameNode.Client
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;

    public class NameNode
    {
        public static List<string> RemoveElementFromList(List<string> elements, int index)
        {
            elements.RemoveAt(index);
            return elements;
        }

        public static void DiscoverDataNodes(NameNodeService nameNodeInstance, List<string> listOfDataNodes)
        {
            nameNodeInstance.IdToDataNodes = new Dictionary<ulong, DataNodeInstance>();

            int availableNumberOfDataNodes = listOfDataNodes.Count;
            if (availableNumberOfDataNodes == 0)
            {
                Console.WriteLine("No DataNodes specified, discovering ...");

                string host = "localhost";
                int serverPort = 7000;

                NameNodePingRequest pingRequest = new NameNodePingRequest { Host = host, Port = nameNodeInstance.Port };
                NameNodePingResponse pingResponse;

                while (serverPort < 7050)
                {
                    try
                    {
                        using (TcpClient client = new TcpClient(host, serverPort))
                        {
                            NetworkStream stream = client.GetStream();
                            IFormatter formatter = new BinaryFormatter();
                            formatter.Serialize(stream, pingRequest);

                            pingResponse = (NameNodePingResponse)formatter.Deserialize(stream);
                            // Process pingResponse as needed
                        }
                    }
                    catch (SocketException)
                    {
                        // Handle connection failure
                    }

                    serverPort++;
                }
            }
        }
    }
}