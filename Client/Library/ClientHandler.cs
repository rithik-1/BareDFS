namespace BareDFS.Client.Library
{
    using System;
    using System.Net.Sockets;

    public class ClientHandler
    {
        public ClientHandler()
        {
        }

        public bool PutHandler(string nameNodeAddress, string sourcePath, string fileName)
        {
            TcpClient tcpClient = InitializeClientUtil(nameNodeAddress);
            try
            {
                return Client.Put(tcpClient, sourcePath, fileName);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n" + e.Message + "\n" + e.StackTrace + "\n\n");
                return false;
            }
            finally
            {
                Console.WriteLine("[Client] Closing connection");
                tcpClient.Close();
                tcpClient.Dispose();
            }
        }

        public (string, bool) GetHandler(string nameNodeAddress, string fileName)
        {
            TcpClient tcpClient = InitializeClientUtil(nameNodeAddress);
            try
            {
                return Client.Get(ref tcpClient, fileName);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n" + e.Message + "\n" + e.StackTrace + "\n\n");
                return ("", false);
            }
            finally
            {
                Console.WriteLine("[Client] Closing connection");
                tcpClient.Close();
                tcpClient.Dispose();
            }
        }

        private static TcpClient InitializeClientUtil(string nameNodeAddress)
        {
            string[] hostPort = nameNodeAddress.Split(':');
            if (hostPort.Length != 2)
            {
                throw new ArgumentException("[Client] Invalid nameNodeAddress format");
            }

            string host = hostPort[0];
            int port = int.Parse(hostPort[1]);

            Console.WriteLine($"[Client] NameNode to connect to is {nameNodeAddress}");
            return new TcpClient(host, port);
        }
    }
}