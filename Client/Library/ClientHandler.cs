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
            TcpClient rpcClient = InitializeClientUtil(nameNodeAddress);
            try
            {
                return Client.Put(ref rpcClient, sourcePath, fileName);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n" + e.Message + "\n" + e.StackTrace + "\n\n");
                return false;
            }
            finally
            {
                rpcClient.Close();
                rpcClient.Dispose();
            }
        }

        public (string, bool) GetHandler(string nameNodeAddress, string fileName)
        {
            TcpClient rpcClient = InitializeClientUtil(nameNodeAddress);
            try
            {
                return Client.Get(ref rpcClient, fileName);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n" + e.Message + "\n" + e.StackTrace + "\n\n");
                return ("", false);
            }
            finally
            {
                rpcClient.Close();
                rpcClient.Dispose();
            }
        }

        private static TcpClient InitializeClientUtil(string nameNodeAddress)
        {
            string[] hostPort = nameNodeAddress.Split(':');
            if (hostPort.Length != 2)
            {
                throw new ArgumentException("Invalid nameNodeAddress format");
            }

            string host = hostPort[0];
            int port = int.Parse(hostPort[1]);

            Console.WriteLine($"NameNode to connect to is {nameNodeAddress}");
            return new TcpClient(host, port);
        }
    }
}