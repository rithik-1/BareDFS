namespace BareDFS.Client.Library
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.IO;
    using System.Net.Sockets;
    using BareDFS.Common;
    using System.Text;

    public static class Client
    {
        public static bool Put(TcpClient nameNodeClient, string sourcePath, string fileName)
        {
            return _Put(nameNodeClient, sourcePath, fileName);
        }

        private static bool _Put(TcpClient nameNodeClient, string sourcePath, string fileName)
        {
            // Get the file info
            string fullFilePath = Path.Combine(sourcePath, fileName);
            FileInfo fileInfo = new FileInfo(fullFilePath);

            if (!fileInfo.Exists)
            {
                throw new FileNotFoundException("File not found.", fullFilePath);
            }

            ulong fileSize = (ulong)fileInfo.Length;
            var request = new NameNodeWriteRequest { FileName = fileName, FileSize = fileSize };
            var reply = new List<NameNodeMetaData>();

            // Call the NameNode to get the block addresses for the file to write to and the block size
            Call(nameNodeClient, Services.WriteData.ToString(), request, ref reply);
            ulong blockSize = 0;
            Call(nameNodeClient, Services.GetBlockSize.ToString(), true, ref blockSize);

            using (var fileStream = new FileStream(fullFilePath, FileMode.Open, FileAccess.Read))
            {
                byte[] dataStagingBytes = new byte[blockSize];
                foreach (var metaData in reply)
                {
                    int bytesRead = fileStream.Read(dataStagingBytes, 0, dataStagingBytes.Length);
                    byte[] trimmedBytes = new byte[bytesRead];
                    Array.Copy(dataStagingBytes, trimmedBytes, bytesRead);

                    var blockId = metaData.BlockId;
                    var blockAddresses = metaData.BlockAddresses;

                    var startingDataNode = blockAddresses[0];
                    var remainingDataNodes = new List<NodeAddress>();
                    if (blockAddresses.Count > 1)
                        remainingDataNodes = blockAddresses.Skip(1).ToList();

                    using (var dataNodeClient = new TcpClient(startingDataNode.Host, startingDataNode.ServicePort))
                    {
                        var dataNodeWriteRequest = new DataNodeWriteRequest
                        {
                            BlockId = blockId,
                            Data = trimmedBytes,
                            ReplicationNodes = remainingDataNodes
                        };
                        //var dataNodeReply = new DataNodeWriteStatus();
                        DataNodeWriteResponse dataNodeReply = null;

                        Call(dataNodeClient, Services.PutBlock.ToString(), dataNodeWriteRequest, ref dataNodeReply);
                    }
                }
            }

            return true;
        }

        public static (string fileContents, bool getStatus) Get(ref TcpClient nameNodeClient, string fileName)
        {
            return _Get(ref nameNodeClient, fileName);
        }

        private static (string fileContents, bool getStatus) _Get(ref TcpClient nameNodeClient, string fileName)
        {
            var request = new NameNodeReadRequest { FileName = fileName };
            var reply = new NameNodeMetaData[0];

            Call(nameNodeClient, Services.GetData.ToString(), request, ref reply);
            string fileContents = "";

            foreach (var metaData in reply)
            {
                var blockId = metaData.BlockId;
                var blockAddresses = metaData.BlockAddresses;
                bool blockFetchStatus = false;

                foreach (var selectedDataNode in blockAddresses)
                {
                    using (var dataNodeClient = new TcpClient(selectedDataNode.Host, selectedDataNode.ServicePort))
                    {
                        var dataNodeRequest = new DataNodeReadRequest { BlockId = blockId };
                        var dataNodeReply = new DataNodeData();

                        try
                        {
                            Call(dataNodeClient, Services.GetBlock.ToString(), dataNodeRequest, ref dataNodeReply);
                            fileContents += dataNodeReply.Data;
                            blockFetchStatus = true;
                            break;
                        }
                        catch (Exception)
                        {
                            continue;
                        }
                    }
                }

                if (!blockFetchStatus)
                {
                    return (fileContents, false);
                }
            }

            return (fileContents, true);
        }

        private static void Call<TRequest, TResponse>(TcpClient client, string serviceMethod, TRequest request, ref TResponse reply)
        {
            var jsonRequest = JsonConvert.SerializeObject(new RpcRequest(serviceMethod, request));
            Console.WriteLine($"Client sending: {jsonRequest}");
            byte[] bytesToSend = Encoding.UTF8.GetBytes(jsonRequest);
            client.Client.Send(bytesToSend);

            byte[] buffer = new byte[10240];   // 10KB
            int bytesRead = client.GetStream().Read(buffer, 0, buffer.Length);
            if (bytesRead > 0)
            {
                reply = JsonConvert.DeserializeObject<TResponse>(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                Console.WriteLine($"Client Received: {reply}");
            }
        }
    }
}