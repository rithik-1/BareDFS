namespace BareDFS.Client.Library
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.IO;
    using System.Net.Sockets;
    using System.Runtime.Serialization.Formatters.Binary;
    using BareDFS.Common;

    public static class Client
    {
        public static bool Put(ref TcpClient nameNodeClient, string sourcePath, string fileName)
        {
            return _Put(ref nameNodeClient, sourcePath, fileName);
        }

        private static bool _Put(ref TcpClient nameNodeClient, string sourcePath, string fileName)
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
            var reply = new NameNodeMetaData[0];

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
                        var dataNodeReply = "";

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

        private static void Call<TRequest, TResponse>(TcpClient client, string serviceMethod, TRequest request, ref TResponse response)
        {
            using (var networkStream = client.GetStream())
            {
                using (var writer = new StreamWriter(networkStream))
                {
                    var jsonRequest = JsonConvert.SerializeObject(new RpcRequest(serviceMethod, request));
                    writer.WriteLine(jsonRequest);
                    writer.Flush();
                }

                using (var reader = new StreamReader(networkStream))
                {
                    var jsonResponse = reader.ReadLine();
                    response = JsonConvert.DeserializeObject<TResponse>(jsonResponse);
                }
            }
        }
    }
}