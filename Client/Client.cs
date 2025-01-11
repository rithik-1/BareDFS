namespace BareDFS.Client
{
    using System;
    using System.IO;
    using System.Net.Sockets;
    using System.Runtime.Serialization.Formatters.Binary;
    using BareDFS.Common.Models;
    using BareDFS.Common.Enums;

    public class Client
    {
        private static bool _Put(string nameNodeAddress, string sourcePath, string fileName)
        {
            string fullFilePath = Path.Combine(sourcePath, fileName);
            FileInfo fileInfo = new FileInfo(fullFilePath);

            if (!fileInfo.Exists)
            {
                throw new FileNotFoundException("File not found.", fullFilePath);
            }

            ulong fileSize = (ulong)fileInfo.Length;
            var request = new NameNodeWriteRequest { FileName = fileName, FileSize = fileSize };
            var reply = new NameNodeMetaData[0];

            using (var nameNodeClient = new TcpClient(nameNodeAddress.Split(':')[0], int.Parse(nameNodeAddress.Split(':')[1])))
            {
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
                        var remainingDataNodes = blockAddresses[1..];

                        using (var dataNodeClient = new TcpClient(startingDataNode.Host, int.Parse(startingDataNode.ServicePort)))
                        {
                            var dataNodeRequest = new DataNodeWriteRequest
                            {
                                BlockId = blockId,
                                Data = trimmedBytes,
                                ReplicationNodes = remainingDataNodes
                            };
                            //var dataNodeReply = new DataNodeWriteStatus();
                            var dataNodeReply = "";

                            Call(dataNodeClient, Services.PutBlock.ToString(), dataNodeRequest, ref dataNodeReply);
                        }
                    }
                }
            }

            return true;
        }

        private static (string fileContents, bool getStatus) _Get(string nameNodeAddress, string fileName)
        {
            var request = new NameNodeReadRequest { FileName = fileName };
            var reply = new NameNodeMetaData[0];

            using (var nameNodeClient = new TcpClient(nameNodeAddress.Split(':')[0], int.Parse(nameNodeAddress.Split(':')[1])))
            {
                Call(nameNodeClient, Services.GetData.ToString(), request, ref reply);
                string fileContents = "";

                foreach (var metaData in reply)
                {
                    var blockId = metaData.BlockId;
                    var blockAddresses = metaData.BlockAddresses;
                    bool blockFetchStatus = false;

                    foreach (var selectedDataNode in blockAddresses)
                    {
                        using (var dataNodeClient = new TcpClient(selectedDataNode.Host, int.Parse(selectedDataNode.ServicePort)))
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
        }

        private static void Call<TRequest, TResponse>(TcpClient client, string serviceMethod, TRequest request, ref TResponse response)
        {
            using (var networkStream = client.GetStream())
            {
                using (var writer = new StreamWriter(networkStream))
                {
                    var jsonRequest = System.Text.Json.JsonSerializer.Serialize(new { serviceMethod, request });
                    writer.WriteLine(jsonRequest);
                    writer.Flush();
                }

                using (var reader = new StreamReader(networkStream))
                {
                    var jsonResponse = reader.ReadLine();
                    response = System.Text.Json.JsonSerializer.Deserialize<TResponse>(jsonResponse);
                }
            }
        }
    }
}