namespace DFS
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Collections.Generic;

    public static class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("sub-command is required");
                Environment.Exit(1);
            }

            var command = args[0];

            switch (command)
            {
                case "datanode":
                    var dataNodePort = GetArgumentValue(args, "--port", 7000);
                    var dataNodeDataLocation = GetArgumentValue(args, "--data-location", ".");
                    DataNode.InitializeDataNodeUtil(dataNodePort, dataNodeDataLocation);
                    break;

                case "namenode":
                    var nameNodePort = GetArgumentValue(args, "--port", 9000);
                    var nameNodeList = GetArgumentValue(args, "--datanodes", "");
                    var nameNodeBlockSize = GetArgumentValue(args, "--block-size", 32);
                    var nameNodeReplicationFactor = GetArgumentValue(args, "--replication-factor", 1);
                    var listOfDataNodes = string.IsNullOrEmpty(nameNodeList) ? new List<string>() : nameNodeList.Split(',').ToList();
                    NameNode.InitializeNameNodeUtil(nameNodePort, nameNodeBlockSize, nameNodeReplicationFactor, listOfDataNodes);
                    break;

                case "client":
                    var clientNameNodePort = GetArgumentValue(args, "--namenode", "localhost:9000");
                    var clientOperation = GetArgumentValue(args, "--operation", "");
                    var clientSourcePath = GetArgumentValue(args, "--source-path", "");
                    var clientFilename = GetArgumentValue(args, "--filename", "");

                    if (clientOperation == "put")
                    {
                        var status = Client.PutHandler(clientNameNodePort, clientSourcePath, clientFilename);
                        Console.WriteLine($"Put status: {status}");
                    }
                    else if (clientOperation == "get")
                    {
                        var (contents, status) = Client.GetHandler(clientNameNodePort, clientFilename);
                        Console.WriteLine($"Get status: {status}");
                        if (status)
                        {
                            Console.WriteLine(contents);
                        }
                    }
                    break;

                default:
                    Console.WriteLine("Invalid sub-command");
                    Environment.Exit(1);
                    break;
            }
        }

        private static T GetArgumentValue<T>(string[] args, string key, T defaultValue)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == key && i + 1 < args.Length)
                {
                    return (T)Convert.ChangeType(args[i + 1], typeof(T));
                }
            }
            return defaultValue;
        }
    }
}