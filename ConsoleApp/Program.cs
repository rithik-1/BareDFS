namespace BareDFS.ConsoleApp
{
    using BareDFS.Client.Library;
    using BareDFS.DataNode.Library;
    using BareDFS.NameNode.Library;
    using CommandLine;
    using System;
    using System.Linq;
    using System.Collections.Generic;

    public static class Program
    {
        private static ManualResetEvent _quitEvent = new ManualResetEvent(false);

        static void Main(string[] args)
        {
            Console.CancelKeyPress += StopConsoleApp();

            do {
                if (args.Length < 1)
                {
                    Console.WriteLine("Invalid Sub-command");
                    PrintExampleCommand();
                }

                for (int i = 0; i < args.Length; i++)
                {
                    args[i] = args[i].ToLower().Trim();
                }

                Parser.Default.ParseArguments<ClientInputArgs, NameNodeInputArgs, DataNodeInputArgs>(args)
                        .MapResult<ClientInputArgs, NameNodeInputArgs, DataNodeInputArgs, object>(
                            (ClientInputArgs inputArgs) => { RunClient(inputArgs); return null; },
                            (NameNodeInputArgs inputArgs) => { RunNameNode(inputArgs); return null; },
                            (DataNodeInputArgs inputArgs) => { RunDataNode(inputArgs); return null; },
                            errs => { HandleParseError(errs); return null; });

                Console.WriteLine("\nEnter another command or press Ctrl+C to exit\n");
                var input = Console.ReadLine();
                if (input != null)
                {
                    args = input.Split(' ');
                }
                else
                {
                    _quitEvent.Set();
                }
            }
            while (!_quitEvent.WaitOne(0));
        }

        private static void RunClient(ClientInputArgs args)
        {
            string nameNodeAddress = args.NameNodeAddr;
            string operation = args.Operation;
            string fileName = args.FileName;

            ClientHandler clientHandler = new ClientHandler();

            switch (operation)
            {
                case "put":
                    string sourcePath = args.SourcePath;
                    var put_result = clientHandler.PutHandler(nameNodeAddress, sourcePath, fileName);
                    if (put_result == true)
                        Console.WriteLine($"Successfully put {fileName} to BareDFS \n");
                    else
                        Console.WriteLine($"Failed to put {fileName} to BareDFS \n");
                    break;
                case "get":
                    var get_result = clientHandler.GetHandler(nameNodeAddress, fileName);
                    if (get_result.Item2 == true)
                        Console.WriteLine($"Successfully got {fileName} from BareDFS \n");
                    else
                        Console.WriteLine($"Failed to get {fileName} from BareDFS \n");
                    break;
                default:
                    Console.WriteLine("Invalid Operation\n");
                    break;
            }
        }

        private static void RunNameNode(NameNodeInputArgs args)
        {
            NameNodeHandler nameNodeHandler = new NameNodeHandler(
                args.ServicePort,
                args.BlockSize,
                args.ReplicationFactor,
                args.DataNodesAddr.Split(',').ToList());

            try
            {
                nameNodeHandler.StartNameNodeServer();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static void RunDataNode(DataNodeInputArgs args)
        {
            string dataLocation = args.DataLocation;
            ushort servicePort = args.ServicePort;
            DataNodeHandler dataNodeHandler = new DataNodeHandler(dataLocation, servicePort);

            try
            {
                dataNodeHandler.StartDataNodeServer();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static void HandleParseError(IEnumerable<Error> errs)
        {
            Console.WriteLine("Invalid Arguments");
            Console.WriteLine("Errors: ");
            foreach (var err in errs)
            {
                Console.WriteLine(err);
            }
            Console.WriteLine();

            PrintExampleCommand();
        }

        public static ConsoleCancelEventHandler StopConsoleApp()
        {
            void ConsoleCancelHandler(object sender, ConsoleCancelEventArgs e)
            {
                _quitEvent.Set();
            }

            return new ConsoleCancelEventHandler(ConsoleCancelHandler);
        }

        private static void PrintExampleCommand()
        {
            Console.WriteLine("Example Commands: \n");
            Console.WriteLine("datanode --port 7000 --data-location /path/to/data\n");
            Console.WriteLine("namenode --port 9000 --datanodes localhost:7000,localhost:7001 --block-size-in-kb 32 --replication-factor 3\n");
            Console.WriteLine("client --namenode localhost:9000 --operation put --source-path /path/to/file --filename file.txt\n");
            Console.WriteLine("client --namenode localhost:9000 --operation get --filename file.txt\n");
            Console.WriteLine("The datanode should be first setup, then the namenode, and finally the client can do put/get operations.\n");
            Console.WriteLine("\n ------------------------------------ \n\n");
        }
    }
}