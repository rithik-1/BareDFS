namespace BareDFS.Client.ConsoleApp
{
    using CommandLine;
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Runtime.Serialization;
    using BareDFS.Client.Library;

    public class Program
    {
        private static ClientHandler clientHandler;

        public static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("Usage: \n");
                Console.WriteLine(".\\BareDFSClient --namenode <Endpoint> --operation put --source-path <locationToFile> --filename <fileName>");
                Console.WriteLine(".\\BareDFSClient --namenode <Endpoint> --operation get --filename <fileName>");
                Console.WriteLine("\n ------------------------------------ \n\n");
                Console.WriteLine("Example: \n");
                Console.WriteLine(".\\BareDFSClient --namenode localhost:9000 --operation get --filename foo.bar");
                Console.WriteLine("\n ------------------------------------ \n\n\n");
            }

            Parser.Default.ParseArguments<InputArgs>(args)
                   .WithParsed(Run)
                   .WithNotParsed(HandleParseError);
        }

        static void Run(InputArgs args)
        {
            string nameNodeAddress = args.NameNodeAddr;
            string operation = args.Operation;
            string fileName = args.FileName;

            clientHandler = new ClientHandler();

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
                    Console.WriteLine("Invalid Operation");
                    break;
            }
        }

        static void HandleParseError(IEnumerable<Error> errs)
        {
            Console.WriteLine("Invalid Arguments");
            Console.WriteLine("Errors: ");
            foreach (var err in errs)
            {
                Console.WriteLine(err);
            }
        }
    }
}