namespace BareDFS.NameNode.ConsoleApp
{
    using BareDFS.NameNode.Library;
    using CommandLine;
    using System;
    using System.Collections.Generic;

    public class Program
    {
        private static NameNodeHandler? nameNodeHandler = null;

        public static void Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine("Usage: \n");
                Console.WriteLine(".\\Namenode --port <portNumber> --datanodes <dnEndpoints> --block-size-in-kb <blockSize> --replication-factor <replicationFactor>");
                Console.WriteLine("\n ------------------------------------ \n\n");
                Console.WriteLine("Example: \n");
                Console.WriteLine(".\\Namenode --port 9000 --datanodes localhost:7000,localhost:7001,localhost:7002 --block-size-in-kb 10 --replication-factor 2");
                Console.WriteLine("\n ------------------------------------ \n\n\n");
            }

            Parser.Default.ParseArguments<InputArgs>(args)
                   .WithParsed(Run)
                   .WithNotParsed(HandleParseError);
        }

        static void Run(InputArgs args)
        {

            nameNodeHandler ??= new NameNodeHandler(
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