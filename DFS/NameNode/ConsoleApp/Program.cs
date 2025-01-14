namespace BareDFS.NameNode.ConsoleApp
{
    using BareDFS.NameNode.Library;
    using CommandLine;
    using System;
    using System.Collections.Generic;

    public class Program
    {
        private static NameNodeHandler? nameNodeHandler = null;
        private static ManualResetEvent _quitEvent = new ManualResetEvent(false);

        public static void Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine("Invalid Arguments");
                PrintExampleCommand();
            }

            Console.CancelKeyPress += StopConsoleApp();

            Parser.Default.ParseArguments<InputArgs>(args)
                   .WithParsed(Run)
                   .WithNotParsed(HandleParseError);

            _quitEvent.WaitOne();
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

        public static void PrintExampleCommand()
        {
            Console.WriteLine("Example Command: \n");
            Console.WriteLine(".\\Namenode --port <portNumber> --datanodes <dnEndpoints> --block-size-in-kb <blockSize> --replication-factor <replicationFactor>");
            Console.WriteLine("\n ------------------------------------ \n");
            Console.WriteLine("Example: \n");
            Console.WriteLine(".\\Namenode --port 9000 --datanodes localhost:7000,localhost:7001,localhost:7002 --block-size-in-kb 10 --replication-factor 2");
            Console.WriteLine("\n ------------------------------------ \n\n");
        }
    }
}