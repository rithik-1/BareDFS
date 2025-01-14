namespace BareDFS.DataNode.ConsoleApp
{
    using CommandLine;
    using System;
    using BareDFS.DataNode.Library;

    public class Program
    {
        private static DataNodeHandler? dataNodeHandler = null;

        public static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: \n");
                Console.WriteLine(".\\Datanode --port <portNumber> --data-location <dataLocation>");
                Console.WriteLine("\n ------------------------------------ \n\n");
                Console.WriteLine("Example: \n");
                Console.WriteLine(".\\Datanode --port 7002 --data-location .dndata3/");
                Console.WriteLine("\n ------------------------------------ \n\n\n");
            }

            Parser.Default.ParseArguments<InputArgs>(args)
                   .WithParsed(Run)
                   .WithNotParsed(HandleParseError);
        }

        static void Run(InputArgs args)
        {
            string dataLocation = args.DataLocation;
            ushort servicePort = args.ServicePort;
            dataNodeHandler ??= new DataNodeHandler(dataLocation, servicePort);

            try
            {
                dataNodeHandler.StartDataNodeServer();
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