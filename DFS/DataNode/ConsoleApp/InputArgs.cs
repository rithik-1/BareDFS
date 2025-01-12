namespace BareDFS.DataNode.ConsoleApp
{
    using CommandLine;

    public class InputArgs
    {
        [Option('p', "port", Default=null, Required = true, HelpText = "Port to run the DataNode service on")]
        public ushort ServicePort { get; set; }

        [Option("data-location", Default=null, Required = true, HelpText = "DataNode's data location on the machine")]
        public string DataLocation { get; set; }
    }
}