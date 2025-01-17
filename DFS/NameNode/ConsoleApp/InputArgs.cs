namespace BareDFS.NameNode.ConsoleApp
{
    using CommandLine;

    public class InputArgs
    {
        [Option('p', "port", Default=null, Required = true, HelpText = "Port Number for NameNode Service")]
        public ushort ServicePort { get; set; }

        [Option('d', "datanodes", Default=null, Required = true, HelpText = "All the DataNodes Address in the DFS")]
        public required string DataNodesAddr { get; set; }

        [Option("block-size-in-kb", Default=32, Required = false, HelpText = "Size of the Blockin KB. Default is 32KB")]
        public ulong BlockSize { get; set; }

        [Option("replication-factor", Default=null, Required = true, HelpText = "Replication Factor for the Data")]
        public ulong ReplicationFactor { get; set; }
    }
}