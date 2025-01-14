namespace BareDFS.ConsoleApp
{
    using CommandLine;

    [Verb("client", HelpText = "Client Operations")]
    public class ClientInputArgs
    {
        [Option('n', "namenode", Default=null, Required = true, HelpText = "NameNode Address")]
        public string NameNodeAddr { get; set; }

        [Option('o', "operation", Default=null, Required = true, HelpText = "Operation Type")]
        public string Operation { get; set; }

        [Option('s', "source-path", Default=null, Required = false, HelpText = "File Path to be uploaded")]
        public string SourcePath { get; set; }

        [Option('f', "filename", Default=null, Required = true, HelpText = "File Name as in the DFS")]
        public string FileName { get; set; }
    }

    [Verb("namenode", HelpText = "NameNode Operations")]
    public class NameNodeInputArgs
    {
        [Option('p', "port", Default=null, Required = true, HelpText = "Port Number for NameNode Service")]
        public ushort ServicePort { get; set; }

        [Option('d', "datanodes", Default=null, Required = true, HelpText = "All the DataNodes Address in the DFS")]
        public required string DataNodesAddr { get; set; }

        [Option("block-size-in-kb", Default=64, Required = false, HelpText = "Size of the Blockin KB. Default is 64KB")]
        public ulong BlockSize { get; set; }

        [Option("replication-factor", Default=null, Required = true, HelpText = "Replication Factor for the Data")]
        public ulong ReplicationFactor { get; set; }
    }

    [Verb("datanode", HelpText = "DataNode Operations")]
    public class DataNodeInputArgs
    {
        [Option('p', "port", Default=null, Required = true, HelpText = "Port to run the DataNode service on")]
        public ushort ServicePort { get; set; }

        [Option("data-location", Default=null, Required = true, HelpText = "DataNode's data location on the machine")]
        public string DataLocation { get; set; }
    }
}