namespace BareDFS.Client.ConsoleApp
{
    using CommandLine;

    public class InputArgs
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
}