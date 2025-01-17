namespace BareDFS.NameNode.Library
{
    using BareDFS.Common;
    using System;
    using System.Collections.Generic;

    public class NameNodeInstance
    {
        public ushort Port { get; set; }
        public ulong BlockSize { get; set; }
        public ulong ReplicationFactor { get; set; }
        public Dictionary<Guid, NodeAddress> IdToDataNodes { get; set; } = new Dictionary<Guid, NodeAddress>();
        public Dictionary<string, List<string>> FileNameToBlocks { get; set; } = new Dictionary<string, List<string>>();
        public Dictionary<string, List<Guid>> BlockToDataNodeIds { get; set; } = new Dictionary<string, List<Guid>>();

        public NameNodeInstance(ushort serverPort, ulong blockSize, ulong replicationFactor, List<string> listOfDataNodes)
        {
            BlockSize = blockSize * 1024;
            ReplicationFactor = replicationFactor;
            Port = serverPort;

            foreach (var dataNode in listOfDataNodes)
            {
                var dataNodeParts = dataNode.Split(':');
                var dataNodeAddress = new NodeAddress
                {
                    Host = dataNodeParts[0],
                    ServicePort = ushort.Parse(dataNodeParts[1])
                };
                IdToDataNodes.Add(Guid.NewGuid(), dataNodeAddress);
            }
        }

        public void ReportStatus()
        {
            if (IdToDataNodes.Count == 0)
            {
                Console.WriteLine("No DataNodes are in service with the NameNode");
                return;
            }

            Console.WriteLine($"The list of DataNode(s) are:");
            foreach (var dataNode in IdToDataNodes)
            {
                Console.WriteLine($"DataNode ID: {dataNode.Key} at {dataNode.Value.Host}:{dataNode.Value.ServicePort}");
            }
        }
    }
}