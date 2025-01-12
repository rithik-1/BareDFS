namespace BareDFS.DataNode.Library
{
    using System;

    public class DataNodeInstance
    {
        public string NodeId { get; private set; }
        public string DataDirectory { get; private set; }
        public ushort ServicePort { get; private set; }

        public DataNodeInstance(string address, ushort port, string nodeId = null)
        {
            NodeId = nodeId ?? Guid.NewGuid().ToString();
            DataDirectory = address;
            ServicePort = port;
        }

        public void ReportStatus()
        {
            // Report the status of the DataNode instance
            Console.WriteLine($"DataNode instance {NodeId} is running on port {ServicePort} and storing data at {DataDirectory}\n");
        }
    }
}