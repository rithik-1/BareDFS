namespace BareDFS.DataNode.Library
{
    using System;
    using System.IO;

    public class DataNodeInstance
    {
        public string NodeId { get; private set; }
        public string DataDirectory { get; private set; }
        public ushort ServicePort { get; private set; }

        public DataNodeInstance(string path, ushort port, string nodeId = null)
        {
            NodeId = nodeId ?? Guid.NewGuid().ToString();
            DataDirectory = path;
            ServicePort = port;
            CheckPathExists();
        }

        private void CheckPathExists()
        {
            // Check if the path exists
            if (!Directory.Exists(DataDirectory))
            {
                Console.WriteLine($"[DataNode - {ServicePort}] Data location {DataDirectory} does not exist. Creating the directory.");
                try
                {
                    Directory.CreateDirectory(DataDirectory);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DataNode - {ServicePort}] Exception in creating the directory.");
                    Console.WriteLine($"[DataNode - {ServicePort}] Error: {ex.Message}");
                    throw;
                }
            }
        }

        public void ReportStatus()
        {
            // Report the status of the DataNode instance
            Console.WriteLine($"DataNode instance {NodeId} is running on port {ServicePort} and storing data at {DataDirectory}\n");
        }
    }
}