namespace BareDFS.Common
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class DataNodeWriteRequest
    {
        public string BlockId { get; set; }
        public byte[] Data { get; set; }
        public List<NodeAddress> ReplicationNodes { get; set; }
    }
}