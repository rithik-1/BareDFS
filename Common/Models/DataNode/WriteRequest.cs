namespace BareDFS.Common.Models
{
    using System;

    [Serializable]
    public class DataNodeWriteRequest
    {
        public int BlockId { get; set; }
        public byte[] Data { get; set; }
        public NodeAddress[] ReplicationNodes { get; set; }
    }
}