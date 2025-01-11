namespace BareDFS.Common.Models
{
    [Serializable]
    public class DataNodeWriteRequest
    {
        public int BlockId { get; set; }
        public byte[] Data { get; set; }
        public NodeAddress[] ReplicationNodes { get; set; }
    }
}