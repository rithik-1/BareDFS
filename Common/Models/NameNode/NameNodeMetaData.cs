namespace BareDFS.Common.Models
{
    [Serializable]
    public class NameNodeMetaData
    {
        public int BlockId { get; set; }
        public NodeAddress[] BlockAddresses { get; set; }
    }
}