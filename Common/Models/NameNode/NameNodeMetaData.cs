namespace BareDFS.Common.Models
{
    using System;

    [Serializable]
    public class NameNodeMetaData
    {
        public int BlockId { get; set; }
        public NodeAddress[] BlockAddresses { get; set; }
    }
}