namespace BareDFS.NameNode.Library
{
    using System;

    [Serializable]
    public class UnderReplicatedBlocks
    {
        public string BlockId { get; set; }
        public Guid HealthyDataNodeId { get; set; }
    }
}