namespace BareDFS.Common
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class NameNodeMetaData
    {
        public string BlockId { get; set; }
        public List<NodeAddress> BlockAddresses { get; set; }
    }
}