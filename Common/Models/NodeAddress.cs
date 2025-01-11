namespace BareDFS.Common.Models
{
    using System;

    [Serializable]
    public class NodeAddress
    {
        public string Host { get; set; }
        public string ServicePort { get; set; }
    }
}