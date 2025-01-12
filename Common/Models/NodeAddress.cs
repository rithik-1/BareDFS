namespace BareDFS.Common
{
    using System;

    [Serializable]
    public class NodeAddress
    {
        public string Host { get; set; }
        public ushort ServicePort { get; set; }
    }
}