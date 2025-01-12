namespace BareDFS.Common
{
    using System;

    [Serializable]
    public class NameNodeWriteRequest
    {
        public string FileName { get; set; }
        public ulong FileSize { get; set; }
    }
}