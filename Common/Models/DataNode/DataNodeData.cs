namespace BareDFS.Common
{
    using System;

    // Data model for the file contents
    // Can be extended to include custom data types
    [Serializable]
    public class DataNodeData
    {
        public byte[] Data { get; set; }
    }
}