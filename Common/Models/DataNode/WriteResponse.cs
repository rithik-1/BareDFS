namespace BareDFS.Common
{
    using System;

    [Serializable]
    public class DataNodeWriteResponse
    {
        public bool Status { get; set; }
        public string Error { get; set; } = null;
    }
}