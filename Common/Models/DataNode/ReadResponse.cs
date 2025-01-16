namespace BareDFS.Common
{
    using System;

    [Serializable]
    public class DataNodeReadResponse
    {
        public bool Status { get; set; }
        public string Data { get; set; }
        public string Error { get; set; } = null;
    }
}