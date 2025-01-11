namespace BareDFS.Common.Models
{
    [Serializable]
    public class NameNodeWriteRequest
    {
        public string FileName { get; set; }
        public ulong FileSize { get; set; }
    }
}