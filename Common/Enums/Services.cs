namespace BareDFS.Common
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    [JsonConverter(typeof(StringEnumConverter))]
    public enum Services
    {
        GetData,
        WriteData,
        GetBlockSize,
        PutBlock,
        GetBlock,
    }
}