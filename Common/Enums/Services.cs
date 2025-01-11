namespace BareDFS.Common.Enums
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