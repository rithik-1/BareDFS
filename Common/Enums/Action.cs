namespace BareDFS.Common.Enums
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    [JsonConverter(typeof(StringEnumConverter))]
    public enum Action
    {
        GetData,
        WriteData,
        GetBlockSize,
        PutBlock,
        GetBlock,
    }
}