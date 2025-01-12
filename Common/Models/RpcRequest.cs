namespace BareDFS.Common
{
    using System;

    [Serializable]
    public class RpcRequest
    {
        public string Operation { get; set; }
        public object Data { get; set; }

        public RpcRequest(string operation, object data)
        {
            Operation = operation;
            Data = data;
        }
    }
}