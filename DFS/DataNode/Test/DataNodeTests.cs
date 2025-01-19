namespace BareDFS.DataNode.Test
{
    using BareDFS.Common;
    using BareDFS.DataNode.Library;
    using System;
    using System.IO;
    using System.Text;
    using Xunit;

    [Trait("Category", "QTestSkip")]
    public class DataNodeTests
    {
        private readonly DataNodeInstance _dataNodeInstance;
        private DataNode _dataNode;

        public DataNodeTests()
        {
            string currentDirectory = Directory.GetCurrentDirectory();
            _dataNodeInstance = new DataNodeInstance(currentDirectory + "test_datanode", 7500);
            InitializeDataNode();
        }

        [Fact]
        private void InitializeDataNode()
        {
            _dataNode = new DataNode(_dataNodeInstance);
        }

        [Theory]
        [InlineData("123456789")]
        private void PutBlockTest(string blockId)
        {
            var data = "SampleData";
            var writeRequest = new DataNodeWriteRequest
            {
                BlockId = blockId,
                Data = Encoding.ASCII.GetBytes(data),
                ReplicationNodes = null
            };

            var response = _dataNode.PutData(_dataNodeInstance.DataDirectory, writeRequest);
            bool expectedValue = true;

            Assert.Equal(expectedValue, response.Status);
        }

        [Fact]
        private void GetBlockTest()
        {
            string blockId = Guid.NewGuid().ToString();
            PutBlockTest(blockId);
            var readRequest = new DataNodeReadRequest
            {
                BlockId = blockId
            };
            var expectedValue = true;

            var response = _dataNode.GetData(_dataNodeInstance.DataDirectory, readRequest);

            Assert.Equal(expectedValue, response.Status);
        }

        [Fact]
        private void HeartbeatTest()
        {
            var data = "Heartbeat Test";
            var expectedValue = true;

            var actualValue = _dataNode.Heartbeat(data);

            Assert.Equal(expectedValue, actualValue);
        }

        [Fact]
        private void PingTest()
        {
            var data = "Ping Test";
            var expectedValue = true;

            var actualValue = _dataNode.Ping(data);

            Assert.Equal(expectedValue, actualValue);
        }
    }
}