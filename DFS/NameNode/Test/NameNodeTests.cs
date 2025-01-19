namespace BareDFS.NameNode.Test
{
    using BareDFS.Common;
    using BareDFS.NameNode.Library;
    using System.Collections.Generic;
    using Xunit;

    [Trait("Category", "QTestSkip")]
    public class NameNodeTests
    {
        private readonly NameNodeInstance _nameNodeInstance;
        public NameNode _nameNode;

        public NameNodeTests(ushort nameNodePort = 9000)
        {
            var dataNodes = new List<string> { "localhost:7000", "localhost:8000" };
            _nameNodeInstance = new NameNodeInstance(nameNodePort, 16, 1, dataNodes);
            InitializeNameNode();
        }

        [Fact]
        private void InitializeNameNode()
        {
            _nameNode = new NameNode(_nameNodeInstance);
        }

        [Theory]
        [InlineData("SampleFile")]
        private void PutDataTest(string fileName)
        {
            var writeRequest = new NameNodeWriteRequest
            {
                FileName = fileName,
                FileSize = 1024,
            };
            var reply = new List<NameNodeMetaData>();

            var response = _nameNode.PutData(writeRequest, ref reply);
            bool expectedValue = true;

            Assert.Equal(expectedValue, response);
        }

        [Fact]
        private void GetDataTest()
        {
            string fileName = "SampleFile";
            PutDataTest(fileName);
            var readRequest = new NameNodeReadRequest
            {
                FileName = fileName
            };
            var expectedValue = true;
            var reply = new List<NameNodeMetaData>();

            var response = _nameNode.GetData(readRequest, ref reply);

            Assert.Equal(expectedValue, response);
        }

        [Fact]
        private void GetBlockSizeTest()
        {
            var request = true;
            var expectedValue = true;
            ulong reply = 0;

            var actualValue = _nameNode.GetBlockSize(request, ref reply);

            Assert.Equal(expectedValue, actualValue);
        }
    }
}