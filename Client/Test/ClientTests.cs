namespace BareDFS.Client.Test
{
    using BareDFS.Client.Library;
    using BareDFS.NameNode.Library;
    using System.IO;
    using System.Threading.Tasks;
    using Xunit;

    [Trait("Category", "QTestSkip")]
    public class ClientTests
    {
        private ClientHandler _clientHandler;
        private NameNodeHandler _nameNodeHandler;
        private string _nameNodeAddress = "";
        private string _currentDirectory;

        public ClientTests()
        {
            _currentDirectory = Directory.GetCurrentDirectory();
            InitializeNameNode(9000);
            InitializeClient();
        }

        private void InitializeNameNode(ushort port)
        {
            _nameNodeAddress = $"localhost:{port}";
            _nameNodeHandler = new NameNodeHandler(port, 1, 1, new System.Collections.Generic.List<string> { "localhost:7000", "localhost:8000" });
            Task.Run( () => _nameNodeHandler.StartNameNodeServer());
        }

        [Fact]
        private void InitializeClient()
        {
            _clientHandler = new ClientHandler();
        }

        [Theory]
        [InlineData("SampleFile")]
        private void PutDataTest(string fileName)
        {
            var path = Path.Combine(_currentDirectory, fileName);
            var response = _clientHandler.PutHandler(_nameNodeAddress, path, fileName);
            bool expectedValue = true;

            Assert.Equal(expectedValue, response);
        }

        [Fact]
        private void GetDataTest()
        {
            string fileName = "SampleFile";
            PutDataTest(fileName);
            var expectedValue = true;

            var response = _clientHandler.GetHandler(_nameNodeAddress, fileName);

            Assert.Equal(expectedValue, response.Item2);
        }
    }
}