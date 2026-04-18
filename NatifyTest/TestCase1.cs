using NUnit.Framework;
using Natify;

namespace Natify.Tests
{
    [TestFixture]
    public class NatifyTopicsTests
    {
        [Test]
        public void GetClientListenSubject_WithValidInputs_ReturnsCorrectFormat()
        {
            var result = NatifyTopics.GetClientListenSubject("GameClient", "AuthServer", "VN-01", "LoginResult");
            Assert.That(result, Is.EqualTo("NatifyClient.GameClient.AuthServer.VN-01.LoginResult"));
        }

        [Test]
        public void GetServerListenSubject_WithValidInputs_IncludesWildcard()
        {
            var result = NatifyTopics.GetServerListenSubject("AuthServer", "GameClient", "LoginRequest");
            Assert.That(result, Is.EqualTo("NatifyServer.AuthServer.GameClient.*.LoginRequest"));
        }

        [Test]
        public void GetClientPublishSubject_WithValidInputs_ReturnsCorrectFormat()
        {
            var result = NatifyTopics.GetClientPublishSubject("AuthServer", "GameClient", "VN-01", "LoginRequest");
            Assert.That(result, Is.EqualTo("NatifyServer.AuthServer.GameClient.VN-01.LoginRequest"));
        }

        [Test]
        public void GetServerPublishSubject_WithValidInputs_ReturnsCorrectFormat()
        {
            var result = NatifyTopics.GetServerPublishSubject("GameClient", "AuthServer", "VN-01", "LoginResult");
            Assert.That(result, Is.EqualTo("NatifyClient.GameClient.AuthServer.VN-01.LoginResult"));
        }

        [TestCase("NatifyServer.ServerA.ClientB.US-West.Chat", "US-West")]
        [TestCase("NatifyServer.ServerA.ClientB.EU-Central-1.Move", "EU-Central-1")]
        [TestCase("NatifyServer.ServerA.ClientB.12345.Action", "12345")]
        public void ExtractRegionIdFromServerSubject_ValidSubject_ReturnsCorrectRegion(string subject, string expected)
        {
            var result = NatifyTopics.ExtractRegionIdFromServerSubject(subject);
            Assert.That(result, Is.EqualTo(expected));
        }

        [TestCase("NatifyServer.ServerA.ClientB")] // Thiếu parts
        [TestCase("InvalidFormat")]
        [TestCase("")]
        public void ExtractRegionIdFromServerSubject_InvalidSubject_ReturnsEmptyString(string invalidSubject)
        {
            var result = NatifyTopics.ExtractRegionIdFromServerSubject(invalidSubject);
            Assert.That(result, Is.Empty);
        }
    }
}