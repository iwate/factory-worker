using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FactoryWorker.Activity.Templating;

namespace FactoryWorker.Tests
{
    [TestClass]
    public class FactoryWorkerReferenceResolverTest
    {
        [TestMethod]
        public void LoadAssembliesConfig_is_success()
        {
            string[] ASSEMBLIES = { "EntityFramework.dll", "EntityFramework.SqlServer.dll" };
            var resolver = new FactoryWorkerReferenceResolver();
            var assemblies = resolver.Load();
            Assert.IsTrue(assemblies.Count == 2, "assemblies count is ok");
            Assert.IsTrue(assemblies.Where(_ => ASSEMBLIES.Contains(_.Name)).Count() == 2, "loading assemblies.xml is success");
        }
    }
}
