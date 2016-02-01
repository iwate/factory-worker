using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FactoryWorker.Activity.Templating;
using FactoryWorker.Activity.Models;
using System.Collections.Generic;

namespace FactoryWorker.Tests
{
    [TestClass]
    public class FactoryWorkerRazorTest
    {
        [TestMethod]
        public void GenerateSharedTemplate_is_success()
        {
            var expected =
@"@functions {
protected Sample.Sample1DbContext Sample1 { get { return (Sample.Sample1DbContext)Model.Sample1; } }
protected Sample.Sample2DbContext Sample2 { get { return (Sample.Sample2DbContext)Model.Sample2; } }
}
";
            var shared = FactoryWorkerRazor.GenerateSharedTemplate(new List<CustomDbDataset> {
                new CustomDbDataset { InstanceName = "Sample1",DbContextName = "Sample.Sample1DbContext" },
                new CustomDbDataset { InstanceName = "Sample2",DbContextName = "Sample.Sample2DbContext" },
            });
            Assert.AreEqual(expected.Replace("\r",""), shared);
        }
    }
}
