using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using Microsoft.Azure.Management.DataFactories.Models;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Management.DataFactories.Common.Models;
using ADFActivity = Microsoft.Azure.Management.DataFactories.Models.Activity;
using FactoryWorker.Activity;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace FactoryWorker.Tests
{
    [TestClass]
    public class UnitTest1
    {
        class Logger : IActivityLogger
        {
            public void Write(string format, params object[] args)
            {
                Console.Write(format, args);
            }
        }
        [TestMethod]
        public void TestMethod1()
        {
            new FactoryWorkerActivity().Execute(LinkedServices, Datasets, Activity, new Logger());
        }
        IEnumerable<LinkedService> LinkedServices = new List<LinkedService>
        {
            new LinkedService
            {
                Name = "Storage",
                Properties = new LinkedServiceProperties(new AzureStorageLinkedService
                {
                    ConnectionString = "BlobEndpoint=https://fwsample.blob.core.windows.net/;AccountName=fwsample;AccountKey=JJQMG1R2xtxEqdBcIZ5LrRGTAKWEQMD9QLOKvhkNhoZwofC4rU90tWKNqV7G2caTFbpgHNULM1qbBUYbHLGYzA=="
                })
            },
            new LinkedService
            {
                Name = "Database",
                Properties = new LinkedServiceProperties(new AzureSqlDatabaseLinkedService {
                    ConnectionString = "Server=tcp:fwsample.database.windows.net,1433;Database=fwsample;User ID=iwate@fwsample;Password=!Qaz2wsx;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
                })
            },
            new LinkedService
            {
                Name = "PackageStorage",
                Properties = new LinkedServiceProperties(new AzureStorageLinkedService
                {
                    ConnectionString = "BlobEndpoint=https://fwsample.blob.core.windows.net/;AccountName=fwsample;AccountKey=JJQMG1R2xtxEqdBcIZ5LrRGTAKWEQMD9QLOKvhkNhoZwofC4rU90tWKNqV7G2caTFbpgHNULM1qbBUYbHLGYzA=="
                })
            },
            new LinkedService
            {
                Name = "FactoryWorker",
                Properties = new LinkedServiceProperties(new AzureBatchLinkedService
                {
                    AccountName = "fwsample",
                    AccessKey = "+MEhm04GyZjJFp2RrlifKpRCS8RaRnHNk4D5vfnRvJFUppryIDM/0onYsP8FIhVZSHwo+7bSvk1I3FOZkuXoNA==",
                    PoolName = "sample",
                    BatchUri = "https://japaneast.batch.azure.com",
                    LinkedServiceName = "PackageStorage"
                })
            }
        };
        IEnumerable<Dataset> Datasets = new List<Dataset>
        {
            new Dataset
            {
                Name = "Outputs1",
                Properties = new DatasetProperties(new AzureBlobDataset {
                    FolderPath = "outputs/",
                    FileName = "sample.csv",
                    Format = new TextFormat
                    {
                        RowDelimiter = "\n",
                        ColumnDelimiter = ","
                    },
                    PartitionedBy = new List<Partition>
                    {
                        new Partition
                        {
                            Name = "slice",
                            Value = new DateTimePartitionValue
                            {
                                Date = "SliceStart",
                                Format = "yyyy-MM-dd"
                            }
                        }
                    }
                },
                new Availability {
                    Frequency = "Day",
                    Interval = 1
                }, "Storage")
            },
            new Dataset
            {
                Name = "Outputs2",
                Properties = new DatasetProperties(new CustomDataset {
                    ServiceExtraProperties = new Dictionary<string, JToken> {
                        { "InstanceName", "Orders" },
                        { "Type", "AzureBlob" },
                        { "Format", JObject.Parse("{\"Type\": \"json\"}") },
                        { "FilePath", "outputs/sample.json" }
                    }
                }, new Availability{
                    Frequency = "Day",
                    Interval = 1
                }, "Storage")
            },
            new Dataset
            {
                Name = "Database",
                Properties = new DatasetProperties(new CustomDataset {
                    ServiceExtraProperties = new Dictionary<string, JToken> {
                        { "InstanceName", "SampleDb" },
                        { "DbContextName", "FactoryWorker.Sample.SampleDbContext" },
                        { "AssemblyFile", "assemblies/FactoryWorker.Sample.dll" },
                        { "PackageLinkedService", "PackageStorage" }
                    }
                }, new Availability{
                    Frequency = "Day",
                    Interval = 1
                }, "Database")
            }
        };
        ADFActivity Activity = new ADFActivity
        {
            Name = "FactoryWorkerActtivity",
            LinkedServiceName = "FactoryWorker",
            Inputs = new List<ActivityInput> { new ActivityInput { Name = "Outputs2" } },
            Outputs = new List<ActivityOutput> { new ActivityOutput { Name = "Database" } },
            Policy = new ActivityPolicy
            {
                Concurrency = 1,
                ExecutionPriorityOrder = "OlderFirst",
                Retry = 3,
                Timeout = TimeSpan.FromMinutes(30),
                Delay = TimeSpan.Zero
            },
            TypeProperties = new DotNetActivity("FactoryWorker.Activity.dll", "FactoryWorker.Activity.FactoryWorkerActivity", "assemblies/FactoryWorker.Activity.zip", "PackageStorage")
            {
                ExtendedProperties = new Dictionary<string, string>
                {
                    { "transform", "@{\n    var orders = Model.Orders as IEnumerable<dynamic>;\n    var details =  orders.SelectMany<dynamic,dynamic>(_ => _.Details).ToList();\n    var headers = orders.Select(h => new {\n      SalesOrderID = (int)h.SalesOrderID,\n      RevisionNumber = (byte)h.RevisionNumber,\n      OrderDate = Convert.ToDateTime((string)h.OrderDate),\n      DueDate = Convert.ToDateTime((string)h.DueDate),\n      ShipDate = Convert.ToDateTime((string)h.ShipDate),\n      Status = (byte)h.Status,\n      OnlineOrderFlag = (bool)h.OnlineOrderFlag,\n      SalesOrderNumber = (string)h.SalesOrderNumber,\n      PurchaseOrderNumber = (string)h.PurchaseOrderNumber,\n         ShipMethod = (string)h.ShipMethod,\n      SubTotal = (decimal)h.SubTotal,\n      TaxAmt = (decimal)h.TaxAmt,\n    });\n    ViewBag.Exports = new {\n      SalesOrderHeaders = headers };\n  }\n"},
                    //{ "transform", "@{\n  ViewBag.Exports = SampleDb.SalesOrderHeaders\n                      .Where(_ => _.OrderDate >= Slice.Start && _.OrderDate < Slice.End)\n                      .Select(h => new {\n                        SalesOrderID = h.SalesOrderID,\n                        RevisionNumber = h.RevisionNumber,\n                        OrderDate = h.OrderDate,\n                        DueDate = h.DueDate,\n                        ShipDate = h.ShipDate,\n                        Status = h.Status,\n                        OnlineOrderFlag = h.OnlineOrderFlag,\n                        SalesOrderNumber = h.SalesOrderNumber,\n                        PurchaseOrderNumber = h.PurchaseOrderNumber,\n                        AccountNumber = h.AccountNumber,\n                        CustomerID = h.CustomerID,\n                        ShipToAddressID = h.ShipToAddressID,\n                        BillToAddressID = h.BillToAddressID,\n                        ShipMethod = h.ShipMethod,\n                        SubTotal = h.SubTotal,\n                        TaxAmt = h.TaxAmt,\n                        Details = h.SalesOrderDetails.Select (d => new {\n                          ProductID = d.ProductID,\n                          UnitPrice = d.UnitPrice,\n                          UnitPriceDiscount = d.UnitPriceDiscount,\n                          LineTotal = d.LineTotal\n                        })\n                      })\n                      .ToList();\n}" },
                    { "SliceStart", "2008-06-01T00:00:00" },
                    { "SliceEnd", "2008-06-01T01:00:00" }
                }
            }
        };
    }
}
