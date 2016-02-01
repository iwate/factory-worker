using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.WindowsAzure.Storage;
using System;
using System.IO;
using System.Reflection;

namespace FactoryWorker.Activity.Models
{
    /// <summary>
    /// CustomDataset's typeProperties object class.
    /// {
    ///     "name": "Sample",
    ///     "properties": {
    ///         "published": false,
    ///         "type": "CustomDataset",
    ///         "linkedServiceName": "MyAzureSqlDatabaseLinkedService",
    ///         "typeProperties": {
    ///             "InstanceName": "SampleDb"
    ///             "DbContextName": "MyNamespace.MyDbContext",
    ///             "PackageLinkedService": "MyAzureStorageLinkedService",
    ///             "AssemblyFile": "mycontainer/myfolder/my.dll"
    ///         },
    ///         "availability": {
    ///             "frequency": "Hour",
    ///             "interval": 1
    ///         },
    ///         "external": true,
    ///         "policy": {}
    ///     }
    /// }
    /// </summary>
    public class CustomDbDataset
    {
        /// <summary>
        /// Instance name for property in RazorTemplate
        /// </summary>
        /// <example>
        /// @{
        ///     //if you set "SampleDb" to InstanceName, you use it in Razor
        ///     var items = SampleDb.Items.Take(10);
        /// }
        /// </example>
        public string InstanceName { get; set; }
        /// <summary>
        /// Full name(namespace contained) for DbContextName
        /// </summary>
        public string DbContextName { get; set; }
        /// <summary>
        /// Dll path in Azure Blob Storage.
        /// Dll is needed that is extended DbContext of EntityFramework
        /// </summary>
        public string AssemblyFile { get; set; }
    }
}

