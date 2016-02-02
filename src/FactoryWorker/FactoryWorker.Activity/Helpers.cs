using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Common.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FactoryWorker.Activity
{
    public class Helpers
    {
        public static CloudBlockBlob GetBlob(LinkedService linkedService, string filepath)
        {
            var azStorage = linkedService.Properties.TypeProperties as AzureStorageLinkedService;
            if (azStorage == null)
                throw new ArgumentException("LinkedService is not AzureStorageLinkedService");
            var account = CloudStorageAccount.Parse(azStorage.ConnectionString);
            var client = account.CreateCloudBlobClient();
            var index = filepath.IndexOf("/");
            var container = client.GetContainerReference(filepath.Substring(0, index));
            container.CreateIfNotExists();
            return container.GetBlockBlobReference(filepath.Substring(index + 1));
        }
        public static string ReplaceByPatition(string str, IEnumerable<Partition> partitionedBy, Slice slice)
        {
            var partitions = partitionedBy.Select(_ =>
            {
                var dateParition = _.Value as DateTimePartitionValue;
                var name = dateParition.Date.ToLower();
                var value = name == "slicestart" ? slice.Start.ToString(dateParition.Format)
                          : name == "sliceend" ? slice.End.ToString(dateParition.Format)
                          : "";
                return new KeyValuePair<string, string>(_.Name, value);
            });
            foreach (var partition in partitions)
            {
                str = str.Replace($"{{{partition.Key}}}", partition.Value);
            }
            return str;
        }
        public static dynamic DictionaryToObject(IDictionary<string, object> record, IList<DataElement> structure)
        {
            dynamic obj = new ExpandoObject();
            var dict = (IDictionary<string, object>)obj;
            foreach (var column in structure)
            {
                switch (column.Type)
                {
                    case "Int16":
                        try
                        {
                            dict[column.Name] = Convert.ToInt16(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Int16);
                        }
                        break;
                    case "Int32":
                        try
                        {
                            dict[column.Name] = Convert.ToInt32(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Int32);
                        }
                        
                        break;
                    case "Int64":
                        try
                        {
                            dict[column.Name] = Convert.ToInt64(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Int64);
                        }
                        break;
                    case "Single":
                        try
                        {
                            dict[column.Name] = Convert.ToSingle(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Single);
                        }
                        break;
                    case "Double":
                        try
                        {
                            dict[column.Name] = Convert.ToDouble(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Double);
                        }
                        break;
                    case "Decimal":
                        try
                        {
                            dict[column.Name] = Convert.ToDecimal(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Decimal);
                        }
                        break;
                    case "Byte":
                        try
                        {
                            dict[column.Name] = Convert.ToByte(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Byte);
                        }
                        break;
                    case "Bool":
                        try
                        {
                            dict[column.Name] = Convert.ToBoolean(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Boolean);
                        }
                        break;
                    case "String":
                        try
                        {
                            dict[column.Name] = (string)record[column.Name];
                        }
                        catch
                        {
                            dict[column.Name] = default(String);
                        }
                        break;
                    case "Guid":
                        try
                        {
                            dict[column.Name] = Guid.Parse((string)record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(Guid);
                        }
                        break;
                    case "Datetime":
                        try
                        {
                            dict[column.Name] = Convert.ToDateTime(record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(DateTime);
                        }
                        break;
                    case "Datetimeoffset":
                        try
                        {
                            dict[column.Name] = DateTimeOffset.Parse((string)record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(DateTimeOffset);
                        }
                        break;
                    case "Timespan":
                        try
                        {
                            dict[column.Name] = TimeSpan.Parse((string)record[column.Name]);
                        }
                        catch
                        {
                            dict[column.Name] = default(TimeSpan);
                        }
                        break;
                    default:
                        break;
                }
            }
            return obj;
        }
    }
}
