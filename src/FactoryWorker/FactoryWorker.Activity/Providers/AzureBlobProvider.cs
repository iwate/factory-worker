using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Common.Models;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using CsvHelper.Configuration;
using System.Dynamic;

namespace FactoryWorker.Activity.Providers
{
    public class AzureBlobProvider : DatasetProviderBase
    {
        public override string InstanceName { get; set; }
        IList<DataElement> Structure;
        CloudBlockBlob Blob;
        CsvConfiguration Configuration;
        public AzureBlobProvider(Dataset dataset, LinkedService linkedService)
        {
            InstanceName = dataset.Name;
            Structure = (dataset.Properties as DatasetProperties).Structure;
            var azblobDataset = dataset.Properties.TypeProperties as AzureBlobDataset;
            var filepath = Path.Combine(azblobDataset.FolderPath, azblobDataset.FileName);
            Blob = Helpers.GetBlob(linkedService, filepath);
            var format = azblobDataset.Format as TextFormat;
            if (format != null)
                Configuration = new CsvConfiguration
                {
                    Delimiter = format.ColumnDelimiter,
                    Encoding = Encoding.GetEncoding(format.EncodingName)
                };
            else
                Configuration = new CsvConfiguration { };
        }
        public static bool IsMatch(Dataset dataset, LinkedService linkedService)
        {
            return dataset.Properties.Type == "CustomDataset" && linkedService.Properties.Type == "AzureSqlDatabase";
        }
        public override dynamic Load(Slice slice)
        {
            var ret = new List<dynamic>();
            if (!Blob.Exists())
                return ret;
            using (var reader = new CsvHelper.CsvReader(new StreamReader(Blob.OpenRead(), Configuration.Encoding)))
            {
                var records = reader.GetRecords<dynamic>();
                foreach (IDictionary<string, object> record in records)
                    ret.Add(Helpers.DictionaryToObject(record, Structure));
            }
            return ret;
        }

        public override void Save(Slice slice, dynamic data)
        {
            var csv = CreateCsv((IEnumerable<dynamic>)data, Configuration);
            Blob.UploadText(csv ?? "");
        }
        internal string CreateCsv(IEnumerable<dynamic> data, CsvConfiguration configuration)
        {
            if (Structure != null && Structure.Count > 0)
                return createCsvWithStructure(data, configuration);
            else
                return createCsvWithoutStructure(data, configuration);
        }
        internal string createCsvWithStructure(IEnumerable<dynamic> data, CsvConfiguration configuration)
        {
            var guid = Guid.NewGuid().ToString();
            var builder = new StringBuilder();
            var writer = new CsvHelper.CsvWriter(new StringWriter(builder), configuration);

            foreach (var column in Structure)
                writer.WriteField(column.Name);

            writer.NextRecord();

            foreach (var datum in data)
            {
                var dict = ToDictionary(guid, datum);
                foreach (var column in Structure)
                {
                    var value = dict[column.Name];
                    var str = string.IsNullOrEmpty(column.Format) ? value.ToString()
                            : value.ToString(column.Format);

                    writer.WriteField(str);
                }
                writer.NextRecord();
            }
            return builder.ToString();
        }
        internal string createCsvWithoutStructure(IEnumerable<dynamic> data, CsvConfiguration configuration)
        {
            var guid = Guid.NewGuid().ToString();
            var builder = new StringBuilder();
            var writer = new CsvHelper.CsvWriter(new StringWriter(builder), configuration);

            if (data.Count() == 0)
                return null;

            var first = ToDictionary(guid, data.First());

            foreach (var name in first.Keys)
                writer.WriteField(name);
            writer.NextRecord();

            foreach (var datum in data)
            {
                var dict = ToDictionary(guid, datum);

                foreach (var key in first.Keys)
                    writer.WriteField(dict[key].ToString());

                writer.NextRecord();
            }

            return builder.ToString();
        }
    }
}
