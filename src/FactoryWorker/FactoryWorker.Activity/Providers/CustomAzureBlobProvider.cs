using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace FactoryWorker.Activity.Providers
{
    enum CustomAzureBlobFormat
    {
        Unknown,
        Json
    }
    public class CustomAzureBlobProvider : DatasetProviderBase
    {
        public override string InstanceName { get; set; }
        CloudBlockBlob Blob;
        Encoding Encoding = Encoding.UTF8;
        CustomAzureBlobFormat Format;
        public CustomAzureBlobProvider(Dataset dataset, LinkedService linkedService, Slice slice)
        {
            var props = dataset.Properties.TypeProperties as CustomDataset;
            InstanceName = props.ServiceExtraProperties["instanceName"].ToString();
            var enc = props.ServiceExtraProperties.ContainsKey("encoding") 
                ? props.ServiceExtraProperties["encoding"].ToString() : null;
            if (!string.IsNullOrEmpty(enc))
            {
                try
                {
                    Encoding = Encoding.GetEncoding(enc);
                }
                catch (ArgumentException){}
            }
            
            var filepath = props.ServiceExtraProperties["filePath"].ToString();
            var regex = new Regex(@"\{(.+?)\}");
            var matches = regex.Matches(filepath);
            foreach(Match m in matches)
            {
                var key = m.Groups[1].Value;
                if (props.ServiceExtraProperties.ContainsKey(key))
                {
                    var formatAndName = props.ServiceExtraProperties[key].ToString().Split(',').Select(_ => _.Trim()).ToArray();
                    var value = formatAndName[1].ToLower() == "slicestart" ? slice.Start.ToString(formatAndName[0])
                              : formatAndName[1].ToLower() == "sliceend" ? slice.End.ToString(formatAndName[0])
                              : "";
                    filepath = filepath.Replace($"{{{formatAndName[1]}}}", value);
                }
            }
            Blob = Helpers.GetBlob(linkedService, filepath);

            var format = props.ServiceExtraProperties["format"];
            Format = format == null ? CustomAzureBlobFormat.Unknown
                   : format.ToString().ToLower() == "json" ? CustomAzureBlobFormat.Json
                   : CustomAzureBlobFormat.Unknown;

            if (props.ServiceExtraProperties.ContainsKey("test"))
                Console.Write(props.ServiceExtraProperties["test"].ToString());
        }
        public static bool IsMatch(Dataset dataset, LinkedService linkedService)
        {
            return dataset.Properties.Type == "CustomDataset" 
                && linkedService.Properties.Type == "AzureStorage"
                && ((CustomDataset)dataset.Properties.TypeProperties).ServiceExtraProperties["Type"].ToString() == "AzureBlob";
        }
        public override dynamic Load(Slice slice)
        {
            var text = Blob.DownloadText(Encoding);
            switch (Format)
            {
                case CustomAzureBlobFormat.Json:
                    return (IEnumerable<dynamic>)Jil.JSON.DeserializeDynamic(text);
                default:
                    return new List<dynamic>();
            }
            
        }

        public override void Save(Slice slice, dynamic data)
        {
            string text = "";
            switch (Format)
            {
                case CustomAzureBlobFormat.Json:
                    text = JsonConvert.SerializeObject(data);
                    break;
                default:
                    break;
            }
            Blob.UploadText(text ?? "", Encoding);
        }
    }
}
