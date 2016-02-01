﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

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
        public CustomAzureBlobProvider(Dataset dataset, LinkedService linkedService)
        {
            var props = dataset.Properties.TypeProperties as CustomDataset;
            InstanceName = props.ServiceExtraProperties["InstanceName"].ToString();
            var enc = props.ServiceExtraProperties.ContainsKey("Encoding") 
                ? props.ServiceExtraProperties["Encoding"].ToString() : null;
            if (!string.IsNullOrEmpty(enc))
            {
                try
                {
                    Encoding = Encoding.GetEncoding(enc);
                }
                catch (ArgumentException){}
            }
            
            var filepath = props.ServiceExtraProperties["FilePath"].ToString();
            Blob = Helpers.GetBlob(linkedService, filepath);

            var format = props.ServiceExtraProperties["Format"];
            Format = format == null ? CustomAzureBlobFormat.Unknown
                   : format["Type"].ToString().ToLower() == "json" ? CustomAzureBlobFormat.Json
                   : CustomAzureBlobFormat.Unknown;
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