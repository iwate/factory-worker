using FactoryWorker.Activity.Providers;
using Microsoft.Azure.Management.DataFactories.Models;
using ADFActivity = Microsoft.Azure.Management.DataFactories.Models.Activity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Runtime;
using FactoryWorker.Activity.Models;
using System.Dynamic;

namespace FactoryWorker.Activity
{
    public class FactoryWorkerActivity : IDotNetActivity
    {
        public IDictionary<string, string> Execute(
            IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets,
            ADFActivity activity,
            IActivityLogger logger)
        {
            logger.Write("Actiity start.\n");
            Func<string, LinkedService> linkedServiceResolver = name => linkedServices.Single(_ => _.Name == name);
            IEnumerable<string> inputNames = activity.Inputs.Select(_ => _.Name);
            IEnumerable<string> outputNames = activity.Outputs.Select(_ => _.Name);
            IList<CustomDbDataset> dbDatasets = new List<CustomDbDataset>();

            // convert transform properties from activity's one.
            var dotNetActivity = (DotNetActivity)activity.TypeProperties;
            Slice slice = new Slice
            {
                Start = Convert.ToDateTime(dotNetActivity.ExtendedProperties["SliceStart"].ToString()),
                End = Convert.ToDateTime(dotNetActivity.ExtendedProperties["SliceEnd"].ToString())
            };
            string transform = dotNetActivity.ExtendedProperties["transform"].ToString();

            logger.Write("Slice from {0} to {1}\n", slice.Start, slice.End);
            logger.Write("Transform:\n");
            //logger.Write("{0}\n", transform);

            // create providers
            logger.Write("create providers\n");
            IDictionary<string, IDatasetProvider> providers =
                inputNames.Concat(outputNames)
                .Distinct()
                .Select(datasetName => {
                    IDatasetProvider provider = null;
                    var dataset = datasets.Single(_ => _.Name == datasetName);
                    var linkedService = linkedServiceResolver(dataset.Properties.LinkedServiceName);

                    if (CustomDbDatasetProvider.IsMatch(dataset, linkedService))
                    {
                        provider = new CustomDbDatasetProvider(dataset, linkedService, linkedServiceResolver);
                        dbDatasets.Add((provider as CustomDbDatasetProvider).Dataset);
                        logger.Write("{0} is CustomDbDataset\n", datasetName);
                    }
                    else if (CustomAzureBlobProvider.IsMatch(dataset, linkedService))
                    {
                        provider = new CustomAzureBlobProvider(dataset, linkedService, slice);
                        logger.Write("{0} is CustomAzureBlobDataset\n", datasetName);
                    }
                    else if (AzureBlobProvider.IsMatch(dataset, linkedService))
                    {
                        provider = new AzureBlobProvider(dataset, linkedService, slice);
                        logger.Write("{0} is AzureBlobDataset\n", datasetName);
                    }
                    else
                    {
                        logger.Write("{0} is UnknownDataset\n", datasetName);
                    }

                    return new { dataset = datasetName, provider = provider };
                }).ToDictionary(_ => _.dataset, _ => _.provider);

            // create model for transform razor.
            dynamic model = new ExpandoObject();
            var dict = (IDictionary<string, dynamic>)model;
            foreach (var name in inputNames)
            {
                logger.Write("Try load {0}\n", name);
                dict[providers[name].InstanceName] = providers[name].Load(slice);
                logger.Write("Success\n");
            }

            // transform
            logger.Write("Start transform\n");
            var transformer = new FactoryWorkerTransformer(dbDatasets, logger);
            dynamic transformed = transformer.Transform(transform, model, slice);
            logger.Write("End\n");

            // save all
            foreach (var name in outputNames)
            {
                logger.Write("Try save {0}\n", name);
                providers[name].Save(slice, transformed);
                logger.Write("Success\n");
            }
            
            return new Dictionary<string, string>();
        }
    }
}
