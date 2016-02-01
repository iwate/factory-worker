using FactoryWorker.Activity.Models;
using FactoryWorker.Activity.Templating;
using Microsoft.Azure.Management.DataFactories.Runtime;
using RazorEngine.Templating;
using System.Collections.Generic;

namespace FactoryWorker.Activity
{
    public class FactoryWorkerTransformer
    {
        protected FactoryWorkerRazor Razor;
        public FactoryWorkerTransformer(IEnumerable<CustomDbDataset> datasets, IActivityLogger logger)
        {
            Razor = new FactoryWorkerRazor(datasets, logger);
        }
        public dynamic Transform(string template, dynamic model, Slice slice)
        {
            var viewBag = new DynamicViewBag();
            model.Slice = slice;
            Razor.RunWithTemplate(template ?? "", model, viewBag);
            return ((dynamic)viewBag).Exports;
        }
    }
}
