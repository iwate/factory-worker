using RazorEngine.Templating;
using System;
using System.Linq;
using System.Collections.Generic;
using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace FactoryWorker.Activity.Templating
{
    public class FactoryWorkerTemplateBase : TemplateBase<dynamic>
    {
        protected Slice Slice
        {
            get
            {
                return Model.Slice as Slice;
            }
        }
        protected IActivityLogger Logger;
        public FactoryWorkerTemplateBase()
        {
            Logger = FactoryWorkerTemplateGlobals.Logger;
        }
    }
}
