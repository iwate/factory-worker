using FactoryWorker.Activity.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FactoryWorker.Activity.Providers
{
    public interface IDatasetProvider
    {
        string InstanceName { get; set; }
        dynamic Load(Slice slice);
        void Save(Slice slice, dynamic data);
    }
}
