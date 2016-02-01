using System.Collections.Generic;
using System.Linq;
using FactoryWorker.Activity.Models;
using System.Reflection;

namespace FactoryWorker.Activity.Providers
{
    public abstract class DatasetProviderBase : IDatasetProvider
    {
        public abstract dynamic Load(Slice slice);
        public abstract void Save(Slice slice, dynamic data);

        private Dictionary<string, List<PropertyInfo>> cache = new Dictionary<string, List<PropertyInfo>>();

        public abstract string InstanceName { get; set; }

        protected Dictionary<string, object> ToDictionary(object obj, string key = "default")
        {
            if (!cache.ContainsKey(key))
                cache.Add(key, obj.GetType().GetProperties().Where(x => x.CanRead).ToList());

            return cache[key].ToDictionary(_ => _.Name, _ => _.GetValue(obj));
        }
    }
}
