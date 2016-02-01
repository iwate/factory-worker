using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FactoryWorker.Activity.Providers
{
    public class CustomDbDatasetProvider : DatasetProviderBase
    {
        public override string InstanceName { get; set; }
        public CustomDbDataset Dataset;
        DbContext Context;
        public CustomDbDatasetProvider(Dataset dataset, LinkedService linkedService, Func<string, LinkedService> linkedServiceResolver)
        {
            var props = dataset.Properties.TypeProperties as CustomDataset;
            var packageLnkedServiceName = props.ServiceExtraProperties["PackageLinkedService"].ToString();
            Dataset = new CustomDbDataset
            {
                InstanceName = props.ServiceExtraProperties["InstanceName"].ToString(),
                DbContextName = props.ServiceExtraProperties["DbContextName"].ToString(),
                AssemblyFile = props.ServiceExtraProperties["AssemblyFile"].ToString()
            };
            InstanceName = Dataset.InstanceName;

            var blob = Helpers.GetBlob(linkedServiceResolver(packageLnkedServiceName), Dataset.AssemblyFile);
            var path = Path.Combine(Environment.CurrentDirectory, Path.GetFileName(Dataset.AssemblyFile));
            blob.DownloadToFile(path, FileMode.Create);
            var m = Assembly.LoadFrom(path);
            Context = (DbContext)Activator.CreateInstance(m.GetType(Dataset.DbContextName), (linkedService.Properties.TypeProperties as AzureSqlDatabaseLinkedService).ConnectionString);
        }
        public static bool IsMatch(Dataset dataset, LinkedService linkedService)
        {
            return dataset.Properties.Type == "CustomDataset" && linkedService.Properties.Type == "AzureSqlDatabase";
        }

        public override dynamic Load(Slice slice)
        {
            return Context;
        }

        public override void Save(Slice slice, dynamic data)
        {
            var dict = ToDictionary(data);
            var properties = data.GetType().GetProperties() as PropertyInfo[];
            foreach (var property in properties)
            {
                var guid = Guid.NewGuid().ToString();
                var type = GetTypeFromPropertyName(property.Name);
                var keyProps = GetKeys(Context, type);
                foreach (var item in dict[property.Name] as IEnumerable<dynamic>)
                {
                    var props = item is ExpandoObject ? item : ToDictionary(item, guid);
                    var obj = CreateAndCopy(type, props);
                    AddOrModify(type, obj, keyProps, props);
                }
            }
            using (var tran = Context.Database.BeginTransaction())
            {
                try
                {
                    Context.SaveChanges();
                    tran.Commit();
                }
                catch (DbUpdateException)
                {
                    tran.Rollback();
                }
            }
        }
        /// <summary>
        /// get prop's generic arg type. ex: if prop is IEnumerable<AnonymousType_1>, return AnonymousType_1 
        /// </summary>
        /// <param name="prop"></param>
        /// <returns></returns>
        internal Type getGenericType(PropertyInfo prop)
        {
            return prop.PropertyType.GetGenericArguments().First();
        }
        internal Type GetTypeFromPropertyName(string propertyName)
        {
            return Context.GetType().GetProperties().Where(_ => _.Name == propertyName).ToList().Select(_ => _.PropertyType.GetGenericArguments()[0]).First();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="createType"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        internal object CreateAndCopy(Type createType, IEnumerable<KeyValuePair<string, object>> properties)
        {
            var obj = Activator.CreateInstance(createType);
            foreach (var p in properties)
                createType.GetProperty(p.Key).SetValue(obj, p.Value);
            return obj;
        }
        void AddOrModify(Type type, object obj, PropertyInfo[] keys, IEnumerable<KeyValuePair<string, object>> properties)
        {
            if (IsInitialKeys(obj, keys))
                Context.Set(type).Add(obj);
            else
            {
                Context.Set(type).Attach(obj);
                foreach (var prop in properties)
                    Context.Entry(obj).Property(prop.Key).IsModified = true;
            }
        }
        /// <summary>
        /// get key members of entity class.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entityType"></param>
        /// <returns></returns>
        internal PropertyInfo[] GetKeys(DbContext context, Type entityType)
        {
            var objContext = ((IObjectContextAdapter)context).ObjectContext;
            var method = objContext.GetType().GetMethod("CreateObjectSet", new Type[] { });
            var generic = method.MakeGenericMethod(entityType);
            dynamic set = generic.Invoke(objContext, null);
            return ((IEnumerable<dynamic>)set.EntitySet.ElementType.KeyMembers).Select(_ => (PropertyInfo)entityType.GetProperty(_.Name)).ToArray();
        }
        /// <summary>
        /// check any default value in key members.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="keyProps"></param>
        /// <returns></returns>
        internal bool IsInitialKeys(object entity, PropertyInfo[] keyProps)
        {
            return !keyProps.Any(p => !p.GetValue(entity).Equals(Activator.CreateInstance(p.PropertyType)));
        }
    }
}
