using FactoryWorker.Activity.Models;
using RazorEngine.Compilation.ReferenceResolver;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml.Serialization;
using RazorEngine.Compilation;

namespace FactoryWorker.Activity.Templating
{
    public class FactoryWorkerReferenceResolver : IReferenceResolver
    {
        public IEnumerable<CompilerReference> GetReferences(TypeContext context, IEnumerable<CompilerReference> includeAssemblies = null)
        {
            var setupAssemblies = new List<CompilerReference>();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var needAssemblies = Load().Select(_ => Assembly.LoadFile(Path.Combine(binPath, _.Name))).Select(_ => CompilerReference.From(_.Location)).ToArray();
            setupAssemblies.AddRange(needAssemblies);

            var loadedAssemblies = (new UseCurrentAssembliesReferenceResolver()).GetReferences(context, includeAssemblies);
            setupAssemblies.AddRange(loadedAssemblies);

            return setupAssemblies.Distinct();
        }

        const string LOAD_ASSEMBLIES_CONFIG = "assemblies.xml";
        internal LoadAssemblies Load()
        {
            var path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), LOAD_ASSEMBLIES_CONFIG);
            var fs = new FileStream(path, FileMode.Open);
            var serializer = new XmlSerializer(typeof(LoadAssemblies));
            return (LoadAssemblies)serializer.Deserialize(fs);
        }

    }
}
