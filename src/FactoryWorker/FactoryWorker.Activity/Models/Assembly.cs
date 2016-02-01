using System.Collections.Generic;
using System.Xml.Serialization;

namespace FactoryWorker.Activity.Models
{
    [XmlType("add")]
    public class LoadAssembly
    {
        [XmlAttribute("name")]
        public string Name { get; set; }
    }
    [XmlRoot("Assemblies")]
    public class LoadAssemblies : List<LoadAssembly> { }
}
