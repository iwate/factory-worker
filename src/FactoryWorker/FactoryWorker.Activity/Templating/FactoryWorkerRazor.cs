using FactoryWorker.Activity.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using RazorEngine;
using RazorEngine.Configuration;
using RazorEngine.Templating;
using RazorEngine.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FactoryWorker.Activity.Templating
{
    public class FactoryWorkerRazor
    {
        const string DEFAULT = "Default";
        protected IDictionary<string, string> Templates { get; private set; }
        protected string SharedTemplate = "";

        public FactoryWorkerRazor()
        {
            Templates = new Dictionary<string, string>();
            var config = new TemplateServiceConfiguration
            {
                EncodedStringFactory = new RawStringFactory(),
                BaseTemplateType = typeof(FactoryWorkerTemplateBase),
                ReferenceResolver = new FactoryWorkerReferenceResolver(),
                TemplateManager = new DelegateTemplateManager(_ => Templates[_]),
                Debug = true
            };
            var service = RazorEngineService.Create(config);
            Engine.Razor = service;
        }
        public FactoryWorkerRazor(IEnumerable<CustomDbDataset> datasets, IActivityLogger logger) : this()
        {
            SharedTemplate = GenerateSharedTemplate(datasets);
            FactoryWorkerTemplateGlobals.Logger = logger;
        }
        public void AddTemplate(string name, string template)
        {
            Templates[name] = SharedTemplate + template;
        }
        public void Run(string name, object model, DynamicViewBag viewBag)
        {
            Engine.Razor.RunCompile(name, null, model, viewBag);
        }
        public void RunWithTemplate(string template, dynamic model, DynamicViewBag viewBag)
        {
            Templates[DEFAULT] = SharedTemplate + template;
            Run(DEFAULT, model, viewBag);
        }
        internal static string GenerateSharedTemplate(IEnumerable<CustomDbDataset> datasets)
        {
            string template = "";

            template += "@functions {\n";
            foreach (var _ in datasets)
            {
                template += $"protected {_.DbContextName} {_.InstanceName} {{ get {{ return ({_.DbContextName})Model.{_.InstanceName}; }} }}\n";
            }
            template += "}\n";

            return template;
        }
    }
}
