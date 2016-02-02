#FactoryWorker
FactoryWorker is a useful CustomActivity for [Azure DataFactory](https://azure.microsoft.com/documentation/services/data-factory/).
This library can connect SqlServer through EntityFramework and you can write `transform code` by using C# in Razor. That is like the following pipeline settings.
```yml
name: ExportPipeline
properties:
  activities:
    - name: FactoryWorkerActivity
      type: DotNetActivity
      inputs:
        - name: AdventureWorksLT
      outputs:
        - name: OutputDataset
      linkedServiceName: FactoryWorkerBatch
      policy:
        concurrency: 1
        executionPriorityOrder: OldestFirst
        retry: 3
        timeout: 00:30:00
        delay: 00:00:00
      typeProperties:
        assemblyName: FactoryWorker.Activity.dll
        entryPoint: FactoryWorker.Activity.FactoryWorkerActivity
        packageLinkedService: PackageStorage
        packageFile: <container>/FactoryWorker.Activity.zip
        extendedProperties:
          SliceStart: $$Text.Format('{0}', SliceStart)
          SliceEnd: $$Text.Format('{0}', SliceEnd)
          transform:|
            @{
              Logger("Transform Start");
              ViewBag.Export = AdventureWorksLT.SalesOrderHeaders
                      .Where(_ => _.OrderDate >= Slice.Start && _.OrderDate < Slice.End)
                      .Select(h => new {
                        SalesOrderID = h.SalesOrderID,
                        OrderDate = h.OrderDate,
                        DueDate = h.DueDate,
                        ShipDate = h.ShipDate,
                        Status = h.Status,
                        OnlineOrderFlag = h.OnlineOrderFlag,
                        SalesOrderNumber = h.SalesOrderNumber,
                        PurchaseOrderNumber = h.PurchaseOrderNumber,
                        AccountNumber = h.AccountNumber,
                        SubTotal = h.SubTotal,
                        TaxAmt = h.TaxAmt,
                        Freight= h.Freight,
                        TotalDue = h.TotalDue,
                        Details = h.SalesOrderDetails.Select(d => new {
                          SalesOrderDetailID = d.SalesOrderDetailID,
                          OrderQty = d.OrderQty,
                          ProductID = d.ProductID,
                          UnitPrice = d.UnitPrice,
                          UnitPriceDiscount = d.UnitPriceDiscount,
                          LineTotal = d.LineTotal
                        })
                      }).ToList();
            }
```

##How to use
This sample is that AdventureWorks's orders export to storage as json file.
1. Create environments: Azure DataFactories, Azure Batch, Azure Storage and SqlDatabase.(create SqlDatabase from AdventureWorksLT sample)
2. Download [FactoryWorker.Activity.zip]() and [FactoryWorker.Sample.dll]() (FacoryWorker.Sample.dll is DbContext lib for AdventureWorksLT)
3. Upload `FactoryWorker.Activity.zip` and `FactoryWorker.Sample.dll` to Azure Blob storage.
4. Create pool in Azure Batch
5. Deploy the following settings to Azure DataFactory.(convert yaml to json beforehand)

###LinkedServices
```yml
name: AdventureWorksDatabase
properties:
  type: AzureSqlDatabase
  typeProperties:
    connectionString: Data Source=<sqlsvr>.database.windows.net;Initial Catalog=<db>;User Id=<user>;Password=<password>;
```

```yml
name: Storage
properties:
  type: AzureStorage
  typeProperties:
    connectionString: BlobEndpoint=https://<account>.blob.core.windows.net/;AccountName=<account>;AccountKey=<key>
```

```yml
name: PackageStorage
properties:
  type: AzureStorage
  typeProperties:
    connectionString: BlobEndpoint=https://<account>.blob.core.windows.net/;AccountName=<account>;AccountKey=<key>
```

```yml
name: FactoryWorkerBatch
properties:
  type: AzureBatch
  typeProperties:
    accountName: <account>
    accessKey: <key>
    poolName: <pool>
    batchUri: https://<region>.batch.azure.com
    linkedServiceName: PackageStorage
```

###Datasets
```yml
name: AdventureWorksLT
properties:
  type: CustomDataset
  linkedServiceName: AdventureWorksDatabase
  typeProperties:
    type: EntityFramework
    instanceName: AdventureWorksLT
    dbContextName: FactoryWorker.Sample.SampleDbContext
    packageLinkedService: PackageStorage
    assemblyFile: <container>/FactoryWorker.Sample.dll
  availability:
    frequency: Hour
    interval: 1
```

```yml
name: OutputDataset
properties:
  type: CustomDataset
  linkedServiceName: Storage
  typeProperties:
    type: AzureBlob
    format: json
    filePath: <container>/outputs/{slice}.tsv
    slice: yyyy-MM-dd, SliceStart
    test: $$Text.Format('{yyyy-MM-dd}', SliceStart)
  availability:
    frequency: Hour
    interval: 1
```
 
###Pipeline
```yml
name: ExportPipeline
properties:
  activities:
    - name: FactoryWorkerActivity
      type: DotNetActivity
      inputs:
        - name: AdventureWorksLT
      outputs:
        - name: OutputDataset
      linkedServiceName: FactoryWorkerBatch
      policy:
        concurrency: 1
        executionPriorityOrder: OldestFirst
        retry: 3
        timeout: 00:30:00
        delay: 00:00:00
      typeProperties:
        assemblyName: FactoryWorker.Activity.dll
        entryPoint: FactoryWorker.Activity.FactoryWorkerActivity
        packageLinkedService: PackageStorage
        packageFile: <container>/FactoryWorker.Activity.zip
        extendedProperties:
          SliceStart: $$Text.Format('{0}', SliceStart)
          SliceEnd: $$Text.Format('{0}', SliceEnd)
          transform: |
            @{
              Logger("Transform Start");
              ViewBag.Export = AdventureWorksLT.SalesOrderHeaders
                      .Where(_ => _.OrderDate >= Slice.Start && _.OrderDate < Slice.End)
                      .Select(h => new {
                        SalesOrderID = h.SalesOrderID,
                        OrderDate = h.OrderDate,
                        DueDate = h.DueDate,
                        ShipDate = h.ShipDate,
                        Status = h.Status,
                        OnlineOrderFlag = h.OnlineOrderFlag,
                        SalesOrderNumber = h.SalesOrderNumber,
                        PurchaseOrderNumber = h.PurchaseOrderNumber,
                        AccountNumber = h.AccountNumber,
                        SubTotal = h.SubTotal,
                        TaxAmt = h.TaxAmt,
                        Freight= h.Freight,
                        TotalDue = h.TotalDue,
                        Details = h.SalesOrderDetails.Select(d => new {
                          SalesOrderDetailID = d.SalesOrderDetailID,
                          OrderQty = d.OrderQty,
                          ProductID = d.ProductID,
                          UnitPrice = d.UnitPrice,
                          UnitPriceDiscount = d.UnitPriceDiscount,
                          LineTotal = d.LineTotal
                        })
                      }).ToList();
            }
 ```
 
##Other samples

###Export to storage as csv
```yml
name: OutputDataset
properties:
  type: AzureBlob
  linkedServiceName: Storage
  typeProperties:
    folderPath: <container>/outputs/{slice}.tsv
    format:
      type: TextFormat
      rowDelimiter: \n
      columnDelimiter: \t
    partitionedBy: 
      - name: slice
        value:
          type: DateTime
          date: SliceStart
          format: yyyy-MM-dd
  availability:
    frequency: Hour
    interval: 1
```
```yml
name: ExportPipeline
properties:
  activities:
    - name: FactoryWorkerActivity
      type: DotNetActivity
      inputs:
        - name: AdventureWorksLT
      outputs:
        - name: OutputDataset
      linkedServiceName: FactoryWorkerBatch
      policy:
        concurrency: 1
        executionPriorityOrder: OldestFirst
        retry: 3
        timeout: 00:30:00
        delay: 00:00:00
      typeProperties:
        assemblyName: FactoryWorker.Activity.dll
        entryPoint: FactoryWorker.Activity.FactoryWorkerActivity
        packageLinkedService: PackageStorage
        packageFile: <container>/FactoryWorker.Activity.zip
        extendedProperties:
          SliceStart: $$Text.Format('{0}', SliceStart)
          SliceEnd: $$Text.Format('{0}', SliceEnd)
          transform: |
            @{
              Logger("Transform Start");
              ViewBag.Export = AdventureWorksLT.SalesOrderHeaders
                      .Where(_ => _.OrderDate >= Slice.Start && _.OrderDate < Slice.End)
                      .Select(h => new {
                        SalesOrderID = h.SalesOrderID,
                        OrderDate = h.OrderDate,
                        DueDate = h.DueDate,
                        ShipDate = h.ShipDate,
                        Status = h.Status,
                        OnlineOrderFlag = h.OnlineOrderFlag,
                        SalesOrderNumber = h.SalesOrderNumber,
                        PurchaseOrderNumber = h.PurchaseOrderNumber,
                        AccountNumber = h.AccountNumber,
                        SubTotal = h.SubTotal,
                        TaxAmt = h.TaxAmt,
                        Freight= h.Freight,
                        TotalDue = h.TotalDue
                      }).ToList();
            }
```

### Import from json 
```yml
name: InputDataset
properties:
  type: CustomDataset
  linkedServiceName: Storage
  typeProperties:
    type: AzureBlob
    format: json
    instanceName: Orders
    filePath: <container>/outputs/{slice}.tsv
    slice: yyyy-MM-dd, SliceStart
  availability:
    frequency: Hour
    interval: 1
```

```yml
name: ExportPipeline
properties:
  activities:
    - name: FactoryWorkerActivity
      type: DotNetActivity
      inputs:
        - name: InputDataset
      outputs:
        - name: AdventureWorksLT
      linkedServiceName: FactoryWorkerBatch
      policy:
        concurrency: 1
        executionPriorityOrder: OldestFirst
        retry: 3
        timeout: 00:30:00
        delay: 00:00:00
      typeProperties:
        assemblyName: FactoryWorker.Activity.dll
        entryPoint: FactoryWorker.Activity.FactoryWorkerActivity
        packageLinkedService: PackageStorage
        packageFile: <container>/FactoryWorker.Activity.zip
        extendedProperties:
          SliceStart: $$Text.Format('{0}', SliceStart)
          SliceEnd: $$Text.Format('{0}', SliceEnd)
          transform: |
            @{
              var orders = Model.Orders as IEnumerable<dynamic>;
              var details =  orders.SelectMany<dynamic,dynamic>(_ => _.Details).ToList();
              var headers = orders.Select(_ => new {
                SalesOrderID = (int)_.SalesOrderID,
                OrderDate = Convert.ToDateTime((string)_.OrderDate),
                DueDate = Convert.ToDateTime((string)_.DueDate),
                ShipDate = Convert.ToDateTime((string)_.ShipDate),
                Status = (byte)_.Status,
                OnlineOrderFlag = (bool)_.OnlineOrderFlag,
                SalesOrderNumber = (string)_.SalesOrderNumber,
                PurchaseOrderNumber = (string)_.PurchaseOrderNumber,
                AccountNumber = (string)_.AccountNumber,
                SubTotal = (decimal)_.SubTotal,
                TaxAmt = (decimal)_.TaxAmt,
                Freight= (decimal)_.Freight,
                TotalDue = (decimal)_.TotalDue
              });
              ViewBag.Exports = new {
                SalesOrderHeaders = headers,
                SalesOrderDetails = details
              };
            }
```

## Usable member of razor template
- Slice: { Start: `DateTime`, End: `DateTime` }
- Model: { [key]: `IEnumerable<dynamic>` } (key := `instanceName` or `name` of Input Datasets)
- { [db]: `T extends DbContext` } (db := `instanceName` of EntityFramework Input Datasets)

## How to load custom assemblies 
If you use other library in razor. You upload dll in zip file together and you modify `assemblies.xml`

```xml
<?xml version="1.0" encoding="utf-8" ?>
<Assemblies>
  <add name="EntityFramework.dll" />
  <add name="EntityFramework.SqlServer.dll" />
  <!-- if you use other library in razor, add here and libraries are zip together.-->
</Assemblies>
```