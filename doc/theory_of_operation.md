The Azure Sentinel => Axiom exporter is built around the Azure Log Analytics Data Exporter feature set. Azure Sentinel is built on-top of Azure Log Analytics so all the data in Sentinel can be found there too. 

At a high level, Log Analytics Data Export will continuously export rows that are ingested into Azure Tables into Azure Storage blobs. 

```mermaid
flowchart LR
    AMA[Azure Monitor Agent] -->|HTTP Ingest| Ingest{{Ingestion Pipeline}}
    Cloudflare -->|Data Connector| Ingest
    AWSFlowLogs -->|Data Connector| Ingest 
    MSD[Microsoft Defender for Cloud Apps] -->|Data Connector| Ingest

    Ingest --> LogAnalytics(Azure Log Analytics)
    Ingest --> DataExport
    DataExport --> AzureStorage[(Azure Storage)]

    LogAnalytics --> AzureSentinel{{Azure Sentinel}}

    AzureStorage --> AxiomExporter{{Axiom Exporter Tool}} --> Axiom(Axiom) 
```

These blobs are then read by the exporter tool, which can then forward the rows to axiom for long term storage, then finally deleting the Azure Storage blob that was exported as it is no longer needed. 

# Continuous Data Export setup
## Setup a Unique Azure Storage Account
>[!warning]
>The tool *will* delete blobs in the storage account you setup after exporting said blob into axiom. Do not share this storage account with other export uses.   
referencing [this article](https://learn.microsoft.com/en-gb/azure/azure-monitor/logs/logs-data-export?tabs=portal#storage-account) you will need to setup a unique storage account to export your Log Analytics tables into. 
There are some requirements that the Storage Account must match
- Destinations must be in the same region as the Log Analytics workspace.
- The Storage Account must be unique across rules in the workspace.
- Export to Premium Storage Account isn't supported.
- The Storage Account must be StorageV1 or later.

## Setup a Log Analytics Data Export Rule
Referencing [this article](https://learn.microsoft.com/en-gb/azure/azure-monitor/logs/logs-data-export?tabs=portal#create-or-update-a-data-export-rule) you must create a data export rule to export the tables you select into the given storage account as blob storage. 
This is where you will select the tables you want to export, any tables not selected here will not be exported. All tables you select here will be exported by the export tool.
> [!note] 
>any _new_ tables you want to export will need to be added to this rule. It will not automatically pick up new tables.

## Setup an Axiom Personal Access Token 
>[!warning] 
>Personal access tokens allow access to any organisation and has all the permissions that your user has. 
API Token support is coming soon, but for testing a Personal Access Token may be used. 

## Deploy the Azure Sentinel Exporter tool 
TBD


# Azure Tables vs Custom Legacy Tables

![screenshot of azure tables alongside custom tables](tables.png)

Only Azure Tables can be exported by the Log Analytics Data Export, Custom (legacy) tables are not supported (for a full list see [this article](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tables-feature-support)) 

## Custom Legacy Table Exporting
Custom (legacy) tables can still be exported to axiom, but must go via the less efficient and more restricted Logic Apps mechanism to run periodic queries against your custom tables, which then egress the results to axiom. See [this article](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-export-logic-app) on exporting logs via logic apps

In addition, the Datasets matching the Tables you are exporting *must* already exist in axiom. The dataset name must match the table name exactly