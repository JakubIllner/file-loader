# __file-loader__

## Purpose

The purpose of the `file-loader` repository is to provide sample programs for loading of
files from object store such as OCI Object Storage to Oracle Autonomous Database. The
programs may be used for automation of the data loading process.


## Prerequisites

* Instance of Oracle Autonomous Database Serverless service with Oracle Database 23ai.
* Database schema granted `CONNECT` and `DWROLE` privileges.
* Credential (such as `OCI$RESOURCE_PRINCIPAL`) allowing access to OCI Object Storage bucket with data.


## Programs

* __LOAD_LOGGER__ - PLSQL package for writing log messages into LOAD_LOG table. The log
messages are written as autonomous transactions and persisted even when the calling
transaction is rolled back.

* __JSON_LOAD__ - PLSQL package for loading of JSON documents from an object store into a
JSON Collection Table. The package expects the source JSON files are stored in object
store in JSON Lines format, with every JSON document on a single line.


## LOAD_LOGGER

`LOAD_LOGGER` is a PLSQL library (package) for writing log messages into LOAD_LOG table.
The log messages are written as autonomous transactions, out of scope of the calling
transaction. The purpose is to persist log messages even when the calling transaction is
rolled back. Log records consist of common attributes and arbitrary log message in JSON
format.

### Procedures and Functions

* `write_log()` writes log record into LOAD_LOG table.
* `info()` writes log record of type INFO.
* `warning()` writes log record of type WARNING.
* `error()` writes log record of type ERROR.
* `debug()` writes log record of type DEBUG.

### Parameters

* `p_log_type` - Type of log record (INFO|WARNING|ERROR|DEBUG)
* `p_log_application` - Application name
* `p_log_module` - Module name
* `p_log_schema` - Schema of the target object (optional)
* `p_log_object` - Target object name (optional)
* `p_log_context` - Additional context, such as partitioning key (optional)
* `p_log_body` - Log message in the JSON format

### Installation

* Create `LOAD_LOG` table by running script [create-load-log-table.sql](create-load-log-table.sql).
* Create PLSQL package header `LOAD_LOGGER` by running script [load-logger-package-header.sql](load-logger-package-header.sql).
* Create PLSQL package body `LOAD_LOGGER` by running script [load-logger-package-body.sql](load-logger-package-body.sql).


## JSON_LOAD

`JSON_LOAD` is a PLSQL library (package) for reading JSON documents from object store and
inserting them to target JSON Collection Table. The package expects the source JSON files
are stored in object store in JSON Lines format, with every JSON document on a single
line. One file may contain one or more JSON documents, separated by newlines.

### Procedures and Functions

* `into_collection_direct()` loads JSON documents directly into target collection, using External table with jsondoc file format.
* `into_collection_2steps()` loads JSON documents first to staging Collection with DBMS_CLOUD.COPY_COLLECTION, and then it inserts documents into the target collection.

### Parameters

* `p_credential` - Credential to authorize ADB to access files in object store
* `p_source_file_uri_list` - Location of object store files to be loaded, with optional wildcards
* `p_source_date` - Date used for files partitioned by date (default sysdate)
* `p_source_date_var` - Variable used for substitution of date in file location (default ${date})
* `p_source_date_mask` - Date mask used for substitution of date in file location (default YYYY-MM-DD)
* `p_target_schema` - Schema of the target collection
* `p_target_collection` - Target collection name

### Location substitution

Text defined by parameter `p_source_date_var` in `p_source_file_uri_list` is substituded
by `p_source_date`, using the mask `p_source_date_mask`. If the text is not found, no
substitution takes place.

Example of parameter values:
```
p_source_file_uri_list => 'https://objectstorage.uk-london-1.oraclecloud.com/n/<namespace>/b/invoice-data/o/date=${date}/invoice-*.json',
p_source_date => to_date('2024-09-01','YYYY-MM-DD'),
p_source_date_var => '${date}',
p_source_date_mask => '${YYYYMMDD}'
```

For the above parameters, the following files will be loaded:
```
file_uri_list => 'https://objectstorage.uk-london-1.oraclecloud.com/n/<namespace>/b/invoice-data/o/date=20240901/invoice-*.json'
```

### JSON document transformation:

JSON documents from source files will be extended by runtime metadata `document_date` and
`inserted_timestamp` as follows:

```
{
   "document_date": "value of p_source_date in YYYY-MM-DD format",
   "inserted_timestamp": "insert timestamp in YYYY-MM-DD"T"HH24:MI:SS.FF3 format",
   "document_body": { JSON document from source file }
}
```

### Installation

* Create PLSQL package header `JSON_LOAD` by running script [load-json-package-header.sql](load-json-package-header.sql).
* Create PLSQL package body `JSON_LOAD` by running script [load-json-package-body.sql](load-json-package-body.sql).

