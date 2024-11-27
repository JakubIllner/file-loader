create or replace package load_json is

/*
   Module: load_json (header)

   Purpose:
      PLSQL library for reading JSON documents from object store to target JSON Collection
      Table. The package expects the source JSON files are stored in object store in JSON
      Lines format, with every JSON document on a single line. One file may contain one or
      more JSON documents, separated by newlines. The library was tested with ADB
      Serverless 23ai and OCI Object Storage.

   Procedures:
      into_collection_direct()  -- Load JSON documents directly into target collection, using External table with jsondoc file format
      into_collection_2steps()  -- Load JSON documents first to staging Collection with DBMS_CLOUD.COPY_COLLECTION, and then insert into target collection

   Parameters:
      p_credential           -- Credential to authorize ADB to access files in object store
      p_source_file_uri_list -- Location of object store files to be loaded, with optional wildcards
      p_source_date          -- Date used for files partitioned by date (default sysdate)
      p_source_date_var      -- Variable used for substitution of date in file location (default ${date})
      p_source_date_mask     -- Date mask used for substitution of date in file location (default YYYY-MM-DD)
      p_target_schema        -- Schema of the target collection
      p_target_collection    -- Target collection name

   Location substitution:
      If parameter p_source_date is not null, variable p_source_date_var in
      p_source_file_uri_list is substituded by p_source_date, using the mask
      p_source_date_mask.

   JSON document transformation:
      The JSON document from source files is extended by runtime metadata as follows:

      {
         "document_date": "value of p_source_date in YYYY-MM-DD format",
         "inserted_timestamp": "insert timestamp in YYYY-MM-DD"T"HH24:MI:SS.FF3 format",
         "document_body": { JSON document from source file }
      }
*/

procedure into_collection_direct (
   p_credential in varchar2,
   p_source_file_uri_list in varchar2,
   p_source_date in date default trunc(sysdate),
   p_source_date_var in varchar2 default '${date}',
   p_source_date_mask in varchar2 default 'YYYY-MM-DD',
   p_target_schema in varchar2,
   p_target_collection in varchar2
);

procedure into_collection_2steps (
   p_credential in varchar2,
   p_source_file_uri_list in varchar2,
   p_source_date in date default trunc(sysdate),
   p_source_date_var in varchar2 default '${date}',
   p_source_date_mask in varchar2 default 'YYYY-MM-DD',
   p_target_schema in varchar2,
   p_target_collection in varchar2
);

end load_json;
/

