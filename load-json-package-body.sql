create or replace package body load_json is
/*
   Module: load_json (header)
*/


   v_application varchar2(100) := 'LOAD_JSON';


/*
   Function: get_timestamp_diff()
   Purpose: Return difference between two timestamps in seconds.
*/
function get_timestamp_diff (
   p_end_ts in timestamp,
   p_start_ts in timestamp
) return number is
   v_diff interval day to second := p_end_ts - p_start_ts;
begin
   return
      extract(day from v_diff)*24*60*60 +
      extract(hour from v_diff)*60*60 +
      extract(minute from v_diff)*60 +
      extract(second from v_diff);
end get_timestamp_diff;


/*
   Procedure: into_collection_direct()
   Purpose: Load JSON documents directly into target collection, using External table with jsondoc file format.
*/
procedure into_collection_direct (
   p_credential in varchar2,
   p_source_file_uri_list in varchar2,
   p_source_date in date default trunc(sysdate),
   p_source_date_var in varchar2 default '${date}',
   p_source_date_mask in varchar2 default 'YYYY-MM-DD',
   p_target_schema in varchar2,
   p_target_collection in varchar2
) is
   v_module varchar2(100) := 'INTO_COLLECTION_DIRECT';
   v_context varchar2(100) := to_char(p_source_date,'YYYY-MM-DD');
   v_file_uri_list varchar2(4000) := replace(p_source_file_uri_list,'${date}',to_char(p_source_date,p_source_date_mask));
   v_target_collection_name varchar2(100) := p_target_schema||'.'||p_target_collection;
   v_start_timestamp timestamp := systimestamp;
   v_end_timestamp timestamp;
   v_insert_counter number;
   v_sql_statement clob;
begin
   /*
      Log the start time and parameters
   */
   load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
      'message' value 'load started'
      returning json
   ));
   load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
      'message' value 'parameters',
      'p_credential' value p_credential,
      'p_source_file_uri_list' value p_source_file_uri_list,
      'p_source_date' value to_char(p_source_date, p_source_date_mask),
      'p_target_schema' value p_target_schema,
      'p_target_collection' value p_target_collection
      returning json
   ));
   /*
      Insert into target collection
   */
   begin
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'inserting into target collection',
         'collection' value v_target_collection_name,
         'file_uri_list' value v_file_uri_list
         returning json
      ));
      v_sql_statement :=
         'insert into '||v_target_collection_name||'
          select json_object(
             ''document_date'' value '''||to_char(p_source_date,'YYYY-MM-DD')||''',
             ''inserted_timestamp'' value '''||to_char(v_start_timestamp,'YYYY-MM-DD"T"HH24:MI:SS.FF3')||''',
             ''document_body'' value data
             returning json
          )
          from external (
              (data json)
              type ORACLE_BIGDATA
              access parameters (
                  com.oracle.bigdata.fileformat = jsondoc
                  com.oracle.bigdata.credential.name = '||p_credential||'
              )
              location ('''||v_file_uri_list||''')
          )';
      execute immediate v_sql_statement;
      v_insert_counter := sql%rowcount;
      commit;
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'success inserting into target collection',
         'collection' value v_target_collection_name,
         'inserted_records' value v_insert_counter
         returning json
      ));
      v_end_timestamp := systimestamp;
   exception
      when others then
         load_logger.error(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
            'message' value 'error inserting into target collection',
            'collection' value v_target_collection_name,
            'error_code' value sqlcode,
            'error_message' value sqlerrm
            returning json
         ));
         raise;
   end;
   /*
      Log the end time and parameters
   */
   load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
      'message' value 'load finished',
      'status' value 'SUCCESS',
      'inserted_records' value v_insert_counter,
      'total_seconds' value get_timestamp_diff(v_end_timestamp,v_start_timestamp),
      'load_seconds' value get_timestamp_diff(v_end_timestamp,v_start_timestamp)
      returning json
   ));
   return;
end into_collection_direct;


/*
   Procedure: into_collection_2steps()
   Purpose: Load JSON documents first to staging Collection with DBMS_CLOUD.COPY_COLLECTION, and then insert into target collection.
*/
procedure into_collection_2steps (
   p_credential in varchar2,
   p_source_file_uri_list in varchar2,
   p_source_date in date default trunc(sysdate),
   p_source_date_var in varchar2 default '${date}',
   p_source_date_mask in varchar2 default 'YYYY-MM-DD',
   p_target_schema in varchar2,
   p_target_collection in varchar2
) is
   v_module varchar2(100) := 'INTO_COLLECTION_2STEPS';
   v_context varchar2(100) := to_char(p_source_date,'YYYY-MM-DD');
   v_file_uri_list varchar2(4000) := replace(p_source_file_uri_list,'${date}',to_char(p_source_date,p_source_date_mask));
   v_target_collection_name varchar2(100) := p_target_schema||'.'||p_target_collection;
   v_stage_collection_name varchar2(100) := p_target_collection||'_STG';
   v_log_table_name varchar2(100);
   v_stage_collection Soda_Collection_T;
   v_start_timestamp timestamp := systimestamp;
   v_stage_timestamp timestamp;
   v_filecount_timestamp timestamp;
   v_end_timestamp timestamp;
   v_operation_id number;
   v_result number;
   v_insert_counter number;
   v_file_counter number;
   v_format clob;
   v_sql_statement clob;
begin
   /*
      Log the start time and parameters
   */
   load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
      'message' value 'load started'
      returning json
   ));
   load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
      'message' value 'parameters',
      'p_credential' value p_credential,
      'p_source_file_uri_list' value p_source_file_uri_list,
      'p_source_date' value to_char(p_source_date, p_source_date_mask),
      'p_target_schema' value p_target_schema,
      'p_target_collection' value p_target_collection
      returning json
   ));
   /*
      create the staging collection
   */
   begin
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'creating staging collection',
         'collection' value v_stage_collection_name
         returning json
      ));
      v_sql_statement := 'drop table if exists '||v_stage_collection_name;
      execute immediate v_sql_statement;
      v_sql_statement := 'create json collection table '||v_stage_collection_name;
      execute immediate v_sql_statement;
   exception
      when others then
         load_logger.error(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
            'message' value 'error creating staging collection',
            'collection' value v_stage_collection_name,
            'error_code' value sqlcode,
            'error_message' value sqlerrm
            returning json
         ));
         raise;
   end;
   /*
      Load documents into staging collection
   */
   begin
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'loading staging collection',
         'collection' value v_stage_collection_name,
         'file_uri_list' value v_file_uri_list
         returning json
      ));
      v_format :=
        '{
           "characterset" : "AL32UTF8",
           "ignoreblanklines" : "true",
           "rejectlimit" : "10000",
           "unpackarrays" : "true",
           "maxdocsize" : 64000000,
           "regexuri" : "false"
         }';
      dbms_cloud.copy_collection (
         collection_name => v_stage_collection_name,
         credential_name => p_credential,
         file_uri_list => v_file_uri_list,
         format => v_format,
         operation_id => v_operation_id
      );
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'success loading staging collection',
         'collection' value v_stage_collection_name,
         'operation_id' value v_operation_id
         returning json
      ));
      v_stage_timestamp := systimestamp;
   exception
      when others then
         v_stage_timestamp := systimestamp;
         /*
            Exit if no files found
         */
         if (sqlerrm like 'ORA-20000: KUP-05002:%') then
            load_logger.warning(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
               'message' value 'no files found',
               'file_uri_list' value v_file_uri_list,
               'error_code' value sqlcode,
               'error_message' value sqlerrm
               returning json
            ));
            load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
               'message' value 'load finished',
               'status' value 'WARNING',
               'loaded_files' value 0,
               'inserted_records' value 0,
               'total_seconds' value get_timestamp_diff(v_stage_timestamp,v_start_timestamp),
               'load_seconds' value get_timestamp_diff(v_stage_timestamp,v_start_timestamp),
               'insert_seconds' value 0
               returning json
            ));
            return;
         else
            load_logger.error(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
               'message' value 'error loading staging collection',
               'collection' value v_stage_collection_name,
               'file_uri_list' value v_file_uri_list,
               'error_code' value sqlcode,
               'error_message' value sqlerrm
               returning json
            ));
            raise;
         end if;
   end;
   /*
      Get number of loaded files from log table
   */
   begin
      v_log_table_name := 'COPY$'||v_operation_id||'_LOG';
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'reading log table',
         'table' value v_log_table_name
         returning json
      ));
      v_sql_statement :=
         'select /*+ NO_PARALLEL */ count(*)
          from '||v_log_table_name||'
          where record like ''Data File:%''';
      execute immediate v_sql_statement into v_file_counter;
      v_filecount_timestamp := systimestamp;
   exception
      when others then
         load_logger.error(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
            'message' value 'error reading log table',
            'table' value v_log_table_name,
            'error_code' value sqlcode,
            'error_message' value sqlerrm
            returning json
         ));
         raise;
   end;
   /*
      Insert into target collection
   */
   begin
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'inserting into target collection',
         'collection' value v_target_collection_name
         returning json
      ));
      v_sql_statement :=
         'insert into '||v_target_collection_name||'
          select json_object(
             ''document_date'' value '''||to_char(p_source_date,'YYYY-MM-DD')||''',
             ''inserted_timestamp'' value '''||to_char(v_start_timestamp,'YYYY-MM-DD"T"HH24:MI:SS.FF3')||''',
             ''document_body'' value json_transform(data, remove ''$._id'')
             returning json
          )
          from '||v_stage_collection_name;
      execute immediate v_sql_statement;
      v_insert_counter := sql%rowcount;
      commit;
      load_logger.debug(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
         'message' value 'success inserting into target collection',
         'collection' value v_target_collection_name,
         'inserted_records' value v_insert_counter
         returning json
      ));
   exception
      when others then
         load_logger.error(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
            'message' value 'error inserting into target collection',
            'collection' value v_target_collection_name,
            'error_code' value sqlcode,
            'error_message' value sqlerrm
            returning json
         ));
         raise;
   end;
   /*
      Drop the staging collection
   */
   begin
      v_sql_statement := 'drop table if exists '||v_stage_collection_name;
      execute immediate v_sql_statement;
      v_end_timestamp := systimestamp;
   exception
      when others then
         load_logger.error(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
            'message' value 'error dropping staging collection',
            'collection' value v_stage_collection_name,
            'error_code' value sqlcode,
            'error_message' value sqlerrm
            returning json
         ));
         raise;
   end;
   /*
      Log the end time and parameters
   */
   load_logger.info(v_application,v_module,p_target_schema,p_target_collection,v_context,json_object(
      'message' value 'load finished',
      'status' value 'SUCCESS',
      'loaded_files' value v_file_counter,
      'inserted_records' value v_insert_counter,
      'total_seconds' value get_timestamp_diff(v_end_timestamp,v_start_timestamp),
      'load_seconds' value get_timestamp_diff(v_stage_timestamp,v_start_timestamp),
      'filecount_seconds' value get_timestamp_diff(v_filecount_timestamp,v_stage_timestamp),
      'insert_seconds' value get_timestamp_diff(v_end_timestamp,v_filecount_timestamp)
      returning json
   ));
   return;
end into_collection_2steps;


end load_json;
/
