create or replace package load_logger is

/*
   Module: load_logger (body)

   Purpose:
      PLSQL library for writing log messages into LOAD_LOG table. The log messages are
      written as autonomous transactions, out of scope of the calling transaction. The
      purpose is to persist log messages even when the calling transactions is rolled
      back. Log records consist of common attributes and arbitrary log message in JSON
      format.

   Procedures:
      write_log() -- Write log record into LOAD_LOG table
      info()      -- Write log record of type INFO
      warning()   -- Write log record of type WARNING
      error()     -- Write log record of type ERROR
      debug()     -- Write log record of type DEBUG

   Parameters:
      p_log_type        -- Type of log record (INFO|WARNING|ERROR|DEBUG)
      p_log_application -- Application name
      p_log_module      -- Module name
      p_log_schema      -- Schema of the target object (optional)
      p_log_object      -- Target object name (optional)
      p_log_context     -- Additional context, such as partitioning key (optional)
      p_log_body        -- Log message in the JSON format
*/

procedure write_log (
   p_log_type          in varchar2,
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
);

procedure info (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
);

procedure warning (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
);

procedure error (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
);

procedure debug (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
);

end load_logger;
/
