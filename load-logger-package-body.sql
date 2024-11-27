create or replace package body load_logger is

/*
   Module: load_logger (body)
*/

procedure write_log (
   p_log_type          in varchar2,
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
) is
PRAGMA AUTONOMOUS_TRANSACTION;
begin
   insert into load_log (
      log_session_id,
      log_type,
      log_application,
      log_module,
      log_schema,
      log_object,
      log_context,
      log_body
   ) values (
      dbms_session.unique_session_id(),
      upper(p_log_type),
      p_log_application,
      p_log_module,
      p_log_schema,
      p_log_object,
      p_log_context,
      p_log_body
   );
   commit;
end write_log;

procedure info (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
) is
begin
   write_log(
      p_log_type => 'INFO',
      p_log_application => p_log_application,
      p_log_module => p_log_module,
      p_log_schema => p_log_schema,
      p_log_object => p_log_object,
      p_log_context => p_log_context,
      p_log_body => p_log_body
   );
end info;

procedure warning (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
) is
begin
   write_log(
      p_log_type => 'WARNING',
      p_log_application => p_log_application,
      p_log_module => p_log_module,
      p_log_schema => p_log_schema,
      p_log_object => p_log_object,
      p_log_context => p_log_context,
      p_log_body => p_log_body
   );
end warning;

procedure error (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
) is
begin
   write_log(
      p_log_type => 'ERROR',
      p_log_application => p_log_application,
      p_log_module => p_log_module,
      p_log_schema => p_log_schema,
      p_log_object => p_log_object,
      p_log_context => p_log_context,
      p_log_body => p_log_body
   );
end error;

procedure debug (
   p_log_application   in varchar2,
   p_log_module        in varchar2,
   p_log_schema        in varchar2 default null,
   p_log_object        in varchar2 default null,
   p_log_context       in varchar2 default null,
   p_log_body          in json
) is
begin
   write_log(
      p_log_type => 'DEBUG',
      p_log_application => p_log_application,
      p_log_module => p_log_module,
      p_log_schema => p_log_schema,
      p_log_object => p_log_object,
      p_log_context => p_log_context,
      p_log_body => p_log_body
   );
end debug;

end load_logger;
/
