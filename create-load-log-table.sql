
drop table if exists load_log purge
/

create table load_log (
  log_id             number generated always as identity not null,
  log_timestamp      timestamp default systimestamp not null,
  log_session_id     varchar2(200) not null,
  log_type           varchar2(20) not null,
  log_application    varchar2(200) not null,
  log_module         varchar2(200) not null,
  log_schema         varchar2(200),
  log_object         varchar2(200),
  log_context        varchar2(200),
  log_body           json(object),
  constraint load_log_pk primary key (log_id),
  constraint load_log_type_ck check (log_type in ('ERROR', 'WARNING', 'INFO', 'DEBUG'))
)
/

comment on table load_log is 'Table storing log records from data loading operations';

comment on column load_log.log_id             is 'Unique identity column, automatically generated';
comment on column load_log.log_timestamp      is 'Timestamp when the log record was created';
comment on column load_log.log_session_id     is 'Unique identifier of the database session';
comment on column load_log.log_type           is 'Type of the log record (INFO|WARNING|ERROR|DEBUG)';
comment on column load_log.log_application    is 'Application that produced the log record';
comment on column load_log.log_module         is 'Module that produced the log record';
comment on column load_log.log_schema         is 'Schema of the target object (optional)';
comment on column load_log.log_object         is 'Target object name (optional)';
comment on column load_log.log_context        is 'Additional context, such as partitioning key (optional)';
comment on column load_log.log_body           is 'Log message in the JSON(object) format';

