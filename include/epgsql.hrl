%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午7:56
%%%-------------------------------------------------------------------
-author("root").
-record(column,
{name :: binary(),
  type :: epgsql:epgsql_type(),
  oid :: non_neg_integer(),
  size :: -1 | pos_integer(),
  modifier :: -1 | pos_integer(),
  format :: integer(),
  table_oid :: non_neg_integer(),
  table_attr_number :: pos_integer()}).

-record(statement,
{name :: string(),
  columns :: [#column{}],
  types :: [epgsql:epgsql_type()],
  parameter_info :: [epgsql_oid_db:oid_entry()]}).

-record(error,
{severity ::
debug |
log |
info |
notice |
warning |
error |
fatal |
panic,
  code :: binary(),
  codename :: atom(),
  message :: binary(),
  extra ::
  [{severity |
  detail |
  hint |
  position |
  internal_position |
  internal_query |
  where |
  schema_name |
  table_name |
  column_name |
  data_type_name |
  constraint_name |
  file |
  line |
  routine,
    binary()}]}).