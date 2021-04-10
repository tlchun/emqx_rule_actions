%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午8:05
%%%-------------------------------------------------------------------
-author("root").
-record(file_info,
{size :: non_neg_integer() | undefined,
  type ::
  device |
  directory |
  other |
  regular |
  symlink |
  undefined,
  access :: read | write | read_write | none | undefined,
  atime ::
  file:date_time() | non_neg_integer() | undefined,
  mtime ::
  file:date_time() | non_neg_integer() | undefined,
  ctime ::
  file:date_time() | non_neg_integer() | undefined,
  mode :: non_neg_integer() | undefined,
  links :: non_neg_integer() | undefined,
  major_device :: non_neg_integer() | undefined,
  minor_device :: non_neg_integer() | undefined,
  inode :: non_neg_integer() | undefined,
  uid :: non_neg_integer() | undefined,
  gid :: non_neg_integer() | undefined}).

-record(file_descriptor,
{module :: module(), data :: term()}).