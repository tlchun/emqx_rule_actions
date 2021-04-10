%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午7:01
%%%-------------------------------------------------------------------
-module(emqx_rule_actions_utils).
-author("root").

-export([save_upload_file/2,
  get_ssl_opts/2,
  is_resource_alive/2,
  fetch_opts/2,
  parse_server/2,
  safe_exec/2,
  exec/2,
  split_insert_sql/1,
  batch_sql_insert/2,
  start_pool/3,
  stop_pool/1,
  start_batcher_pool/4,
  stop_batcher_pool/1]).


fetch_opts(Keys, OrgOpts) ->
  fetch_opts(Keys, OrgOpts, #{}).

fetch_opts([], _Opts, ResOpt) -> ResOpt;
fetch_opts([{Name, Trans} | Keys], Opts, ResOpt)
  when is_function(Trans) ->
  NewResOpt = case maps:find(Name, Opts) of
                error -> ResOpt;
                {ok, Val} ->
                  {Key, Val2} = Trans(Val),
                  ResOpt#{Key => Val2}
              end,
  fetch_opts(Keys, Opts, NewResOpt);
fetch_opts([{Name, Key} | Keys], Opts, ResOpt) ->
  NewResOpt = case maps:find(Name, Opts) of
                error -> ResOpt;
                {ok, Val} -> ResOpt#{Key => Val}
              end,
  fetch_opts(Keys, Opts, NewResOpt).

save_upload_file(#{<<"file">> := <<>>,
  <<"filename">> := <<>>},
    _ResId) ->
  "";
save_upload_file(FilePath, _)
  when is_binary(FilePath) ->
  binary_to_list(FilePath);
save_upload_file(#{<<"file">> := File,
  <<"filename">> := FileName},
    ResId) ->
  FullFilename = filename:join([emqx:get_env(data_dir),
    rules,
    ResId,
    FileName]),
  ok = filelib:ensure_dir(FullFilename),
  case file:write_file(FullFilename, File) of
    ok -> binary_to_list(FullFilename);
    {error, Reason} ->
      logger:error("Store file failed, ResId: ~p, ~0p",
        [ResId, Reason]),
      error({ResId, store_file_fail})
  end;
save_upload_file(_, _) -> "".

get_ssl_opts(Opts, ResId) ->
  KeyFile = maps:get(<<"keyfile">>, Opts, undefined),
  CertFile = maps:get(<<"certfile">>, Opts, undefined),
  CAFile = case maps:get(<<"cacertfile">>,
    Opts,
    undefined)
           of
             undefined -> maps:get(<<"cafile">>, Opts, undefined);
             CAFile0 -> CAFile0
           end,
  Filter = fun (Opts1) ->
    [{K, V}
      || {K, V} <- Opts1, V =/= undefined, V =/= <<>>,
      V =/= ""]
           end,
  Key = save_upload_file(KeyFile, ResId),
  Cert = save_upload_file(CertFile, ResId),
  CA = save_upload_file(CAFile, ResId),
  Verify = case maps:get(<<"verify">>, Opts, false) of
             false -> verify_none;
             true -> verify_peer
           end,
  case Filter([{keyfile, Key},
    {certfile, Cert},
    {cacertfile, CA}])
  of
    [] -> [{verify, Verify}];
    SslOpts -> [{verify, Verify} | SslOpts]
  end.

-spec is_resource_alive(Name :: atom(),
    Handler :: fun((pid()) -> ok | error)) -> boolean().

is_resource_alive(Name, Handler) ->
  Status = [begin
              {ok, Client} = ecpool_worker:client(Worker),
              Handler(Client)
            end
    || {_, Worker} <- ecpool:workers(Name)],
  length(Status) > 0 andalso
    lists:all(fun (St) -> St =:= ok end, Status).

parse_ip(Host) when is_binary(Host) ->
  parse_ip(binary_to_list(Host));
parse_ip(Host) when is_list(Host) ->
  case inet:parse_address(Host) of
    {ok, IpAddr} -> IpAddr;
    _ -> Host
  end.

parse_server(Server, DefaultPort)
  when is_binary(Server) ->
  case string:split(Server, ":", trailing) of
    [Host, Port] ->
      {parse_ip(Host), binary_to_integer(Port)};
    [Host] -> {parse_ip(Host), DefaultPort}
  end.

safe_exec(Action, MainArgs) when is_list(MainArgs) ->
  try exec(Action, MainArgs) catch
    E:R:ST ->
      logger:error("[RuleActions] safe_exec ~p, failed: "
      "~0p",
        [Action, {E, R, ST}]),
      {error, {exec_failed, E, R}}
  end.

exec({M, F, A}, MainArgs) ->
  erlang:apply(M, F, MainArgs ++ A);
exec(Action, MainArgs) when is_function(Action) ->
  erlang:apply(Action, MainArgs).

split_insert_sql(SQL) ->
  case re:split(SQL, "((?i)values)", [{return, binary}])
  of
    [Part1, _, Part3] ->
      case string:trim(Part1, leading) of
        <<"insert", _/binary>> = InsertSQL ->
          {ok, {InsertSQL, Part3}};
        <<"INSERT", _/binary>> = InsertSQL ->
          {ok, {InsertSQL, Part3}};
        _ -> {error, not_insert_sql}
      end;
    _ -> {error, not_insert_sql}
  end.

batch_sql_insert(Batch, InsertFun) ->
  AggrBatch = maps:to_list(lists:foldl(fun ({From,
    {K, V}},
      Acc0) ->
    L = maps:get(K, Acc0, []),
    Acc0#{K => [{From, V} | L]}
                                       end,
    #{},
    Batch)),
  lists:flatmap(fun ({SQLInsertPart, FromAndValList}) ->
    {FromList, SQLParamPartList} =
      lists:unzip(FromAndValList),
    SQL = insert_batch_sql(SQLInsertPart,
      SQLParamPartList),
    Result = safe_exec(InsertFun,
      [SQL, length(SQLParamPartList)]),
    [{From, Result} || From <- FromList]
                end,
    AggrBatch).

start_pool(Name, Mod, Options) ->
  case ecpool:start_sup_pool(Name, Mod, Options) of
    {ok, _} ->
      logger:log(info, "Initiated ~0p Successfully", [Name]);
    {error, {already_started, _Pid}} ->
      stop_pool(Name),
      start_pool(Name, Mod, Options);
    {error, Reason} ->
      logger:log(error,
        "Initiate ~0p failed ~0p",
        [Name, Reason]),
      error({start_pool_failed, Name})
  end.

stop_pool(Name) ->
  case ecpool:stop_sup_pool(Name) of
    ok ->
      logger:log(info, "Destroyed ~0p Successfully", [Name]);
    {error, Reason} ->
      logger:log(error,
        "Destroy ~0p failed, ~0p",
        [Name, Reason]),
      error({stop_pool_failed, Name})
  end.

insert_batch_sql(SQLInsertPart, SQLParamPartList) ->
  SQLParamPartStr = values(SQLParamPartList),
  <<SQLInsertPart/binary, " VALUES ",
    SQLParamPartStr/binary>>.

values(ValuesSQL) -> values(ValuesSQL, <<>>).

values([], Acc) -> Acc;
values([ValuesSQL | Rest], Acc) ->
  NewValuesSQL = re:replace(ValuesSQL,
    ";$",
    "",
    [{return, binary}, unicode]),
  NewAcc = case Rest == [] of
             true -> <<Acc/binary, NewValuesSQL/binary>>;
             false -> <<Acc/binary, NewValuesSQL/binary, ", ">>
           end,
  values(Rest, NewAcc).

start_batcher_pool(Name, CbMod, Options, State) ->
  Keys = [{<<"batch_size">>, batch_size},
    {<<"batch_time">>, batch_time},
    {<<"insert_mode">>, mode}],
  Options0 = [{pool_size, 4},
    {pool, Name},
    {batch_cbmod, CbMod},
    {batch_opts,
      emqx_rule_actions_utils:fetch_opts(Keys, Options)},
    {batch_state, State}],
  start_pool(Name, emqx_rule_actions_batcher, Options0).

stop_batcher_pool(Name) -> stop_pool(Name).
