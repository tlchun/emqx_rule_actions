%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:58
%%%-------------------------------------------------------------------
-module(emqx_backend_dynamo_actions).
-author("root").
-export([logger_header/0]).
-include("../include/emqx.hrl").
-behaviour(ecpool_worker).
-include("../include/logger.hrl").
-include("../include/erlcloud_aws.hrl").
-include("../include/rule_actions.hrl").



-import(emqx_rule_utils, [str/1]).

-export([connect/1]).

-export([on_resource_create/2,
  on_get_resource_status/2,
  on_resource_destroy/2]).

-export([on_action_create_data_to_dynamo/2,
  on_action_data_to_dynamo/2]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en =>
  <<68, 121, 110, 97, 109, 111, 68, 66, 32, 82, 101,
    115, 111, 117, 114, 99, 101>>,
    zh =>
    <<68, 121, 110, 97, 109, 111, 68, 66, 32, 232, 181,
      132, 230, 186, 144>>},
  destroy => on_resource_destroy, name => backend_dynamo,
  params =>
  #{auto_reconnect =>
  #{default => 2,
    description =>
    #{en =>
    <<65, 117, 116, 111, 32, 82, 101, 99,
      111, 110, 110, 101, 99, 116, 32, 84,
      105, 109, 101, 115, 32, 102, 111, 114,
      32, 68, 121, 110, 97, 109, 111, 68, 66,
      32, 67, 108, 105, 101, 110, 116>>,
      zh =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        231, 154, 132, 232, 135, 170, 229, 138,
        168, 233, 135, 141, 232, 191, 158, 233,
        151, 180, 233, 154, 148>>},
    order => 6,
    title =>
    #{en =>
    <<65, 117, 116, 111, 32, 82, 101, 99,
      111, 110, 110, 101, 99, 116, 32, 84,
      105, 109, 101, 115>>,
      zh =>
      <<232, 135, 170, 229, 138, 168, 233, 135,
        141, 232, 191, 158, 233, 151, 180, 233,
        154, 148>>},
    type => number},
    aws_access_key_id =>
    #{default =>
    <<97, 119, 115, 95, 97, 99, 99, 101, 115, 115,
      95, 107, 101, 121, 95, 105, 100>>,
      description =>
      #{en =>
      <<65, 99, 99, 101, 115, 115, 32, 75, 101,
        121, 32, 73, 100, 32, 102, 111, 114,
        32, 99, 111, 110, 110, 101, 99, 116,
        105, 110, 103, 32, 116, 111, 32, 68,
        121, 110, 97, 109, 111, 68, 66>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          231, 154, 132, 232, 174, 191, 233, 151,
          174, 32, 73, 68>>},
      order => 4, required => true,
      title =>
      #{en =>
      <<65, 87, 83, 32, 65, 99, 99, 101, 115,
        115, 32, 75, 101, 121, 32, 73, 100>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 232, 174,
          191, 233, 151, 174, 32, 73, 68>>},
      type => string},
    aws_secret_access_key =>
    #{default =>
    <<97, 119, 115, 95, 115, 101, 99, 114, 101,
      116, 95, 97, 99, 99, 101, 115, 115, 95, 107,
      101, 121>>,
      description =>
      #{en =>
      <<65, 87, 83, 32, 83, 101, 99, 114, 101,
        116, 32, 65, 99, 99, 101, 115, 115, 32,
        75, 101, 121, 32, 102, 111, 114, 32,
        99, 111, 110, 110, 101, 99, 116, 105,
        110, 103, 32, 116, 111, 32, 68, 121,
        110, 97, 109, 111, 68, 66>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          231, 154, 132, 232, 174, 191, 233, 151,
          174, 229, 175, 134, 233, 146, 165>>},
      order => 5, required => true,
      title =>
      #{en =>
      <<65, 87, 83, 32, 83, 101, 99, 114, 101,
        116, 32, 65, 99, 99, 101, 115, 115, 32,
        75, 101, 121>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 232, 174,
          191, 233, 151, 174, 229, 175, 134, 233,
          146, 165>>},
      type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        68, 121, 110, 97, 109, 111, 68, 66, 32,
        67, 111, 110, 110, 101, 99, 116, 105,
        111, 110, 32, 80, 111, 111, 108>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          232, 191, 158, 230, 142, 165, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      order => 3, required => true,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    region =>
    #{default =>
    <<117, 115, 45, 119, 101, 115, 116, 45, 50>>,
      description =>
      #{en =>
      <<82, 101, 103, 105, 111, 110, 32, 111,
        102, 32, 116, 104, 101, 32, 65, 87, 83,
        32, 100, 121, 110, 97, 109, 111>>,
        zh =>
        <<65, 87, 83, 32, 68, 121, 110, 97, 109,
          111, 68, 66, 32, 230, 137, 128, 229,
          156, 168, 231, 154, 132, 229, 140, 186,
          229, 159, 159>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        82, 101, 103, 105, 111, 110>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          229, 140, 186, 229, 159, 159>>},
      type => string},
    url =>
    #{default =>
    <<104, 116, 116, 112, 58, 47, 47, 49, 50, 55,
      46, 48, 46, 48, 46, 49, 58, 56, 48, 48, 48>>,
      description =>
      #{en =>
      <<85, 114, 108, 32, 111, 102, 32, 68,
        121, 110, 97, 109, 111, 68, 66, 32, 83,
        101, 114, 118, 101, 114>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          230, 156, 141, 229, 138, 161, 229, 153,
          168, 229, 156, 176, 229, 157, 128>>},
      order => 2,
      title =>
      #{en =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        83, 101, 114, 118, 101, 114>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          230, 156, 141, 229, 138, 161, 229, 153,
          168>>},
      type => string}},
  status => on_get_resource_status,
  title =>
  #{en => <<68, 121, 110, 97, 109, 111, 68, 66>>,
    zh => <<68, 121, 110, 97, 109, 111, 68, 66>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_dynamo,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 68, 121, 110, 97, 109, 111, 68, 66>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 68, 121, 110, 97, 109,
      111, 68, 66>>},
  for => '$any', name => data_to_dynamo,
  params =>
  #{'$resource' =>
  #{description =>
  #{en =>
  <<66, 105, 110, 100, 32, 97, 32, 114, 101,
    115, 111, 117, 114, 99, 101, 32, 116,
    111, 32, 116, 104, 105, 115, 32, 97, 99,
    116, 105, 111, 110>>,
    zh =>
    <<231, 187, 153, 229, 138, 168, 228, 189,
      156, 231, 187, 145, 229, 174, 154, 228,
      184, 128, 228, 184, 170, 232, 181, 132,
      230, 186, 144>>},
    required => true,
    title =>
    #{en =>
    <<82, 101, 115, 111, 117, 114, 99, 101, 32,
      73, 68>>,
      zh =>
      <<232, 181, 132, 230, 186, 144, 32, 73,
        68>>},
    type => string},
    hash_key =>
    #{default => <<>>,
      description =>
      #{en =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        72, 97, 115, 104, 32, 75, 101, 121>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          72, 97, 115, 104, 32, 75, 101, 121>>},
      order => 2, required => true,
      title =>
      #{en => <<72, 97, 115, 104, 32, 75, 101, 121>>,
        zh => <<72, 97, 115, 104, 32, 75, 101, 121>>},
      type => string},
    range_key =>
    #{default => <<>>,
      description =>
      #{en =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        82, 97, 110, 103, 101, 32, 75, 101,
        121>>,
        zh =>
        <<68, 121, 110, 97, 109, 111, 68, 66, 32,
          82, 97, 110, 103, 101, 32, 75, 101,
          121>>},
      order => 3, required => false,
      title =>
      #{en =>
      <<82, 97, 110, 103, 101, 32, 75, 101,
        121>>,
        zh =>
        <<82, 97, 110, 103, 101, 32, 75, 101,
          121>>},
      type => string},
    table =>
    #{description =>
    #{en =>
    <<68, 121, 110, 97, 109, 111, 68, 66, 32,
      84, 97, 98, 108, 101>>,
      zh =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        232, 161, 168>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<68, 121, 110, 97, 109, 111, 68, 66, 32,
        84, 97, 98, 108, 101>>,
        zh =>
        <<68, 121, 97, 110, 109, 111, 68, 66, 32,
          232, 161, 168>>},
      type => string}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 68, 121, 110, 97,
    109, 111, 68, 66>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 68, 121, 110, 97, 109,
      111, 68, 66>>},
  types => [backend_dynamo]}).

-vsn("4.2.5").

on_resource_create(ResId,
    #{<<"region">> := Region, <<"url">> := Url,
      <<"pool_size">> := PoolSize,
      <<"aws_access_key_id">> := AccessKeyId,
      <<"aws_secret_access_key">> := SecretAccessKey,
      <<"auto_reconnect">> := AutoReconnect}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_dynamo, ResId]}
      end,
        mfa =>
        {emqx_backend_dynamo_actions, on_resource_create, 2},
        line => 157})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(erlcloud),
  application:set_env(erlcloud, aws_region, str(Region)),
  URIMAP = uri_string:parse(str(Url)),
  Host = maps:get(host, URIMAP),
  Port = maps:get(port, URIMAP),
  Scheme = maps:get(scheme, URIMAP) ++ "://",
  Options = [{scheme, Scheme},
    {host, Host},
    {port, Port},
    {pool_size, PoolSize},
    {aws_access_key_id, str(AccessKeyId)},
    {aws_secret_access_key, str(SecretAccessKey)},
    {auto_reconnect, AutoReconnect}],
  PoolName = pool_name(ResId),
  start_resource(ResId, PoolName, Options),
  case test_resource_status(PoolName) of
    true -> ok;
    false ->
      error({{backend_dynamo, ResId}, connection_failed})
  end,
  #{<<"pool">> => PoolName}.

start_resource(ResId, PoolName, Options) ->
  case ecpool:start_sup_pool(PoolName,
    emqx_backend_dynamo_actions,
    Options)
  of
    {ok, _} ->
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiated Resource ~p Successfully, "
              "ResId: ~p",
              [backend_dynamo, ResId]}
          end,
            mfa =>
            {emqx_backend_dynamo_actions,
              start_resource,
              3},
            line => 183})
      end;
    {error, {already_started, _Pid}} ->
      ok = on_resource_destroy(ResId,
        #{<<"pool">> => PoolName}),
      start_resource(ResId, PoolName, Options);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiate Resource ~p failed, ResId: "
              "~p, ~p",
              [backend_dynamo, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_dynamo_actions,
              start_resource,
              3},
            line => 188})
      end,
      error({{backend_dynamo, ResId}, create_failed})
  end.

test_resource_status(PoolName) ->
  GetTables = fun (Worker) ->
    {ok, Client} = ecpool_worker:client(Worker),
    emqx_backend_dynamo_client:list_tables(Client, 1000)
              end,
  Tables = [GetTables(Worker)
    || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
  length(Tables) > 0 andalso
    lists:all(fun ({Key, _Table}) -> Key == ok end, Tables).

-spec on_get_resource_status(ResId :: binary(),
    Params :: map()) -> Status :: map().

on_get_resource_status(_ResId,
    #{<<"pool">> := PoolName}) ->
  #{is_alive => test_resource_status(PoolName)}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [backend_dynamo, ResId]}
      end,
        mfa =>
        {emqx_backend_dynamo_actions, on_resource_destroy, 2},
        line => 205})
  end,
  case ecpool:stop_sup_pool(PoolName) of
    ok ->
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroyed Resource ~p Successfully, "
              "ResId: ~p",
              [backend_dynamo, ResId]}
          end,
            mfa =>
            {emqx_backend_dynamo_actions,
              on_resource_destroy,
              2},
            line => 208})
      end,
      ok;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource ~p failed, ResId: ~p, ~p",
              [backend_dynamo, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_dynamo_actions,
              on_resource_destroy,
              2},
            line => 211})
      end,
      error({{backend_dynamo, ResId}, destroy_failed})
  end.

on_action_create_data_to_dynamo(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"table">> := TableName,
      <<"hash_key">> := HashKey0,
      <<"range_key">> := RangeKey0}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Action ~p, Table: ~p",
          [on_action_create_data_to_dynamo, TableName]}
      end,
        mfa =>
        {emqx_backend_dynamo_actions,
          on_action_create_data_to_dynamo,
          2},
        line => 217})
  end,
  GetKey = fun (<<>>) -> undefined;
    (Key) -> binary_to_atom(Key, utf8)
           end,
  HashKey = GetKey(HashKey0),
  RangeKey = GetKey(RangeKey0),
  {[{'HashKey0', HashKey0},
    {'RangeKey0', RangeKey0},
    {'Opts', Opts},
    {'PoolName', PoolName},
    {'TableName', TableName},
    {'ActId', ActId},
    {'GetKey', GetKey},
    {'HashKey', HashKey},
    {'RangeKey', RangeKey}],
    Opts}.

on_action_data_to_dynamo(Msg,
    _Env = #{'__bindings__' :=
    #{'ActId' := ActId, 'HashKey' := HashKey,
      'RangeKey' := RangeKey,
      'PoolName' := PoolName,
      'TableName' := TableName}}) ->
  case filter_keys(Msg, [HashKey, RangeKey]) of
    [] ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to store message, no matched "
              "keys",
              []}
          end,
            mfa =>
            {emqx_backend_dynamo_actions,
              on_action_data_to_dynamo,
              2},
            line => 233})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, dynamodb_save_msg_failed};
    _Keys ->
      Msg1 = maps:fold(fun (_K, <<>>, AccIn) -> AccIn;
        (K, V, AccIn) ->
          [{convert2binary(K), convert2binary(V)}
            | AccIn]
                       end,
        [],
        Msg),
      save_msg_to_dynamodb(ActId, PoolName, TableName, Msg1)
  end.

save_msg_to_dynamodb(ActId, Pool, Table, Msg) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Save data to dynamo, pool: ~p, table: "
          "~p, message: ~p",
          [Pool, Table, Msg]}
      end,
        mfa =>
        {emqx_backend_dynamo_actions, save_msg_to_dynamodb, 4},
        line => 246})
  end,
  case ecpool:with_client(Pool,
    fun (C) ->
      emqx_backend_dynamo_client:insert_item(C,
        Table,
        Msg)
    end)
  of
    {ok, _} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      ok;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to store message: ~p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_dynamo_actions,
              save_msg_to_dynamodb,
              4},
            line => 252})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason}
  end.

convert2binary(Value) when is_atom(Value) ->
  atom_to_binary(Value, utf8);
convert2binary(Value)
  when is_binary(Value); is_number(Value) ->
  Value;
convert2binary(Value) when is_list(Value) ->
  iolist_to_binary(Value);
convert2binary(Value) when is_map(Value) ->
  emqx_json:encode(Value).

filter_keys(Msg, Keys) -> filter_keys(Msg, Keys, []).

filter_keys(_Msg, [], Acc) -> Acc;
filter_keys(Msg, [Key | LeftKeys], Acc) ->
  case maps:get(Key, Msg, undefined) of
    undefined -> filter_keys(Msg, LeftKeys, Acc);
    Value ->
      NewAcc = [{convert2binary(Key), convert2binary(Value)}
        | Acc],
      filter_keys(Msg, LeftKeys, NewAcc)
  end.

connect(Opts) ->
  emqx_backend_dynamo_client:start_link(Opts).

pool_name(ResId) ->
  list_to_atom("backend_dynamo:" ++ str(ResId)).

logger_header() -> "".
