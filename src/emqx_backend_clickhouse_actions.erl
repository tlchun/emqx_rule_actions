%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:58
%%%-------------------------------------------------------------------
-module(emqx_backend_clickhouse_actions).
-author("root").

-export([logger_header/0]).
-include("../include/emqx.hrl").
-behaviour(ecpool_worker).
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").

-export([batcher_flush/2]).

-export([insert/2, batch_insert/3, async_call_back/3]).

-export([connect/1]).

-import(emqx_rule_utils, [str/1]).

-export([on_resource_create/2,on_get_resource_status/2,on_resource_destroy/2]).
-export([on_action_create_data_to_clickhouse/2,on_action_data_to_clickhouse/2,on_action_destroy_data_to_clickhouse/2]).

-resource_type(#{create => on_resource_create, description =>
  #{
    en => <<67, 108, 105, 99, 107, 72, 111, 117, 115, 101>>,
    zh => <<67, 108, 105, 99, 107, 72, 111, 117, 115, 101>>},
  destroy => on_resource_destroy,
  name => backend_clickhouse,
  params =>
  #{key =>
  #{default => <<>>,
    description =>
    #{en =>
    <<75, 101, 121, 32, 102, 111, 114, 32,
      99, 111, 110, 110, 101, 99, 116, 105,
      110, 103, 32, 116, 111, 32, 67, 108,
      105, 99, 107, 72, 111, 117, 115, 101>>,
      zh =>
      <<67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 229, 175, 134, 231, 160,
        129>>},
    order => 4, required => false,
    title =>
    #{en =>
    <<67, 108, 105, 99, 107, 72, 111, 117,
      115, 101, 32, 75, 101, 121>>,
      zh =>
      <<67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 229, 175, 134, 231, 160,
        129>>},
    type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 67, 111, 110, 110, 101,
        99, 116, 105, 111, 110, 32, 80, 111,
        111, 108>>,
        zh =>
        <<67, 108, 105, 99, 107, 72, 111, 117,
          115, 101, 32, 232, 191, 158, 230, 142,
          165, 230, 177, 160, 229, 164, 167, 229,
          176, 143>>},
      order => 2,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    server =>
    #{default =>
    <<104, 116, 116, 112, 58, 47, 47, 49, 50, 55,
      46, 48, 46, 48, 46, 49, 58, 56, 49, 50, 51>>,
      description =>
      #{en =>
      <<67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 83, 101, 114, 118, 101,
        114>>,
        zh =>
        <<67, 108, 105, 99, 107, 72, 111, 117,
          115, 101, 32, 230, 156, 141, 229, 138,
          161, 229, 153, 168, 229, 156, 176, 229,
          157, 128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 83, 101, 114, 118, 101,
        114>>,
        zh =>
        <<67, 108, 105, 99, 107, 72, 111, 117,
          115, 101, 32, 230, 156, 141, 229, 138,
          161, 229, 153, 168>>},
      type => string},
    user =>
    #{default => <<>>,
      description =>
      #{en =>
      <<85, 115, 101, 114, 32, 102, 111, 114,
        32, 99, 111, 110, 110, 101, 99, 116,
        105, 110, 103, 32, 116, 111, 32, 67,
        108, 105, 99, 107, 72, 111, 117, 115,
        101>>,
        zh =>
        <<67, 108, 105, 99, 107, 72, 111, 117,
          115, 101, 32, 231, 148, 168, 230, 136,
          183, 229, 144, 141>>},
      order => 3, required => false,
      title =>
      #{en =>
      <<67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 85, 115, 101, 114>>,
        zh =>
        <<67, 108, 105, 99, 107, 72, 111, 117,
          115, 101, 32, 231, 148, 168, 230, 136,
          183, 229, 144, 141>>},
      type => string}},
  status => on_get_resource_status,
  title =>
  #{en =>
  <<67, 108, 105, 99, 107, 72, 111, 117, 115, 101>>,
    zh =>
    <<67, 108, 105, 99, 107, 72, 111, 117, 115, 101>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_clickhouse,
  description =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 67, 108, 105, 99,
    107, 72, 111, 117, 115, 101>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 67, 108, 105, 99, 107,
      72, 111, 117, 115, 101, 32, 230, 149, 176, 230, 141,
      174, 229, 186, 147>>},
  destroy => on_action_destroy_data_to_clickhouse,
  for => '$any', name => data_to_clickhouse,
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
    enable_batch =>
    #{default => false,
      description =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 66, 97,
        116, 99, 104, 32, 73, 110, 115, 101, 114,
        116>>,
        zh =>
        <<229, 144, 175, 231, 148, 168, 230, 137,
          185, 233, 135, 143, 230, 143, 146, 229,
          133, 165>>},
      enum => [true, false],
      items =>
      #{false => #{},
        true =>
        #{batch_size =>
        #{default => 100,
          description =>
          #{en =>
          <<77, 97, 120, 105, 109,
            117, 109, 32, 110, 117,
            109, 98, 101, 114, 32,
            111, 102, 32, 73, 78,
            83, 69, 82, 84, 32, 83,
            81, 76, 32, 99, 108,
            97, 117, 115, 101, 115,
            32, 116, 104, 97, 116,
            32, 99, 97, 110, 32,
            98, 101, 32, 115, 101,
            110, 116, 32, 105, 110,
            32, 97, 32, 115, 105,
            110, 103, 108, 101, 32,
            114, 101, 113, 117,
            101, 115, 116>>,
            zh =>
            <<229, 141, 149, 230,
              172, 161, 230, 137,
              185, 233, 135, 143,
              232, 175, 183, 230,
              177, 130, 229, 143,
              175, 228, 187, 165,
              229, 143, 145, 233,
              128, 129, 231, 154,
              132, 230, 156, 128,
              229, 164, 167, 32, 73,
              78, 83, 69, 82, 84, 32,
              83, 81, 76, 32, 230,
              157, 161, 231, 155,
              174>>},
          order => 2, required => false,
          title =>
          #{en =>
          <<77, 97, 120, 32, 66,
            97, 116, 99, 104, 32,
            73, 110, 115, 101, 114,
            116, 32, 67, 111, 117,
            110, 116>>,
            zh =>
            <<230, 156, 128, 229,
              164, 167, 230, 137,
              185, 233, 135, 143,
              230, 149, 176>>},
          type => number},
          batch_time =>
          #{default => 10,
            description =>
            #{en =>
            <<77, 97, 120, 105, 109,
              117, 109, 32, 105, 110,
              116, 101, 114, 118, 97,
              108, 32, 105, 110, 32,
              109, 105, 108, 108,
              105, 115, 101, 99, 111,
              110, 100, 115, 32, 116,
              104, 97, 116, 32, 105,
              115, 32, 97, 108, 108,
              111, 119, 101, 100, 32,
              98, 101, 116, 119, 101,
              101, 110, 32, 116, 119,
              111, 32, 115, 117, 99,
              99, 101, 115, 115, 105,
              118, 101, 32, 40, 98,
              97, 116, 99, 104, 41,
              32, 114, 101, 113, 117,
              101, 115, 116>>,
              zh =>
              <<228, 184, 164, 230,
                172, 161, 40, 230, 137,
                185, 233, 135, 143, 41,
                232, 175, 183, 230,
                177, 130, 228, 185,
                139, 233, 151, 180,
                230, 156, 128, 229,
                164, 167, 231, 154,
                132, 231, 173, 137,
                229, 190, 133, 233,
                151, 180, 233, 154,
                148, 227, 128, 130,
                230, 140, 137, 230,
                175, 171, 231, 167,
                146, 232, 174, 161>>},
            order => 3, required => false,
            title =>
            #{en =>
            <<77, 97, 120, 32, 66,
              97, 116, 99, 104, 32,
              73, 110, 116, 101, 114,
              118, 97, 108>>,
              zh =>
              <<230, 156, 128, 229,
                164, 167, 230, 137,
                185, 233, 135, 143,
                233, 151, 180, 233,
                154, 148, 40, 109, 115,
                41>>},
            type => number},
          insert_mode =>
          #{default => <<115, 121, 110, 99>>,
            description =>
            #{en =>
            <<83, 121, 110, 99, 32,
              111, 114, 32, 97, 115,
              121, 110, 99, 32, 73,
              78, 83, 69, 82, 84, 32,
              116, 111, 32, 68, 66>>,
              zh =>
              <<229, 144, 140, 230,
                173, 165, 230, 136,
                150, 232, 128, 133,
                229, 188, 130, 230,
                173, 165, 230, 137,
                167, 232, 161, 140, 32,
                73, 78, 83, 69, 82, 84,
                32, 232, 175, 173, 229,
                143, 165>>},
            enum =>
            [<<115, 121, 110, 99>>,
              <<97, 115, 121, 110, 99>>],
            order => 4, required => false,
            title =>
            #{en =>
            <<83, 121, 110, 99, 32,
              111, 114, 32, 65, 115,
              121, 110, 99, 32, 73,
              78, 83, 69, 82, 84>>,
              zh =>
              <<229, 144, 140, 230,
                173, 165, 230, 136,
                150, 232, 128, 133,
                229, 188, 130, 230,
                173, 165, 230, 143,
                146, 229, 133, 165>>},
            type => string},
          sync_timeout =>
          #{default => 5000,
            description =>
            #{en =>
            <<84, 105, 109, 101, 111,
              117, 116, 32, 105, 110,
              32, 109, 105, 108, 108,
              105, 115, 101, 99, 111,
              110, 100, 115, 32, 102,
              111, 114, 32, 112, 101,
              114, 102, 111, 114,
              109, 105, 110, 103, 32,
              97, 99, 116, 105, 111,
              110, 115, 32, 105, 110,
              32, 97, 32, 115, 121,
              110, 99, 104, 114, 111,
              110, 105, 122, 101,
              100, 32, 109, 97, 110,
              110, 101, 114>>,
              zh =>
              <<228, 187, 165, 229,
                144, 140, 230, 173,
                165, 230, 150, 185,
                229, 188, 143, 230,
                137, 167, 232, 161,
                140, 229, 138, 168,
                228, 189, 156, 231,
                154, 132, 232, 182,
                133, 230, 151, 182,
                230, 151, 182, 233,
                151, 180, 227, 128,
                130, 230, 140, 137,
                230, 175, 171, 231,
                167, 146, 232, 174,
                161>>},
            order => 5, required => false,
            title =>
            #{en =>
            <<67, 97, 108, 108, 32,
              84, 105, 109, 101, 111,
              117, 116>>,
              zh =>
              <<232, 176, 131, 231,
                148, 168, 232, 182,
                133, 230, 151, 182,
                230, 151, 182, 233,
                151, 180, 40, 109, 115,
                41>>},
            type => number}}},
      order => 1,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 66, 97,
        116, 99, 104, 32, 73, 110, 115, 101, 114,
        116>>,
        zh =>
        <<229, 144, 175, 231, 148, 168, 230, 137,
          185, 233, 135, 143, 230, 143, 146, 229,
          133, 165>>},
      type => cfgselect},
    sql =>
    #{description =>
    #{en =>
    <<83, 81, 76, 32, 116, 101, 109, 112, 108,
      97, 116, 101, 32, 119, 105, 116, 104, 32,
      112, 108, 97, 99, 101, 104, 111, 108,
      100, 101, 114, 115, 32, 102, 111, 114,
      32, 105, 110, 115, 101, 114, 116, 105,
      110, 103, 47, 117, 112, 100, 97, 116,
      105, 110, 103, 32, 100, 97, 116, 97, 32,
      116, 111, 32, 67, 108, 105, 99, 107, 72,
      111, 117, 115, 101>>,
      zh =>
      <<229, 140, 133, 229, 144, 171, 228, 186,
        134, 229, 141, 160, 228, 189, 141, 231,
        172, 166, 231, 154, 132, 32, 83, 81, 76,
        32, 230, 168, 161, 230, 157, 191, 239,
        188, 140, 231, 148, 168, 228, 187, 165,
        230, 143, 146, 229, 133, 165, 230, 136,
        150, 230, 155, 180, 230, 150, 176, 230,
        149, 176, 230, 141, 174, 229, 136, 176,
        32, 67, 108, 105, 99, 107, 72, 111, 117,
        115, 101, 32, 230, 149, 176, 230, 141,
        174, 229, 186, 147>>},
      input => textarea, order => 6, required => true,
      title =>
      #{en =>
      <<83, 81, 76, 32, 84, 101, 109, 112, 108,
        97, 116, 101>>,
        zh =>
        <<83, 81, 76, 32, 230, 168, 161, 230, 157,
          191>>},
      type => string}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 67, 108, 105, 99,
    107, 72, 111, 117, 115, 101>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 67, 108, 105, 99, 107,
      72, 111, 117, 115, 101>>},
  types => [backend_clickhouse]}).


on_resource_create(ResId, #{<<"server">> := Server, <<"pool_size">> := PoolSize, <<"user">> := User, <<"key">> := Key}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_clickhouse, ResId]}
      end,
        mfa =>
        {emqx_backend_clickhouse_actions, on_resource_create, 2}, line => 214})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(clickhouse),
  Options = [{pool_size, PoolSize}, {url, Server}, {user, User}, {key, Key}, {pool, binary_to_atom(ResId, utf8)}],
  PoolName = list_to_atom("clickhouse:" ++ str(ResId)),
  emqx_rule_actions_utils:start_pool(PoolName, emqx_backend_clickhouse_actions, Options),
  case test_resource_status(PoolName) of
    true -> ok;
    false ->
      error({{backend_clickhouse, ResId}, connection_failed})
  end,
  #{<<"pool">> => PoolName}.

-spec on_get_resource_status(ResId :: binary(), Params :: map()) -> Status :: map().
on_get_resource_status(_ResId,#{<<"pool">> := PoolName}) ->
  #{is_alive => test_resource_status(PoolName)}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [backend_clickhouse, ResId]}
      end,
        mfa =>
        {emqx_backend_clickhouse_actions,
          on_resource_destroy,
          2},
        line => 231})
  end,
  emqx_rule_actions_utils:stop_pool(PoolName).

-spec on_action_create_data_to_clickhouse(Id :: binary(), #{}) -> fun((Msg :: map()) -> any()).
on_action_create_data_to_clickhouse(ActId,
    Opts = #{<<"enable_batch">> :=
    EnableBatch = true,
      <<"pool">> := PoolName,
      <<"sql">> := SQL}) ->
  BatcherPName = list_to_atom("clickhouse_batcher:" ++
  str(ActId)),
  SyncTimeout = maps:get(<<"sync_timeout">>, Opts, 5000),
  InsertMode = maps:get(<<"insert_mode">>,
    Opts,
    <<"sync">>),
  emqx_rule_actions_utils:start_batcher_pool(BatcherPName,
    emqx_backend_clickhouse_actions,
    Opts,
    case InsertMode of
      <<"sync">> ->
        {PoolName,
          ActId,
          {sync, SyncTimeout}};
      <<"async">> ->
        {PoolName, ActId, async}
    end),
  {ok, {SQLInsertPart0, SQLParamPart0}} =
    emqx_rule_actions_utils:split_insert_sql(SQL),
  SQLInsertPartTks =
    emqx_rule_utils:preproc_tmpl(SQLInsertPart0),
  SQLParamPartTks =
    emqx_rule_utils:preproc_tmpl(SQLParamPart0),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
    {'EnableBatch', EnableBatch},
    {'SQL', SQL},
    {'ActId', ActId},
    {'BatcherPName', BatcherPName},
    {'SyncTimeout', SyncTimeout},
    {'InsertMode', InsertMode},
    {'SQLInsertPart0', SQLInsertPart0},
    {'SQLParamPart0', SQLParamPart0},
    {'SQLInsertPartTks', SQLInsertPartTks},
    {'SQLParamPartTks', SQLParamPartTks}],
    Opts#{batcher_pname => BatcherPName}};
on_action_create_data_to_clickhouse(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"sql">> := SQL}) ->
  PayloadTks = emqx_rule_utils:preproc_tmpl(SQL),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
    {'SQL', SQL},
    {'ActId', ActId},
    {'PayloadTks', PayloadTks}],
    Opts}.

on_action_destroy_data_to_clickhouse(_ActId,
    #{batcher_pname := BatcherPName}) ->
  emqx_rule_actions_utils:stop_batcher_pool(BatcherPName);
on_action_destroy_data_to_clickhouse(_ActId, _) -> ok.

on_action_data_to_clickhouse(Data,
    _Envs = #{'__bindings__' :=
    #{'EnableBatch' := true,
      'ActId' := ActId,
      'BatcherPName' := BatcherPName,
      'SQLInsertPartTks' :=
      SQLInsertPartTks,
      'SQLParamPartTks' :=
      SQLParamPartTks,
      'InsertMode' := InsertMode,
      'SyncTimeout' := SyncTimeout}}) ->
  SQLInsertPart =
    emqx_rule_utils:proc_tmpl(SQLInsertPartTks, Data),
  SQLParamPart =
    emqx_rule_utils:proc_tmpl(SQLParamPartTks, Data),
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "ClickHouse Pool: ~p, Insert SQL: ~p, "
          "Params: ~p",
          [BatcherPName, SQLInsertPart, SQLParamPart]}
      end,
        mfa =>
        {emqx_backend_clickhouse_actions,
          on_action_data_to_clickhouse,
          2},
        line => 270})
  end,
  case ecpool:pick_and_do(BatcherPName,
    emqx_rule_actions_batcher:accumulate_mfa(InsertMode,
      {SQLInsertPart,
        SQLParamPart},
      SyncTimeout),
    no_handover)
  of
    ok ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "async insert request sent",
              []}
          end,
            mfa =>
            {emqx_backend_clickhouse_actions,
              on_action_data_to_clickhouse,
              2},
            line => 273})
      end;
    {ok, Code, Resp} ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "insert successfully, response: ~p",
              [{Code, Resp}]}
          end,
            mfa =>
            {emqx_backend_clickhouse_actions,
              on_action_data_to_clickhouse,
              2},
            line => 275})
      end,
      emqx_rule_metrics:inc_actions_success(ActId);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "insert failed, reason: ~0p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_clickhouse_actions,
              on_action_data_to_clickhouse,
              2},
            line => 278})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason}
  end;
on_action_data_to_clickhouse(Data,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PayloadTks' := PayloadTks,
      'PoolName' := PoolName}}) ->
  SQL = emqx_rule_utils:proc_tmpl(PayloadTks, Data),
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "ClickHouse Pool: ~p, Query SQL: ~p",
          [PoolName, SQL]}
      end,
        mfa =>
        {emqx_backend_clickhouse_actions,
          on_action_data_to_clickhouse,
          2},
        line => 290})
  end,
  case ecpool:pick_and_do(PoolName,
    {emqx_backend_clickhouse_actions, insert, [SQL]},
    no_handover)
  of
    {ok, _, Resp} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "query successfully, resp: ~p",
              [Resp]}
          end,
            mfa =>
            {emqx_backend_clickhouse_actions,
              on_action_data_to_clickhouse,
              2},
            line => 294})
      end;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "query failed, reason: ~0p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_clickhouse_actions,
              on_action_data_to_clickhouse,
              2},
            line => 296})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason}
  end.

test_resource_status(PoolName) ->
  Status = [begin
              case ecpool_worker:client(Worker) of
                {ok, Conn} -> clickhouse:status(Conn);
                _ -> false
              end
            end
    || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
  length(Status) > 0 andalso
    lists:all(fun (St) -> St =:= true end, Status).

connect(Options) -> clickhouse:start_link(Options).

batcher_flush(Batch, State) ->
  {emqx_rule_actions_utils:batch_sql_insert(Batch,
    {emqx_backend_clickhouse_actions,
      batch_insert,
      [State]}),
    State}.

batch_insert(SQL, _BatchLen,
    {PoolName, _ActId, {sync, SyncTimeout}}) ->
  ecpool:pick_and_do(PoolName,
    {emqx_backend_clickhouse_actions, insert, [SQL]},
    {handover, SyncTimeout});
batch_insert(SQL, BatchLen, {PoolName, ActId, async}) ->
  ecpool:pick_and_do(PoolName,
    {emqx_backend_clickhouse_actions, insert, [SQL]},
    {handover_async,
      {emqx_backend_clickhouse_actions,
        async_call_back,
        [ActId, BatchLen]}}).

async_call_back({ok, Code, Resp}, ActId, BatchLen) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "async insert success, result: ~0p",
          [{Code, Resp}]}
      end,
        mfa =>
        {emqx_backend_clickhouse_actions, async_call_back, 3},
        line => 327})
  end,
  emqx_rule_metrics:inc_actions_success(ActId, BatchLen);
async_call_back({error, Reason}, ActId, BatchLen) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "async insert failed, result: ~0p",
          [Reason]}
      end,
        mfa =>
        {emqx_backend_clickhouse_actions, async_call_back, 3},
        line => 330})
  end,
  emqx_rule_metrics:inc_actions_error(ActId, BatchLen).

insert(Conn, SQL) when is_pid(Conn) ->
  clickhouse:insert(Conn, SQL, []).

logger_header() -> "[ClickHouse Action] ".
