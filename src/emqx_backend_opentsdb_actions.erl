%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:59
%%%-------------------------------------------------------------------
-module(emqx_backend_opentsdb_actions).
-author("root").
-export([logger_header/0]).

-behaviour(ecpool_worker).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").

-export([connect/1]).
-import(emqx_rule_utils, [str/1]).

-export([on_resource_create/2, on_get_resource_status/2, on_resource_destroy/2]).

-export([on_action_create_data_to_opentsdb/2,
  on_action_data_to_opentsdb/2]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en => <<79, 112, 101, 110, 84, 83, 68, 66>>,
    zh => <<79, 112, 101, 110, 84, 83, 68, 66>>},
  destroy => on_resource_destroy,
  name => backend_opentsdb,
  params =>
  #{pool_size =>
  #{default => 8,
    description =>
    #{en =>
    <<83, 105, 122, 101, 32, 111, 102, 32,
      79, 112, 101, 110, 84, 83, 68, 66, 32,
      67, 111, 110, 110, 101, 99, 116, 105,
      111, 110, 32, 80, 111, 111, 108>>,
      zh =>
      <<79, 112, 101, 110, 84, 83, 68, 66, 32,
        232, 191, 158, 230, 142, 165, 230, 177,
        160, 229, 164, 167, 229, 176, 143>>},
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
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 52,
      50, 52, 50>>,
      description =>
      #{en =>
      <<79, 112, 101, 110, 84, 83, 68, 66, 32,
        83, 101, 114, 118, 101, 114>>,
        zh =>
        <<79, 112, 101, 110, 84, 83, 68, 66, 32,
          230, 156, 141, 229, 138, 161, 229, 153,
          168, 229, 156, 176, 229, 157, 128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<79, 112, 101, 110, 84, 83, 68, 66, 32,
        83, 101, 114, 118, 101, 114>>,
        zh =>
        <<79, 112, 101, 110, 84, 83, 68, 66, 32,
          230, 156, 141, 229, 138, 161, 229, 153,
          168>>},
      type => string}},
  status => on_get_resource_status,
  title =>
  #{en => <<79, 112, 101, 110, 84, 83, 68, 66>>,
    zh => <<79, 112, 101, 110, 84, 83, 68, 66>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_opentsdb,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 79, 112, 101, 110, 84, 83, 68, 66>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 79, 112, 101, 110, 84,
      83, 68, 66>>},
  for => 'message.publish', name => data_to_opentsdb,
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
    details =>
    #{default => false,
      description =>
      #{en =>
      <<87, 104, 101, 116, 104, 101, 114, 32,
        111, 114, 32, 110, 111, 116, 32, 116,
        111, 32, 114, 101, 116, 117, 114, 110,
        32, 100, 101, 116, 97, 105, 108, 101,
        100, 32, 105, 110, 102, 111, 114, 109,
        97, 116, 105, 111, 110>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 232, 191,
          148, 229, 155, 158, 232, 175, 166, 231,
          187, 134, 228, 191, 161, 230, 129,
          175>>},
      title =>
      #{en => <<68, 101, 116, 97, 105, 108, 115>>,
        zh =>
        <<232, 175, 166, 231, 187, 134, 228, 191,
          161, 230, 129, 175>>},
      type => boolean},
    max_batch_size =>
    #{default => 20,
      description =>
      #{en =>
      <<77, 97, 120, 32, 66, 97, 116, 99, 104,
        32, 83, 105, 122, 101>>,
        zh =>
        <<230, 156, 128, 229, 164, 167, 230, 137,
          185, 229, 164, 132, 231, 144, 134, 230,
          149, 176, 233, 135, 143>>},
      title =>
      #{en =>
      <<77, 97, 120, 32, 66, 97, 116, 99, 104,
        32, 83, 105, 122, 101>>,
        zh =>
        <<230, 156, 128, 229, 164, 167, 230, 137,
          185, 229, 164, 132, 231, 144, 134, 230,
          149, 176, 233, 135, 143>>},
      type => number},
    summary =>
    #{default => true,
      description =>
      #{en =>
      <<87, 104, 101, 116, 104, 101, 114, 32,
        111, 114, 32, 110, 111, 116, 32, 116,
        111, 32, 114, 101, 116, 117, 114, 110,
        32, 115, 117, 109, 109, 97, 114, 121, 32,
        105, 110, 102, 111, 114, 109, 97, 116,
        105, 111, 110>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 232, 191,
          148, 229, 155, 158, 230, 145, 152, 232,
          166, 129, 228, 191, 161, 230, 129,
          175>>},
      title =>
      #{en => <<83, 117, 109, 109, 97, 114, 121>>,
        zh =>
        <<230, 145, 152, 232, 166, 129, 228, 191,
          161, 230, 129, 175>>},
      type => boolean},
    sync =>
    #{default => false,
      description =>
      #{en =>
      <<87, 104, 101, 116, 104, 101, 114, 32,
        111, 114, 32, 110, 111, 116, 32, 116,
        111, 32, 119, 97, 105, 116, 32, 102, 111,
        114, 32, 116, 104, 101, 32, 100, 97, 116,
        97, 32, 116, 111, 32, 98, 101, 32, 102,
        108, 117, 115, 104, 101, 100, 32, 116,
        111, 32, 115, 116, 111, 114, 97, 103,
        101, 32, 98, 101, 102, 111, 114, 101, 32,
        114, 101, 116, 117, 114, 110, 105, 110,
        103, 32, 116, 104, 101, 32, 114, 101,
        115, 117, 108, 116, 115>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 156,
          168, 232, 191, 148, 229, 155, 158, 231,
          187, 147, 230, 158, 156, 228, 185, 139,
          229, 137, 141, 231, 173, 137, 229, 190,
          133, 230, 149, 176, 230, 141, 174, 232,
          162, 171, 229, 136, 183, 230, 150, 176,
          229, 136, 176, 229, 173, 152, 229, 130,
          168, 229, 140, 186>>},
      title =>
      #{en =>
      <<83, 121, 110, 99, 32, 67, 97, 108, 108>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 144,
          140, 230, 173, 165, 232, 176, 131, 231,
          148, 168>>},
      type => boolean},
    sync_timeout =>
    #{default => 0,
      description =>
      #{en =>
      <<65, 32, 116, 105, 109, 101, 111, 117,
        116, 44, 32, 105, 110, 32, 109, 105, 108,
        108, 105, 115, 101, 99, 111, 110, 100,
        115, 44, 32, 116, 111, 32, 119, 97, 105,
        116, 32, 102, 111, 114, 32, 116, 104,
        101, 32, 100, 97, 116, 97, 32, 116, 111,
        32, 98, 101, 32, 102, 108, 117, 115, 104,
        101, 100, 32, 116, 111, 32, 115, 116,
        111, 114, 97, 103, 101, 32, 98, 101, 102,
        111, 114, 101, 32, 114, 101, 116, 117,
        114, 110, 105, 110, 103, 32, 119, 105,
        116, 104, 32, 97, 110, 32, 101, 114, 114,
        111, 114>>,
        zh =>
        <<231, 173, 137, 229, 190, 133, 230, 149,
          176, 230, 141, 174, 232, 162, 171, 229,
          134, 153, 229, 133, 165, 229, 136, 176,
          229, 173, 152, 229, 130, 168, 229, 140,
          186, 231, 154, 132, 230, 156, 128, 229,
          164, 167, 230, 151, 182, 233, 151, 180,
          40, 229, 141, 149, 228, 189, 141, 58, 32,
          230, 175, 171, 231, 167, 146, 41>>},
      title =>
      #{en =>
      <<83, 121, 110, 99, 32, 84, 105, 109, 101,
        111, 117, 116>>,
        zh =>
        <<229, 144, 140, 230, 173, 165, 232, 176,
          131, 231, 148, 168, 232, 182, 133, 230,
          151, 182, 230, 151, 182, 233, 151,
          180>>},
      type => number}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 79, 112, 101, 110,
    84, 83, 68, 66>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 79, 112, 101, 110, 84,
      83, 68, 66>>},
  types => [backend_opentsdb]}).

-vsn("4.2.5").

on_resource_create(ResId,
    Config = #{<<"server">> := Server}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_opentsdb, ResId]}
      end,
        mfa =>
        {emqx_backend_opentsdb_actions, on_resource_create, 2},
        line => 167})
  end,
  {ok, _} = application:ensure_all_started(opentsdb),
  {ok, _} = application:ensure_all_started(ecpool),
  Options = [{server, str(Server)},
    {auto_reconnect, false},
    {pool_size, maps:get(<<"pool_size">>, Config, 8)},
    {sync_timeout, maps:get(<<"sync_timeout">>, Config, 0)},
    {max_batch_size,
      maps:get(<<"max_batch_size">>, Config, 20)}],
  PoolName = pool_name(ResId),
  start_resource(ResId, PoolName, Options),
  #{<<"pool">> => PoolName, options => Options}.

start_resource(ResId, PoolName, Options) ->
  case ecpool:start_sup_pool(PoolName,
    emqx_backend_opentsdb_actions,
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
              [backend_opentsdb, ResId]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              start_resource,
              3},
            line => 183})
      end;
    {ok, _, _} ->
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiated Resource ~p Successfully, "
              "ResId: ~p",
              [backend_opentsdb, ResId]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              start_resource,
              3},
            line => 185})
      end;
    {error, {already_started, _Pid}} ->
      on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
      start_resource(ResId, PoolName, Options);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiate Resource ~p failed, ResId: "
              "~p, ~0p",
              [backend_opentsdb, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              start_resource,
              3},
            line => 191})
      end,
      error({backend_opentsdb, {ResId, create_failed}})
  end.

-spec on_get_resource_status(ResId :: binary(),
    Params :: map()) -> Status :: map().

on_get_resource_status(ResId,
    _Params = #{options := Options}) ->
  #{is_alive =>
  case opentsdb_connectivity(proplists:get_value(server,
    Options))
  of
    ok -> true;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Connectivity Check for ~p failed, ResId: "
              "~p, ~0p",
              [backend_opentsdb,
                ResId,
                Reason]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              on_get_resource_status,
              2},
            line => 202})
      end,
      false
  end}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [backend_opentsdb, ResId]}
      end,
        mfa =>
        {emqx_backend_opentsdb_actions,
          on_resource_destroy,
          2},
        line => 207})
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
              [backend_opentsdb, ResId]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              on_resource_destroy,
              2},
            line => 210})
      end;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource ~p failed, ResId: ~p, ~p",
              [backend_opentsdb, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              on_resource_destroy,
              2},
            line => 212})
      end,
      error({{backend_opentsdb, ResId}, destroy_failed})
  end.

-spec on_action_create_data_to_opentsdb(ActId ::
binary(),
    #{}) -> fun((Msg :: map(),
Envs :: map()) -> any()).

on_action_create_data_to_opentsdb(ActId,
    Params = #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Action ~p, Params: ~p",
          [on_action_create_data_to_opentsdb, Params]}
      end,
        mfa =>
        {emqx_backend_opentsdb_actions,
          on_action_create_data_to_opentsdb,
          2},
        line => 228})
  end,
  Params1 = maps:to_list(maps:map(fun (K, V)
    when K =:= <<"summary">>;
    K =:= <<"details">>;
    K =:= <<"sync">> ->
    {erlang:binary_to_atom(K, utf8),
      {present, V}};
    (K, V) ->
      {erlang:binary_to_atom(K, utf8), V}
                                  end,
    maps:with([<<"summary">>,
      <<"details">>,
      <<"sync">>,
      <<"sync_timeout">>,
      <<"max_batch_size">>],
      Params))),
  {[{'Params', Params},
    {'PoolName', PoolName},
    {'ActId', ActId},
    {'Params1', Params1}],
    Params}.

on_action_data_to_opentsdb(Msg,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'Params1' := Params1}}) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "OpenTSDB Pool: ~p, DataPoints: ~p, Options: ~p",
          [PoolName, Msg, Params1]}
      end,
        mfa =>
        {emqx_backend_opentsdb_actions,
          on_action_data_to_opentsdb,
          2},
        line => 244})
  end,
  case ecpool:with_client(PoolName,
    fun (C) -> opentsdb:put(C, Msg, Params1) end)
  of
    {ok, _, _} ->
      emqx_rule_metrics:inc_actions_success(ActId);
    ErrRet ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Write data to OpenTSDB failed, Action "
              "Id: ~p, reason: ~p",
              [ActId, ErrRet]}
          end,
            mfa =>
            {emqx_backend_opentsdb_actions,
              on_action_data_to_opentsdb,
              2},
            line => 249})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, ErrRet}
  end.

connect(Options) ->
  case opentsdb_connectivity(proplists:get_value(server,
    Options))
  of
    ok -> opentsdb:start_link(Options);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiate Resource ~p failed: ~0p",
              [backend_opentsdb, Reason]}
          end,
            mfa => {emqx_backend_opentsdb_actions, connect, 1},
            line => 264})
      end,
      {error, {connect_failure, Reason}}
  end.

pool_name(ResId) ->
  list_to_atom("backend_opentsdb:" ++ str(ResId)).

opentsdb_connectivity(Url) ->
  Url1 = case list_to_binary(Url) of
           <<"http://", _/binary>> -> Url;
           <<"https://", _/binary>> -> Url;
           _ -> "http://" ++ Url
         end,
  emqx_rule_utils:http_connectivity(Url1).

logger_header() -> "".
