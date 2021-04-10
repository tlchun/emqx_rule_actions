%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午7:00
%%%-------------------------------------------------------------------
-module(emqx_bridge_rabbit_actions).
-author("root").
-export([logger_header/0]).

-behaviour(ecpool_worker).
-include("../include/emqx_bridge_rabbit.hrl").
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").


-import(emqx_rule_utils, [str/1]).

-export([connect/1, disconnect/1]).

-export([on_resource_create/2,
  on_get_resource_status/2,
  on_resource_destroy/2]).

-export([on_action_create_data_to_rabbit/2,
  on_action_data_to_rabbit/2]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en =>
  <<82, 97, 98, 98, 105, 116, 77, 81, 32, 82, 101, 115,
    111, 117, 114, 99, 101>>,
    zh =>
    <<82, 97, 98, 98, 105, 116, 77, 81, 32, 232, 181,
      132, 230, 186, 144>>},
  destroy => on_resource_destroy, name => bridge_rabbit,
  params =>
  #{auto_reconnect =>
  #{default => <<50, 115>>,
    description =>
    #{en =>
    <<65, 117, 116, 111, 32, 82, 101, 99,
      111, 110, 110, 101, 99, 116, 32, 84,
      105, 109, 101, 115, 32, 102, 111, 114,
      32, 99, 111, 110, 110, 101, 99, 116,
      105, 110, 103, 32, 116, 111, 32, 82,
      97, 98, 98, 105, 116, 77, 81>>,
      zh =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32,
        232, 135, 170, 229, 138, 168, 233, 135,
        141, 232, 191, 158, 233, 151, 180, 233,
        154, 148>>},
    order => 9,
    title =>
    #{en =>
    <<65, 117, 116, 111, 32, 82, 101, 99,
      111, 110, 110, 101, 99, 116, 32, 84,
      105, 109, 101, 115>>,
      zh =>
      <<232, 135, 170, 229, 138, 168, 233, 135,
        141, 232, 191, 158, 233, 151, 180, 233,
        154, 148>>},
    type => string},
    heartbeat =>
    #{default => <<51, 48, 115>>,
      description =>
      #{en =>
      <<72, 101, 97, 114, 116, 98, 101, 97,
        116, 32, 102, 111, 114, 32, 99, 111,
        110, 110, 101, 99, 116, 105, 110, 103,
        32, 116, 111, 32, 82, 97, 98, 98, 105,
        116, 77, 81>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          229, 191, 131, 232, 183, 179, 233, 151,
          180, 233, 154, 148>>},
      order => 8,
      title =>
      #{en =>
      <<72, 101, 97, 114, 116, 98, 101, 97,
        114, 116>>,
        zh =>
        <<229, 191, 131, 232, 183, 179, 233, 151,
          180, 233, 154, 148>>},
      type => string},
    password =>
    #{default => <<103, 117, 101, 115, 116>>,
      description =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32,
        80, 97, 115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          229, 175, 134, 231, 160, 129>>},
      order => 5,
      title =>
      #{en =>
      <<80, 97, 115, 115, 119, 111, 114, 100>>,
        zh => <<229, 175, 134, 231, 160, 129>>},
      type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        82, 97, 98, 98, 105, 116, 77, 81, 32,
        99, 111, 110, 110, 101, 99, 116, 105,
        111, 110, 32, 112, 111, 111, 108>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
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
    server =>
    #{default =>
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 53,
      54, 55, 50>>,
      description =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32,
        83, 101, 114, 118, 101, 114>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          230, 156, 141, 229, 138, 161, 229, 153,
          168, 229, 156, 176, 229, 157, 128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32,
        83, 101, 114, 118, 101, 114>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          230, 156, 141, 229, 138, 161, 229, 153,
          168>>},
      type => string},
    timeout =>
    #{default => <<53, 115>>,
      description =>
      #{en =>
      <<67, 111, 110, 110, 101, 99, 116, 105,
        111, 110, 32, 116, 105, 109, 101, 111,
        117, 116, 32, 102, 111, 114, 32, 99,
        111, 110, 110, 101, 99, 116, 105, 110,
        103, 32, 116, 111, 32, 82, 97, 98, 98,
        105, 116, 77, 81>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          232, 191, 158, 230, 142, 165, 232, 182,
          133, 230, 151, 182, 230, 151, 182, 233,
          151, 180>>},
      order => 6,
      title =>
      #{en =>
      <<67, 111, 110, 110, 101, 99, 116, 105,
        111, 110, 32, 84, 105, 109, 101, 111,
        117, 116>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 232, 182,
          133, 230, 151, 182, 230, 151, 182, 233,
          151, 180>>},
      type => string},
    username =>
    #{default => <<103, 117, 101, 115, 116>>,
      description =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32,
        85, 115, 101, 114, 110, 97, 109, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          231, 148, 168, 230, 136, 183, 229, 144,
          141>>},
      order => 4,
      title =>
      #{en =>
      <<85, 115, 101, 114, 110, 97, 109, 101>>,
        zh =>
        <<231, 148, 168, 230, 136, 183, 229, 144,
          141>>},
      type => string},
    virtual_host =>
    #{default => <<47>>,
      description =>
      #{en =>
      <<86, 105, 114, 116, 117, 97, 108, 32,
        104, 111, 115, 116, 32, 102, 111, 114,
        32, 99, 111, 110, 110, 101, 99, 116,
        105, 110, 103, 32, 116, 111, 32, 82,
        97, 98, 98, 105, 116, 77, 81>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32,
          232, 153, 154, 230, 139, 159, 228, 184,
          187, 230, 156, 186>>},
      order => 7,
      title =>
      #{en =>
      <<86, 105, 114, 116, 117, 97, 108, 32,
        72, 111, 115, 116>>,
        zh =>
        <<232, 153, 154, 230, 139, 159, 228, 184,
          187, 230, 156, 186>>},
      type => string}},
  status => on_get_resource_status,
  title =>
  #{en => <<82, 97, 98, 98, 105, 116, 77, 81>>,
    zh => <<82, 97, 98, 98, 105, 116, 77, 81>>}}).

-rule_action(#{category => data_forward,
  create => on_action_create_data_to_rabbit,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 82, 97, 98, 98, 105, 116, 77, 81>>,
    zh =>
    <<230, 161, 165, 230, 142, 165, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 82, 97, 98, 98, 105,
      116, 77, 81>>},
  for => '$any', name => data_to_rabbit,
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
    durable =>
    #{default => false,
      description =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
        120, 99, 104, 97, 110, 103, 101, 32, 68,
        117, 114, 97, 98, 108, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
          120, 99, 104, 97, 110, 103, 101, 32, 68,
          117, 114, 97, 98, 108, 101>>},
      order => 4,
      title =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
        120, 99, 104, 97, 110, 103, 101, 32, 68,
        117, 114, 97, 98, 108, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
          120, 99, 104, 97, 110, 103, 101, 32, 68,
          117, 114, 97, 98, 108, 101>>},
      type => boolean},
    exchange =>
    #{default => <<109, 101, 115, 115, 97, 103, 101, 115>>,
      description =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
        120, 99, 104, 97, 110, 103, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
          120, 99, 104, 97, 110, 103, 101>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
        120, 99, 104, 97, 110, 103, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
          120, 99, 104, 97, 110, 103, 101>>},
      type => string},
    exchange_type =>
    #{default => <<116, 111, 112, 105, 99>>,
      description =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
        120, 99, 104, 97, 110, 103, 101, 32, 84,
        121, 112, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
          120, 99, 104, 97, 110, 103, 101, 32, 231,
          177, 187, 229, 158, 139>>},
      enum =>
      [<<100, 105, 114, 101, 99, 116>>,
        <<102, 97, 110, 111, 117, 116>>,
        <<116, 111, 112, 105, 99>>],
      order => 2, required => true,
      title =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
        120, 99, 104, 97, 110, 103, 101, 32, 84,
        121, 112, 101>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 77, 81, 32, 69,
          120, 99, 104, 97, 110, 103, 101, 32, 84,
          121, 112, 101>>},
      type => string},
    payload_tmpl =>
    #{default => <<>>,
      description =>
      #{en =>
      <<84, 104, 101, 32, 112, 97, 121, 108, 111,
        97, 100, 32, 116, 101, 109, 112, 108, 97,
        116, 101, 44, 32, 118, 97, 114, 105, 97,
        98, 108, 101, 32, 105, 110, 116, 101,
        114, 112, 111, 108, 97, 116, 105, 111,
        110, 32, 105, 115, 32, 115, 117, 112,
        112, 111, 114, 116, 101, 100, 46, 32, 73,
        102, 32, 117, 115, 105, 110, 103, 32,
        101, 109, 112, 116, 121, 32, 116, 101,
        109, 112, 108, 97, 116, 101, 32, 40, 100,
        101, 102, 97, 117, 108, 116, 41, 44, 32,
        116, 104, 101, 110, 32, 116, 104, 101,
        32, 112, 97, 121, 108, 111, 97, 100, 32,
        119, 105, 108, 108, 32, 98, 101, 32, 97,
        108, 108, 32, 116, 104, 101, 32, 97, 118,
        97, 105, 108, 97, 98, 108, 101, 32, 118,
        97, 114, 115, 32, 105, 110, 32, 74, 83,
        79, 78, 32, 102, 111, 114, 109, 97,
        116>>,
        zh =>
        <<230, 182, 136, 230, 129, 175, 229, 134,
          133, 229, 174, 185, 230, 168, 161, 230,
          157, 191, 239, 188, 140, 230, 148, 175,
          230, 140, 129, 229, 143, 152, 233, 135,
          143, 227, 128, 130, 232, 139, 165, 228,
          189, 191, 231, 148, 168, 231, 169, 186,
          230, 168, 161, 230, 157, 191, 239, 188,
          136, 233, 187, 152, 232, 174, 164, 239,
          188, 137, 239, 188, 140, 230, 182, 136,
          230, 129, 175, 229, 134, 133, 229, 174,
          185, 228, 184, 186, 32, 74, 83, 79, 78,
          32, 230, 160, 188, 229, 188, 143, 231,
          154, 132, 230, 137, 128, 230, 156, 137,
          229, 173, 151, 230, 174, 181>>},
      input => textarea, order => 5, required => false,
      title =>
      #{en =>
      <<80, 97, 121, 108, 111, 97, 100, 32, 84,
        101, 109, 112, 108, 97, 116, 101>>,
        zh =>
        <<230, 182, 136, 230, 129, 175, 229, 134,
          133, 229, 174, 185, 230, 168, 161, 230,
          157, 191>>},
      type => string},
    routing_key =>
    #{description =>
    #{en =>
    <<82, 97, 98, 98, 105, 116, 77, 81, 32, 82,
      111, 117, 116, 105, 110, 103, 32, 75,
      101, 121>>,
      zh =>
      <<82, 97, 98, 98, 105, 116, 32, 82, 111,
        117, 116, 105, 110, 103, 32, 75, 101,
        121>>},
      order => 3, required => true,
      title =>
      #{en =>
      <<82, 97, 98, 98, 105, 116, 77, 81, 32, 82,
        111, 117, 116, 105, 110, 103, 32, 75,
        101, 121>>,
        zh =>
        <<82, 97, 98, 98, 105, 116, 32, 82, 111,
          117, 116, 105, 110, 103, 32, 75, 101,
          121>>},
      type => string}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 98, 114, 105, 100, 103, 101, 32,
    116, 111, 32, 82, 97, 98, 98, 105, 116, 77, 81>>,
    zh =>
    <<230, 161, 165, 230, 142, 165, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 82, 97, 98, 98, 105,
      116, 77, 81>>},
  types => [bridge_rabbit]}).

-vsn("4.2.5").

on_resource_create(ResId,
    #{<<"server">> := Server, <<"pool_size">> := PoolSize,
      <<"username">> := Username, <<"password">> := Password,
      <<"timeout">> := Timeout,
      <<"virtual_host">> := VirtualHost,
      <<"heartbeat">> := HeartBeat,
      <<"auto_reconnect">> := AutoReconnect}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [bridge_rabbit, ResId]}
      end,
        mfa =>
        {emqx_bridge_rabbit_actions, on_resource_create, 2},
        line => 196})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(amqp_client),
  {Host, Port} = host(Server),
  Options = [{host, str(Host)},
    {port, Port},
    {username, str(Username)},
    {password, str(Password)},
    {virtual_host, str(VirtualHost)},
    {heartbeat,
      cuttlefish_duration:parse(str(HeartBeat), s)},
    {timeout, cuttlefish_duration:parse(str(Timeout), ms)},
    {auto_reconnect,
      cuttlefish_duration:parse(str(AutoReconnect), s)},
    {on_disconnect,
      {emqx_bridge_rabbit_actions, disconnect, []}},
    {pool_size, PoolSize}],
  PoolName = pool_name(ResId),
  start_resource(ResId, PoolName, Options),
  #{<<"pool">> => PoolName}.

start_resource(ResId, PoolName, Options) ->
  case ecpool:start_sup_pool(PoolName,
    emqx_bridge_rabbit_actions,
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
              [bridge_rabbit, ResId]}
          end,
            mfa =>
            {emqx_bridge_rabbit_actions,
              start_resource,
              3},
            line => 218})
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
              "~p, ~p",
              [bridge_rabbit, ResId, Reason]}
          end,
            mfa =>
            {emqx_bridge_rabbit_actions,
              start_resource,
              3},
            line => 223})
      end,
      error({{bridge_rabbit, ResId}, create_failed})
  end.

-spec on_get_resource_status(ResId :: binary(),
    Params :: map()) -> Status :: map().

on_get_resource_status(_ResId,
    #{<<"pool">> := PoolName}) ->
  Status = [begin
              {ok, {_RabbitConn, RabbitCh}} =
                ecpool_worker:client(Worker),
              is_process_alive(RabbitCh)
            end
    || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
  #{is_alive =>
  length(Status) > 0 andalso
    lists:all(fun (St) -> St =:= true end, Status)}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [bridge_rabbit, ResId]}
      end,
        mfa =>
        {emqx_bridge_rabbit_actions, on_resource_destroy, 2},
        line => 236})
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
              [bridge_rabbit, ResId]}
          end,
            mfa =>
            {emqx_bridge_rabbit_actions,
              on_resource_destroy,
              2},
            line => 239})
      end;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource ~p failed, ResId: ~p, ~p",
              [bridge_rabbit, ResId, Reason]}
          end,
            mfa =>
            {emqx_bridge_rabbit_actions,
              on_resource_destroy,
              2},
            line => 241})
      end,
      error({{bridge_rabbit, ResId}, destroy_failed})
  end.

on_action_create_data_to_rabbit(ActId,
    Params = #{<<"pool">> := PoolName,
      <<"exchange">> := Exchange,
      <<"exchange_type">> := ExchangeType,
      <<"routing_key">> := RoutingKey,
      <<"payload_tmpl">> :=
      PayloadTmpl}) ->
  PayloadTks = emqx_rule_utils:preproc_tmpl(PayloadTmpl),
  Durable = maps:get(<<"durable">>, Params, false),
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++ "Initiating Action ~p",
          [on_action_create_data_to_rabbit]}
      end,
        mfa =>
        {emqx_bridge_rabbit_actions,
          on_action_create_data_to_rabbit,
          2},
        line => 252})
  end,
  ecpool:with_client(PoolName,
    fun ({_, Ch}) ->
      amqp_channel:call(Ch,
        #'exchange.declare'{exchange =
        Exchange,
          type =
          ExchangeType,
          durable =
          Durable})
    end),
  {[{'Params', Params},
    {'PoolName', PoolName},
    {'Exchange', Exchange},
    {'ExchangeType', ExchangeType},
    {'RoutingKey', RoutingKey},
    {'PayloadTmpl', PayloadTmpl},
    {'ActId', ActId},
    {'PayloadTks', PayloadTks},
    {'Durable', Durable}],
    Params}.

on_action_data_to_rabbit(Msg,
    _Envs = #{'__bindings__' :=
    #{'PoolName' := PoolName,
      'Exchange' := Exchange,
      'RoutingKey' := RoutingKey,
      'PayloadTks' := PayloadTks,
      'ActId' := ActId}}) ->
  Method = #'basic.publish'{exchange = Exchange,
    routing_key = RoutingKey},
  AmqpMsg = #amqp_msg{props = #'P_basic'{headers = []},
    payload = format_data(PayloadTks, Msg)},
  ecpool:with_client(PoolName,
    fun ({_, Ch}) ->
      amqp_channel:cast(Ch, Method, AmqpMsg),
      emqx_rule_metrics:inc_actions_success(ActId)
    end).

format_data([], Msg) -> emqx_json:encode(Msg);
format_data(Tokens, Msg) ->
  emqx_rule_utils:proc_tmpl(Tokens, Msg).

connect(Options) ->
  {ok, RabbitConn} =
    amqp_connection:start(parse_opt(Options,
      #amqp_params_network{})),
  {ok, RabbitCh} =
    amqp_connection:open_channel(RabbitConn),
  {ok,
    {RabbitConn, RabbitCh},
    #{supervisees => [RabbitConn, RabbitCh]}}.

pool_name(ResId) ->
  list_to_atom("bridge_rabbit:" ++ str(ResId)).

host(Server) when is_binary(Server) ->
  case string:split(Server, ":") of
    [Host, Port] -> {Host, binary_to_integer(Port)};
    [Host] -> {Host, 5672}
  end.

disconnect({RabbitConn, _RabbitCh}) ->
  case is_process_alive(RabbitConn) of
    true -> catch amqp_connection:close(RabbitConn);
    false -> ok
  end.

parse_opt([], Params) -> Params;
parse_opt([{host, Host} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{host = Host});
parse_opt([{port, Port} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{port = Port});
parse_opt([{username, Username} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{username =
    iolist_to_binary(Username)});
parse_opt([{password, Password} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{password =
    iolist_to_binary(Password)});
parse_opt([{timeout, Timeout} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{connection_timeout =
    Timeout});
parse_opt([{virtual_host, VHost} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{virtual_host =
    iolist_to_binary(VHost)});
parse_opt([{heartbeat, Heartbeat} | Opts], Params) ->
  parse_opt(Opts,
    Params#amqp_params_network{heartbeat = Heartbeat});
parse_opt([_ | Opts], Params) ->
  parse_opt(Opts, Params).

logger_header() -> "".
