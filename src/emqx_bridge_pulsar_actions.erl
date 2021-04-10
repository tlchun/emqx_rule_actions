%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午7:00
%%%-------------------------------------------------------------------
-module(emqx_bridge_pulsar_actions).
-author("root").

-export([logger_header/0]).
-include("../include/emqx_bridge_pulsar.hrl").
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").


-export([on_resource_create/2,
  on_resource_destroy/2,
  on_resource_status/2]).

-export([on_action_create_data_to_pulsar/2,
  on_action_data_to_pulsar/2,
  on_action_destroy_data_to_pulsar/2]).

-export([pulsar_callback/2]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en => <<80, 117, 108, 115, 97, 114>>,
    zh => <<80, 117, 108, 115, 97, 114>>},
  destroy => on_resource_destroy, name => bridge_pulsar,
  params =>
  #{batch_size =>
  #{default => <<49, 48, 48>>,
    description =>
    #{en =>
    <<77, 97, 120, 32, 66, 97, 116, 99, 104,
      32, 83, 105, 122, 101>>,
      zh =>
      <<230, 156, 128, 229, 164, 167, 230, 137,
        185, 229, 164, 132, 231, 144, 134, 230,
        149, 176>>},
    order => 3,
    title =>
    #{en =>
    <<77, 97, 120, 32, 66, 97, 116, 99, 104,
      32, 83, 105, 122, 101>>,
      zh =>
      <<230, 156, 128, 229, 164, 167, 230, 137,
        185, 229, 164, 132, 231, 144, 134, 230,
        149, 176>>},
    type => string},
    compression =>
    #{default =>
    <<110, 111, 95, 99, 111, 109, 112, 114, 101,
      115, 115, 105, 111, 110>>,
      description =>
      #{en =>
      <<67, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110>>,
        zh => <<229, 142, 139, 231, 188, 169>>},
      enum =>
      [<<110, 111, 95, 99, 111, 109, 112, 114, 101,
        115, 115, 105, 111, 110>>,
        <<99, 111, 109, 112, 114, 101, 115, 115, 105,
          111, 110>>],
      order => 4,
      title =>
      #{en =>
      <<67, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110>>,
        zh => <<229, 142, 139, 231, 188, 169>>},
      type => string},
    send_buffer =>
    #{default => <<49, 48, 50, 52, 75, 66>>,
      description =>
      #{en =>
      <<83, 111, 99, 107, 101, 116, 32, 83,
        101, 110, 100, 32, 66, 117, 102, 102,
        101, 114>>,
        zh =>
        <<229, 165, 151, 230, 142, 165, 229, 173,
          151, 229, 143, 145, 233, 128, 129, 230,
          182, 136, 230, 129, 175, 231, 154, 132,
          231, 188, 147, 229, 134, 178, 229, 140,
          186, 229, 164, 167, 229, 176, 143>>},
      order => 5,
      title =>
      #{en =>
      <<83, 101, 110, 100, 32, 66, 117, 102,
        102, 101, 114, 32, 83, 105, 122, 101>>,
        zh =>
        <<229, 143, 145, 233, 128, 129, 230, 182,
          136, 230, 129, 175, 231, 154, 132, 231,
          188, 147, 229, 134, 178, 229, 140, 186,
          229, 164, 167, 229, 176, 143>>},
      type => string},
    servers =>
    #{default =>
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 54,
      54, 53, 48>>,
      description =>
      #{en =>
      <<80, 117, 108, 115, 97, 114, 32, 83,
        101, 114, 118, 101, 114, 32, 65, 100,
        100, 114, 101, 115, 115, 44, 32, 77,
        117, 108, 116, 105, 112, 108, 101, 32,
        110, 111, 100, 101, 115, 32, 115, 101,
        112, 97, 114, 97, 116, 101, 100, 32,
        98, 121, 32, 99, 111, 109, 109, 97,
        115>>,
        zh =>
        <<80, 117, 108, 115, 97, 114, 32, 230,
          156, 141, 229, 138, 161, 229, 153, 168,
          229, 156, 176, 229, 157, 128, 44, 32,
          229, 164, 154, 232, 138, 130, 231, 130,
          185, 228, 189, 191, 231, 148, 168, 233,
          128, 151, 229, 143, 183, 229, 136, 134,
          233, 154, 148>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<80, 117, 108, 115, 97, 114, 32, 83,
        101, 114, 118, 101, 114>>,
        zh =>
        <<80, 117, 108, 115, 97, 114, 32, 230,
          156, 141, 229, 138, 161, 229, 153,
          168>>},
      type => string},
    sync_timeout =>
    #{default => <<51, 115>>,
      description =>
      #{en =>
      <<83, 121, 110, 99, 32, 84, 105, 109,
        101, 111, 117, 116>>,
        zh =>
        <<229, 144, 140, 230, 173, 165, 232, 176,
          131, 231, 148, 168, 232, 182, 133, 230,
          151, 182, 230, 151, 182, 233, 151,
          180>>},
      order => 2,
      title =>
      #{en =>
      <<83, 121, 110, 99, 32, 84, 105, 109,
        101, 111, 117, 116>>,
        zh =>
        <<229, 144, 140, 230, 173, 165, 232, 176,
          131, 231, 148, 168, 232, 182, 133, 230,
          151, 182, 230, 151, 182, 233, 151,
          180>>},
      type => string}},
  status => on_resource_status,
  title =>
  #{en => <<80, 117, 108, 115, 97, 114>>,
    zh => <<80, 117, 108, 115, 97, 114>>}}).

-rule_action(#{category => data_forward,
  create => on_action_create_data_to_pulsar,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 80, 117, 108, 115, 97, 114>>,
    zh =>
    <<230, 161, 165, 230, 142, 165, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 80, 117, 108, 115, 97,
      114>>},
  destroy => on_action_destroy_data_to_pulsar,
  for => '$any', name => data_to_pulsar,
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
    key =>
    #{default => <<110, 111, 110, 101>>,
      description =>
      #{en =>
      <<83, 116, 114, 97, 116, 101, 103, 121, 32,
        75, 101, 121, 44, 32, 111, 110, 108, 121,
        32, 116, 97, 107, 101, 32, 101, 102, 102,
        101, 99, 116, 32, 119, 104, 101, 110, 32,
        115, 116, 114, 97, 116, 101, 103, 121,
        32, 105, 115, 32, 102, 105, 114, 115,
        116, 95, 107, 101, 121, 95, 100, 105,
        115, 112, 97, 116, 99, 104>>,
        zh =>
        <<83, 116, 114, 97, 116, 101, 103, 121, 32,
          75, 101, 121, 44, 32, 228, 187, 133, 229,
          156, 168, 231, 173, 150, 231, 149, 165,
          228, 184, 186, 32, 102, 105, 114, 115,
          116, 95, 107, 101, 121, 95, 100, 105,
          115, 112, 97, 116, 99, 104, 32, 230, 151,
          182, 231, 148, 159, 230, 149, 136>>},
      enum =>
      [<<99, 108, 105, 101, 110, 116, 105, 100>>,
        <<117, 115, 101, 114, 110, 97, 109, 101>>,
        <<116, 111, 112, 105, 99>>,
        <<110, 111, 110, 101>>],
      order => 4,
      title =>
      #{en =>
      <<83, 116, 114, 97, 116, 101, 103, 121, 32,
        75, 101, 121>>,
        zh =>
        <<83, 116, 114, 97, 116, 101, 103, 121, 32,
          75, 101, 121>>},
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
    strategy =>
    #{default => <<114, 97, 110, 100, 111, 109>>,
      description =>
      #{en =>
      <<80, 114, 111, 100, 117, 99, 101, 32, 83,
        116, 114, 97, 116, 101, 103, 121>>,
        zh =>
        <<80, 114, 111, 100, 117, 99, 101, 32, 231,
          173, 150, 231, 149, 165>>},
      enum =>
      [<<114, 97, 110, 100, 111, 109>>,
        <<114, 111, 117, 110, 100, 114, 111, 98, 105,
          110>>,
        <<102, 105, 114, 115, 116, 95, 107, 101, 121,
          95, 100, 105, 115, 112, 97, 116, 99, 104>>],
      order => 3,
      title =>
      #{en =>
      <<80, 114, 111, 100, 117, 99, 101, 32, 83,
        116, 114, 97, 116, 101, 103, 121>>,
        zh =>
        <<80, 114, 111, 100, 117, 99, 101, 32, 231,
          173, 150, 231, 149, 165>>},
      type => string},
    topic =>
    #{description =>
    #{en =>
    <<80, 117, 108, 115, 97, 114, 32, 84, 111,
      112, 105, 99>>,
      zh =>
      <<80, 117, 108, 115, 97, 114, 32, 228, 184,
        187, 233, 162, 152>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<80, 117, 108, 115, 97, 114, 32, 84, 111,
        112, 105, 99>>,
        zh =>
        <<80, 117, 108, 115, 97, 114, 32, 228, 184,
          187, 233, 162, 152>>},
      type => string},
    type =>
    #{default => <<115, 121, 110, 99>>,
      description =>
      #{en =>
      <<80, 114, 111, 100, 117, 99, 101, 32, 84,
        121, 112, 101>>,
        zh =>
        <<80, 114, 111, 100, 117, 99, 101, 32, 231,
          177, 187, 229, 158, 139>>},
      enum =>
      [<<115, 121, 110, 99>>,
        <<97, 115, 121, 110, 99>>],
      order => 2,
      title =>
      #{en =>
      <<80, 114, 111, 100, 117, 99, 101, 32, 84,
        121, 112, 101>>,
        zh =>
        <<80, 114, 111, 100, 117, 99, 101, 32, 231,
          177, 187, 229, 158, 139>>},
      type => string}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 98, 114, 105, 100, 103, 101, 32,
    116, 111, 32, 80, 117, 108, 115, 97, 114>>,
    zh =>
    <<230, 161, 165, 230, 142, 165, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 80, 117, 108, 115, 97,
      114>>},
  types => [bridge_pulsar]}).

-vsn("4.2.5").

on_resource_create(ResId,
    Config = #{<<"servers">> := Servers}) ->
  {ok, _} = application:ensure_all_started(pulsar),
  Timeout =
    cuttlefish_duration:parse(str(maps:get(<<"sync_timeout">>,
      Config))),
  BatchSize = binary_to_integer(maps:get(<<"batch_size">>,
    Config,
    <<"0">>)),
  Compression = atom(maps:get(<<"compression">>, Config)),
  Sndbuf =
    cuttlefish_bytesize:parse(str(maps:get(<<"send_buffer">>,
      Config))),
  ClientId = clientid(ResId),
  Servers1 = format_servers(Servers),
  ClientCfg = #{},
  start_resource(ClientId, Servers1, ClientCfg),
  #{<<"clientid">> => ClientId, <<"timeout">> => Timeout,
    <<"batch_size">> => BatchSize,
    <<"compression">> => Compression,
    <<"servers">> => Servers1, <<"sndbuf">> => Sndbuf}.

start_resource(ClientId, Servers, ClientCfg) ->
  {ok, _Pid} = pulsar:ensure_supervised_client(ClientId,
    Servers,
    ClientCfg).

on_resource_destroy(ResId,
    #{<<"clientid">> := ClientId}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [bridge_pulsar, ResId]}
      end,
        mfa =>
        {emqx_bridge_pulsar_actions, on_resource_destroy, 2},
        line => 184})
  end,
  ok = pulsar:stop_and_delete_supervised_client(ClientId).

on_resource_status(_ResId,
    #{<<"clientid">> := ClientId}) ->
  case pulsar_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      #{is_alive => pulsar_client:get_status(Pid)};
    _ -> #{is_alive => false}
  end.

on_action_create_data_to_pulsar(ActId,
    Params = #{<<"clientid">> := ClientId,
      <<"strategy">> := Strategy,
      <<"type">> := Type,
      <<"topic">> := Topic,
      <<"timeout">> := Timeout,
      <<"sndbuf">> := Sndbuf,
      <<"payload_tmpl">> :=
      PayloadTmpl}) ->
  PayloadTks = emqx_rule_utils:preproc_tmpl(PayloadTmpl),
  Key = maps:get(<<"key">>, Params, <<"">>),
  ProducerOpts = #{strategy => atom(Strategy),
    batch_size => maps:get(<<"batch_size">>, Params, 0),
    compression =>
    maps:get(<<"compression">>, Params, no_compression),
    tcp_opts => [{sndbuf, Sndbuf}],
    name => binary_to_atom(ActId, utf8),
    callback =>
    {emqx_bridge_pulsar_actions,
      pulsar_callback,
      [ActId]}},
  {ok, Producers} =
    pulsar:ensure_supervised_producers(ClientId,
      binary_to_list(Topic),
      ProducerOpts),
  {[{'Params', Params},
    {'Topic', Topic},
    {'ClientId', ClientId},
    {'Timeout', Timeout},
    {'Type', Type},
    {'Sndbuf', Sndbuf},
    {'Strategy', Strategy},
    {'PayloadTmpl', PayloadTmpl},
    {'ActId', ActId},
    {'PayloadTks', PayloadTks},
    {'Key', Key},
    {'ProducerOpts', ProducerOpts},
    {'Producers', Producers}],
    Params#{<<"producers">> => Producers}}.

on_action_data_to_pulsar(Msg,
    _Envs = #{'__bindings__' :=
    #{'Key' := Key,
      'PayloadTks' := PayloadTks,
      'Type' := Type, 'Timeout' := Timeout,
      'Producers' := Producers,
      'ActId' := ActId}}) ->
  produce(ActId,
    Producers,
    feed_key(Key, Msg),
    format_data(PayloadTks, Msg),
    Type,
    Timeout).

on_action_destroy_data_to_pulsar(_Id,
    #{<<"producers">> := Producers}) ->
  ok =
    pulsar:stop_and_delete_supervised_producers(Producers).

format_data([], Msg) -> emqx_json:encode(Msg);
format_data(Tokens, Msg) ->
  emqx_rule_utils:proc_tmpl(Tokens, Msg).

feed_key(<<"none">>, _) -> <<>>;
feed_key(<<"clientid">>, Msg) ->
  maps:get(clientid,
    Msg,
    maps:get(<<"clientid">>, Msg, <<>>));
feed_key(<<"username">>, Msg) ->
  maps:get(username,
    Msg,
    maps:get(<<"username">>, Msg, <<>>));
feed_key(<<"topic">>, Msg) ->
  maps:get(topic, Msg, maps:get(<<"topic">>, Msg, <<>>)).

produce(ActId, Producers, Key, JsonMsg, Type,
    Timeout) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++ "Produce payload:~p",
          [JsonMsg]}
      end,
        mfa => {emqx_bridge_pulsar_actions, produce, 6},
        line => 240})
  end,
  case Type of
    <<"sync">> ->
      pulsar:send_sync(Producers,
        [#{key => Key, value => JsonMsg}],
        Timeout),
      emqx_rule_metrics:inc_actions_success(ActId);
    <<"async">> ->
      pulsar:send(Producers,
        [#{key => Key, value => JsonMsg}])
  end.

format_servers(Servers) when is_binary(Servers) ->
  format_servers(str(Servers));
format_servers(Servers) ->
  ServerList = string:tokens(Servers, ", "),
  lists:map(fun (Server) ->
    case string:tokens(Server, ":") of
      [Domain] -> {Domain, 6650};
      [Domain, Port] -> {Domain, list_to_integer(Port)}
    end
            end,
    ServerList).

clientid(ResId) ->
  list_to_atom("bridge_pulsar:" ++ str(ResId)).

str(List) when is_list(List) -> List;
str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(Atom) when is_atom(Atom) -> atom_to_list(Atom).

atom(B) when is_binary(B) ->
  erlang:binary_to_atom(B, utf8);
atom(L) when is_list(L) -> erlang:list_to_atom(L);
atom(A) -> A.

pulsar_callback(_Resp, ActId) ->
  emqx_rule_metrics:inc_actions_success(ActId).

logger_header() -> "".
