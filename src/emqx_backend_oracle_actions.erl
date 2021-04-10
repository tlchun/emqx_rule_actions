%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:59
%%%-------------------------------------------------------------------
-module(emqx_backend_oracle_actions).
-author("root").

-export([logger_header/0]).

-behaviour(ecpool_worker).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").


-import(emqx_rule_utils, [str/1]).

-export([connect/1]).

-export([async_call_back/3,
  batcher_flush/2,
  batch_insert/3,
  query/2]).

-export([on_resource_create/2,
  on_get_resource_status/2,
  on_resource_destroy/2]).

-export([on_action_create_data_to_oracle/2,
  on_action_data_to_oracle/2,
  on_action_destroy_data_to_oracle/2]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en =>
  <<79, 114, 97, 99, 108, 101, 32, 68, 97, 116, 97, 98,
    97, 115, 101, 32, 68, 97, 116, 97, 98, 97, 115,
    101>>,
    zh =>
    <<79, 114, 97, 99, 108, 101, 32, 68, 97, 116, 97, 98,
      97, 115, 101, 32, 230, 149, 176, 230, 141, 174,
      229, 186, 147>>},
  destroy => on_resource_destroy, name => backend_oracle,
  params =>
  #{auto_reconnect =>
  #{default => true,
    description =>
    #{en =>
    <<73, 102, 32, 114, 101, 45, 116, 114,
      121, 32, 119, 104, 101, 110, 32, 116,
      104, 101, 32, 99, 111, 110, 110, 101,
      99, 116, 105, 111, 110, 32, 108, 111,
      115, 116>>,
      zh =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 232,
        191, 158, 230, 142, 165, 230, 150, 173,
        229, 188, 128, 230, 151, 182, 230, 152,
        175, 229, 144, 166, 233, 135, 141, 232,
        191, 158>>},
    order => 6,
    title =>
    #{en =>
    <<69, 110, 97, 98, 108, 101, 32, 82, 101,
      99, 111, 110, 110, 101, 99, 116>>,
      zh =>
      <<230, 152, 175, 229, 144, 166, 233, 135,
        141, 232, 191, 158>>},
    type => boolean},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<80, 97, 115, 115, 119, 111, 114, 100,
        32, 102, 111, 114, 32, 79, 114, 97, 99,
        108, 101, 32, 68, 97, 116, 97, 98, 97,
        115, 101>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 229,
          175, 134, 231, 160, 129>>},
      order => 5, required => true,
      title =>
      #{en =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 229,
          175, 134, 231, 160, 129>>},
      type => string},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<84, 104, 101, 32, 115, 105, 122, 101,
        32, 111, 102, 32, 99, 111, 110, 110,
        101, 99, 116, 105, 111, 110, 32, 112,
        111, 111, 108, 32, 102, 111, 114, 32,
        79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 232,
          191, 158, 230, 142, 165, 230, 177, 160,
          229, 164, 167, 229, 176, 143>>},
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
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 49,
      53, 50, 49>>,
      description =>
      #{en =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 83, 101,
        114, 118, 101, 114, 32, 65, 100, 100,
        114, 101, 115, 115>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 230,
          156, 141, 229, 138, 161, 229, 153, 168,
          229, 156, 176, 229, 157, 128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 83, 101,
        114, 118, 101, 114>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 230,
          156, 141, 229, 138, 161, 229, 153,
          168>>},
      type => string},
    sid =>
    #{description =>
    #{en =>
    <<83, 105, 100, 32, 102, 111, 114, 32,
      79, 114, 97, 99, 108, 101, 32, 68, 97,
      116, 97, 98, 97, 115, 101>>,
      zh =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 83, 105,
        100, 32, 229, 144, 141, 231, 167,
        176>>},
      order => 3, required => true,
      title =>
      #{en =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 83, 105,
        100>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 83, 105,
          100>>},
      type => string},
    user =>
    #{default => <<>>,
      description =>
      #{en =>
      <<85, 115, 101, 114, 32, 102, 111, 114,
        32, 79, 114, 97, 99, 108, 101, 32, 68,
        97, 116, 97, 98, 97, 115, 101>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 231,
          148, 168, 230, 136, 183>>},
      order => 4, required => true,
      title =>
      #{en =>
      <<79, 114, 97, 99, 108, 101, 32, 68, 97,
        116, 97, 98, 97, 115, 101, 32, 85, 115,
        101, 114>>,
        zh =>
        <<79, 114, 97, 99, 108, 101, 32, 68, 97,
          116, 97, 98, 97, 115, 101, 32, 231,
          148, 168, 230, 136, 183>>},
      type => string}},
  status => on_get_resource_status,
  title =>
  #{en =>
  <<79, 114, 97, 99, 108, 101, 32, 68, 97, 116, 97, 98,
    97, 115, 101>>,
    zh =>
    <<79, 114, 97, 99, 108, 101, 32, 68, 97, 116, 97, 98,
      97, 115, 101>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_oracle,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 100, 97, 116, 97, 32,
    116, 111, 32, 79, 114, 97, 99, 108, 101, 32, 68, 97,
    116, 97, 98, 97, 115, 101>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 79, 114, 97, 99, 108,
      101, 32, 68, 97, 116, 97, 98, 97, 115, 101, 32, 230,
      149, 176, 230, 141, 174, 229, 186, 147>>},
  destroy => on_action_destroy_data_to_oracle,
  for => '$any', name => data_to_oracle,
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
    <<83, 81, 76, 32, 84, 101, 109, 112, 108,
      97, 116, 101, 32, 119, 105, 116, 104, 32,
      80, 108, 97, 99, 101, 104, 111, 108, 100,
      101, 114, 115, 32, 102, 111, 114, 32, 73,
      110, 115, 101, 114, 116, 105, 110, 103,
      47, 85, 112, 100, 97, 116, 105, 110, 103,
      32, 68, 97, 116, 97, 32, 116, 111, 32,
      79, 114, 97, 99, 108, 101, 32, 68, 97,
      116, 97, 98, 97, 115, 101>>,
      zh =>
      <<229, 140, 133, 229, 144, 171, 228, 186,
        134, 229, 141, 160, 228, 189, 141, 231,
        172, 166, 231, 154, 132, 32, 83, 81, 76,
        32, 230, 168, 161, 230, 157, 191, 239,
        188, 140, 231, 148, 168, 228, 186, 142,
        230, 143, 146, 229, 133, 165, 230, 136,
        150, 230, 155, 180, 230, 150, 176, 230,
        149, 176, 230, 141, 174, 229, 136, 176,
        32, 79, 114, 97, 99, 108, 101, 32, 68,
        97, 116, 97, 98, 97, 115, 101, 32, 230,
        149, 176, 230, 141, 174, 229, 186,
        147>>},
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
  <<68, 97, 116, 97, 32, 116, 111, 32, 79, 114, 97, 99,
    108, 101, 32, 68, 97, 116, 97, 98, 97, 115, 101>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 79, 114, 97, 99, 108,
      101, 32, 68, 97, 116, 97, 98, 97, 115, 101>>},
  types => [backend_oracle]}).

-vsn("4.2.5").

on_resource_create(ResId,
    Config = #{<<"server">> := Server, <<"sid">> := Sid,
      <<"user">> := User,
      <<"password">> := Password}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_oracle, ResId]}
      end,
        mfa =>
        {emqx_backend_oracle_actions, on_resource_create, 2},
        line => 230})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(jamdb_oracle),
  {Host, Port} =
    emqx_rule_actions_utils:parse_server(Server, 1521),
  Opts = [{host, Host},
    {port, Port},
    {user, str(User)},
    {password, str(Password)},
    {sid, str(Sid)},
    {auto_reconnect,
      case maps:get(<<"auto_reconnect">>, Config, true) of
        true -> 15;
        false -> false
      end},
    {pool_size, maps:get(<<"pool_size">>, Config, 8)}],
  PoolName = list_to_atom("oracle:" ++ str(ResId)),
  emqx_rule_actions_utils:start_pool(PoolName,
    emqx_backend_oracle_actions,
    Opts),
  #{<<"pool">> => PoolName}.

-spec on_get_resource_status(ResId :: binary(),
    Params :: map()) -> Status :: map().

on_get_resource_status(_ResId,
    #{<<"pool">> := PoolName}) ->
  #{is_alive =>
  emqx_rule_actions_utils:is_resource_alive(PoolName,
    fun (Conn) ->
      {ok, _} =
        jamdb_oracle:sql_query(Conn,
          "select 1 from dual"),
      ok
    end)}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [backend_oracle, ResId]}
      end,
        mfa =>
        {emqx_backend_oracle_actions, on_resource_destroy, 2},
        line => 261})
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
              [backend_oracle, ResId]}
          end,
            mfa =>
            {emqx_backend_oracle_actions,
              on_resource_destroy,
              2},
            line => 265})
      end;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource ~p failed, ResId: ~p, ~p",
              [backend_oracle, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_oracle_actions,
              on_resource_destroy,
              2},
            line => 268})
      end,
      error({{backend_oracle, ResId}, destroy_failed})
  end.

-spec on_action_create_data_to_oracle(Id :: binary(),
    #{}) -> fun((Msg :: map()) -> any()).

on_action_create_data_to_oracle(ActId,
    Opts = #{<<"enable_batch">> :=
    EnableBatch = true,
      <<"pool">> := PoolName,
      <<"sql">> := SqlTemplate}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Batch Action ~p, SqlTemplate: ~p",
          [on_action_create_data_to_oracle,
            SqlTemplate]}
      end,
        mfa =>
        {emqx_backend_oracle_actions,
          on_action_create_data_to_oracle,
          2},
        line => 274})
  end,
  BatcherPName = list_to_atom("oracle_batcher:" ++
  str(ActId)),
  SyncTimeout = maps:get(<<"sync_timeout">>, Opts, 5000),
  InsertMode = maps:get(<<"insert_mode">>,
    Opts,
    <<"sync">>),
  emqx_rule_actions_utils:start_batcher_pool(BatcherPName,
    emqx_backend_oracle_actions,
    Opts,
    case InsertMode of
      <<"sync">> ->
        {PoolName,
          ActId,
          {sync, SyncTimeout}};
      <<"async">> ->
        {PoolName, ActId, async}
    end),
  {ok, _} =
    emqx_rule_actions_utils:split_insert_sql(SqlTemplate),
  SqlTemplate1 = re:replace(SqlTemplate,
    ",?\r?\n *",
    "",
    [{return, binary}, global, unicode]),
  SqlTemplate2 = re:replace(SqlTemplate1,
    ";$",
    "",
    [{return, binary}, unicode]),
  PayloadTks = emqx_rule_utils:preproc_tmpl(SqlTemplate2),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
    {'SqlTemplate', SqlTemplate},
    {'EnableBatch', EnableBatch},
    {'ActId', ActId},
    {'BatcherPName', BatcherPName},
    {'SyncTimeout', SyncTimeout},
    {'InsertMode', InsertMode},
    {'SqlTemplate1', SqlTemplate1},
    {'SqlTemplate2', SqlTemplate2},
    {'PayloadTks', PayloadTks}],
    Opts#{batcher_name => BatcherPName}};
on_action_create_data_to_oracle(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"sql">> := SqlTemplate}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Action ~p, SqlTemplate: ~p",
          [on_action_create_data_to_oracle,
            SqlTemplate]}
      end,
        mfa =>
        {emqx_backend_oracle_actions,
          on_action_create_data_to_oracle,
          2},
        line => 290})
  end,
  SqlTemplate1 = re:replace(SqlTemplate,
    ",?\r?\n *",
    "",
    [{return, binary}, global, unicode]),
  SqlTemplate2 = re:replace(SqlTemplate1,
    ";$",
    "",
    [{return, binary}, unicode]),
  PayloadTks = emqx_rule_utils:preproc_tmpl(SqlTemplate2),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
    {'SqlTemplate', SqlTemplate},
    {'ActId', ActId},
    {'SqlTemplate1', SqlTemplate1},
    {'SqlTemplate2', SqlTemplate2},
    {'PayloadTks', PayloadTks}],
    Opts}.

on_action_data_to_oracle(Msg,
    _Envs = #{'__bindings__' :=
    #{'EnableBatch' := true,
      'ActId' := ActId,
      'BatcherPName' := BatcherPName,
      'PayloadTks' := PayloadTks,
      'InsertMode' := InsertMode,
      'SyncTimeout' := SyncTimeout}}) ->
  Sql = emqx_rule_utils:proc_tmpl(PayloadTks, Msg),
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Oracle Database Pool: ~p, SQL: ~p",
          [BatcherPName, Sql]}
      end,
        mfa =>
        {emqx_backend_oracle_actions,
          on_action_data_to_oracle,
          2},
        line => 306})
  end,
  case ecpool:pick_and_do(BatcherPName,
    emqx_rule_actions_batcher:accumulate_mfa(InsertMode,
      Sql,
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
            {emqx_backend_oracle_actions,
              on_action_data_to_oracle,
              2},
            line => 309})
      end;
    {ok, Result} ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "oracle query successfully, result: ~p",
              [Result]}
          end,
            mfa =>
            {emqx_backend_oracle_actions,
              on_action_data_to_oracle,
              2},
            line => 311})
      end,
      emqx_rule_metrics:inc_actions_success(ActId);
    {error, _Type, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Oracle Database insert failed reason: ~p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_oracle_actions,
              on_action_data_to_oracle,
              2},
            line => 314})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {error, Reason}
  end;
on_action_data_to_oracle(Msg,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'PayloadTks' := PayloadTks}}) ->
  Sql = emqx_rule_utils:proc_tmpl(PayloadTks, Msg),
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Oracle Database Pool: ~p, Query SQL: ~p",
          [PoolName, Sql]}
      end,
        mfa =>
        {emqx_backend_oracle_actions,
          on_action_data_to_oracle,
          2},
        line => 326})
  end,
  case ecpool:pick_and_do(PoolName,
    {emqx_backend_oracle_actions, query, [Sql]},
    no_handover)
  of
    {ok, Result} ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "oracle query successfully, result: ~p",
              [Result]}
          end,
            mfa =>
            {emqx_backend_oracle_actions,
              on_action_data_to_oracle,
              2},
            line => 329})
      end,
      emqx_rule_metrics:inc_actions_success(ActId);
    {error, _Type, Reason} ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "oracle query failed, resp: ~p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_oracle_actions,
              on_action_data_to_oracle,
              2},
            line => 332})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason}
  end.

on_action_destroy_data_to_oracle(_ActId,
    #{batcher_pname := BatcherPName}) ->
  emqx_rule_actions_utils:stop_batcher_pool(BatcherPName);
on_action_destroy_data_to_oracle(_ActId, _) -> ok.

connect(Opts) ->
  ConnectOpts = [lists:keyfind(host, 1, Opts),
    lists:keyfind(port, 1, Opts),
    lists:keyfind(sid, 1, Opts),
    lists:keyfind(user, 1, Opts),
    lists:keyfind(password, 1, Opts),
    {timeout, 30000},
    {app_name, "EMQ X Data To Oracle Action"}],
  jamdb_oracle:start_link(ConnectOpts).

batcher_flush(Batch, State) ->
  {insert_all(Batch,
    {emqx_backend_oracle_actions, batch_insert, [State]}),
    State}.

batch_insert(SQL, _BatchLen,
    {PoolName, _ActId, {sync, SyncTimeout}}) ->
  ecpool:pick_and_do(PoolName,
    {emqx_backend_oracle_actions, query, [SQL]},
    {handover, SyncTimeout});
batch_insert(SQL, BatchLen, {PoolName, ActId, async}) ->
  ecpool:pick_and_do(PoolName,
    {emqx_backend_oracle_actions, query, [SQL]},
    {handover_async,
      {emqx_backend_oracle_actions,
        async_call_back,
        [ActId, BatchLen]}}).

async_call_back({ok, Result}, ActId, BatchLen) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "async insert success, result: ~0p",
          [Result]}
      end,
        mfa =>
        {emqx_backend_oracle_actions, async_call_back, 3},
        line => 363})
  end,
  emqx_rule_metrics:inc_actions_success(ActId, BatchLen);
async_call_back({error, _Type, Reason}, ActId,
    BatchLen) ->
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
        {emqx_backend_oracle_actions, async_call_back, 3},
        line => 366})
  end,
  emqx_rule_metrics:inc_actions_error(ActId, BatchLen).

query(Conn, Sql) when is_pid(Conn) ->
  jamdb_oracle:sql_query(Conn, str(Sql)).

insert_all(Batch, InsertFun) ->
  {FromList, SQLList} = lists:unzip(Batch),
  BatchSQL = join_insert_all(SQLList),
  Result = emqx_rule_actions_utils:safe_exec(InsertFun,
    [BatchSQL, length(SQLList)]),
  [{From, Result} || From <- FromList].

join_insert_all(InsertSql) when is_list(InsertSql) ->
  join_insert_all(InsertSql, <<"INSERT ALL">>).

join_insert_all([], Acc) ->
  <<Acc/binary, "select 1 from dual">>;
join_insert_all([<<"insert", Sql/binary>> | Rest],
    Acc) ->
  join_insert_all(Rest,
    <<Acc/binary, " ", Sql/binary, " ">>);
join_insert_all([<<"INSERT", Sql/binary>> | Rest],
    Acc) ->
  join_insert_all(Rest,
    <<Acc/binary, " ", Sql/binary, " ">>).

logger_header() -> "".
