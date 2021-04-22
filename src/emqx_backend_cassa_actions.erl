%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:58
%%%-------------------------------------------------------------------
-module(emqx_backend_cassa_actions).
-author("root").
-export([logger_header/0]).

-behaviour(ecpool_worker).

-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").

-export([async_call_back/3, batcher_flush/2]).
-export([batch_insert/3, ecql_query/3, batch_sql_insert/2]).
-export([connect/1]).

-import(emqx_rule_utils, [str/1]).
%% 资源创建，资源状态，资源销毁
-export([on_resource_create/2, on_get_resource_status/2, on_resource_destroy/2]).
%% 动作创建cassa,查询订阅数据，查询离线嘻嘻
-export([on_action_create_data_to_cassa/2, on_action_destroy_data_to_cassa/2, on_action_create_lookup_sub/2, on_action_create_offline_msg/2]).
%% 数据存到cassa,查询订阅数据，离线消息保存到cassa
-export([on_action_data_to_cassa/2, on_action_lookup_sub_to_cassa/2, on_action_offline_msg_to_cassa/2]).

-resource_type(#{create => on_resource_create, description =>
  #{en => <<67, 97, 115, 115, 97, 110, 100, 114, 97, 32, 68, 97, 116, 97, 98, 97, 115, 101>>,
    zh => <<67, 97, 115, 115, 97, 110, 100, 114, 97, 32, 230, 149, 176, 230, 141, 174, 229, 186, 147>>},
  destroy => on_resource_destroy, name => backend_cassa,
  params =>
  #{auto_reconnect =>
  #{default => true, description =>
    #{en => <<73, 102, 32, 82, 101, 45, 116, 114, 121, 32, 119, 104, 101, 110, 32, 116, 104, 101, 32, 67, 111, 110, 110, 101, 99, 116, 105, 111, 110, 32, 76, 111, 115, 116>>,
      zh => <<67, 97, 115, 115, 97, 110, 100, 114, 97, 32, 232, 191, 158, 230, 142, 165, 230, 150, 173, 229, 188, 128, 229, 144, 142, 232, 135, 170, 229, 138, 168, 233, 135, 141, 232, 191, 158>>},
    order => 5,
    title => #{en => <<69, 110, 97, 98, 108, 101, 32, 82, 101, 99, 111, 110, 110, 101, 99, 116>>, zh => <<230, 152, 175, 229, 144, 166, 233, 135, 141, 232, 191, 158>>},
    type => boolean},
    cafile =>
    #{default => <<>>, description =>
      #{en =>
      <<89, 111, 117, 114, 32, 115, 115, 108,
        32, 99, 97, 102, 105, 108, 101>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 83, 83, 76, 32, 67, 65, 232,
          175, 129, 228, 185, 166>>},
      order => 8,
      title =>
      #{en => <<83, 83, 76, 32, 67, 65>>,
        zh => <<83, 83, 76, 32, 67, 65>>},
      type => file},
    certfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<89, 111, 117, 114, 32, 115, 115, 108,
        32, 99, 101, 114, 116, 102, 105, 108,
        101>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 83, 83, 76, 32, 232, 175, 129,
          228, 185, 166>>},
      order => 10,
      title =>
      #{en => <<83, 83, 76, 32, 67, 101, 114, 116>>,
        zh => <<83, 83, 76, 32, 67, 101, 114, 116>>},
      type => file},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<89, 111, 117, 114, 32, 115, 115, 108,
        32, 107, 101, 121, 102, 105, 108,
        101>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 83, 83, 76, 32, 231, 167, 129,
          233, 146, 165>>},
      order => 9,
      title =>
      #{en => <<83, 83, 76, 32, 75, 101, 121>>,
        zh => <<83, 83, 76, 32, 75, 101, 121>>},
      type => file},
    keyspace =>
    #{description =>
    #{en =>
    <<75, 101, 121, 115, 112, 97, 99, 101,
      32, 78, 97, 109, 101, 32, 102, 111,
      114, 32, 67, 97, 115, 115, 97, 110,
      100, 114, 97>>,
      zh =>
      <<67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 75, 101, 121, 115, 112, 97, 99,
        101, 32, 229, 144, 141, 231, 167,
        176>>},
      order => 2, required => true,
      title =>
      #{en =>
      <<67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 75, 101, 121, 115, 112, 97, 99,
        101>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 75, 101, 121, 115, 97, 112, 99,
          101>>},
      type => string},
    nodes =>
    #{default =>
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 57,
      48, 52, 50>>,
      description =>
      #{en =>
      <<67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 83, 101, 114, 118, 101, 114,
        32, 65, 100, 100, 114, 101, 115, 115>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 230, 156, 141, 229, 138, 161,
          229, 153, 168, 229, 156, 176, 229, 157,
          128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 83, 101, 114, 118, 101, 114>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 230, 156, 141, 229, 138, 161,
          229, 153, 168>>},
      type => string},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<80, 97, 115, 115, 119, 111, 114, 100,
        32, 102, 111, 114, 32, 67, 97, 115,
        115, 97, 110, 100, 114, 97>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 229, 175, 134, 231, 160,
          129>>},
      order => 4,
      title =>
      #{en =>
      <<67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 80, 97, 115, 115, 119, 111,
        114, 100>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 229, 175, 134, 231, 160,
          129>>},
      type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<67, 111, 110, 110, 101, 99, 116, 105,
        111, 110, 32, 80, 111, 111, 108, 32,
        83, 105, 122, 101, 32, 102, 111, 114,
        32, 67, 97, 115, 115, 97, 110, 100,
        114, 97>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 232, 191, 158, 230, 142, 165,
          230, 177, 160, 229, 164, 167, 229, 176,
          143>>},
      order => 6,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    ssl =>
    #{default => false,
      description =>
      #{en =>
      <<73, 102, 32, 101, 110, 97, 98, 108,
        101, 32, 115, 115, 108>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 188,
          128, 229, 144, 175, 67, 97, 115, 115,
          97, 110, 100, 114, 97, 32, 83, 83,
          76>>},
      order => 7,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<229, 188, 128, 229, 144, 175, 83, 83,
          76, 233, 147, 190, 230, 142, 165>>},
      type => boolean},
    username =>
    #{default => <<>>,
      description =>
      #{en =>
      <<85, 115, 101, 114, 110, 97, 109, 101,
        32, 102, 111, 114, 32, 67, 97, 115,
        115, 97, 110, 100, 114, 97>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 231, 148, 168, 230, 136, 183,
          229, 144, 141>>},
      order => 3,
      title =>
      #{en =>
      <<67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 85, 115, 101, 114, 110, 97,
        109, 101>>,
        zh =>
        <<67, 97, 115, 115, 97, 110, 100, 114,
          97, 32, 231, 148, 168, 230, 136, 183,
          229, 144, 141>>},
      type => string},
    verify =>
    #{default => false,
      description =>
      #{en =>
      <<87, 104, 101, 116, 104, 101, 114, 32,
        116, 111, 32, 118, 101, 114, 105, 102,
        121, 32, 116, 104, 101, 32, 115, 101,
        114, 118, 101, 114, 32, 99, 101, 114,
        116, 105, 102, 105, 99, 97, 116, 101,
        46, 32, 66, 121, 32, 100, 101, 102, 97,
        117, 108, 116, 44, 32, 116, 104, 101,
        32, 99, 108, 105, 101, 110, 116, 32,
        119, 105, 108, 108, 32, 110, 111, 116,
        32, 118, 101, 114, 105, 102, 121, 32,
        116, 104, 101, 32, 115, 101, 114, 118,
        101, 114, 39, 115, 32, 99, 101, 114,
        116, 105, 102, 105, 99, 97, 116, 101,
        46, 32, 73, 102, 32, 118, 101, 114,
        105, 102, 105, 99, 97, 116, 105, 111,
        110, 32, 105, 115, 32, 114, 101, 113,
        117, 105, 114, 101, 100, 44, 32, 112,
        108, 101, 97, 115, 101, 32, 115, 101,
        116, 32, 105, 116, 32, 116, 111, 32,
        116, 114, 117, 101, 46>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 230, 160,
          161, 233, 170, 140, 230, 156, 141, 229,
          138, 161, 229, 153, 168, 232, 175, 129,
          228, 185, 166, 227, 128, 130, 32, 233,
          187, 152, 232, 174, 164, 229, 174, 162,
          230, 136, 183, 231, 171, 175, 228, 184,
          141, 228, 188, 154, 229, 142, 187, 230,
          160, 161, 233, 170, 140, 230, 156, 141,
          229, 138, 161, 229, 153, 168, 231, 154,
          132, 232, 175, 129, 228, 185, 166, 239,
          188, 140, 229, 166, 130, 230, 158, 156,
          233, 156, 128, 232, 166, 129, 230, 160,
          161, 233, 170, 140, 239, 188, 140, 232,
          175, 183, 232, 174, 190, 231, 189, 174,
          230, 136, 144, 116, 114, 117, 101, 227,
          128, 130>>},
      order => 11,
      title =>
      #{en =>
      <<86, 101, 114, 105, 102, 121, 32, 83,
        101, 114, 118, 101, 114, 32, 67, 101,
        114, 116, 102, 105, 108, 101>>,
        zh =>
        <<230, 160, 161, 233, 170, 140, 230, 156,
          141, 229, 138, 161, 229, 153, 168, 232,
          175, 129, 228, 185, 166>>},
      type => boolean}},
  status => on_get_resource_status,
  title =>
  #{en => <<67, 97, 115, 115, 97, 110, 100, 114, 97>>,
    zh => <<67, 97, 115, 115, 97, 110, 100, 114, 97>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_cassa,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 100, 97, 116, 97, 32,
    116, 111, 32, 67, 97, 115, 115, 97, 110, 100, 114,
    97>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 67, 97, 115, 115, 97,
      110, 100, 114, 97, 32, 230, 149, 176, 230, 141, 174,
      229, 186, 147>>},
  for => '$any', name => data_to_cassa,
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
      67, 97, 115, 115, 97, 110, 100, 114,
      97>>,
      zh =>
      <<229, 140, 133, 229, 144, 171, 228, 186,
        134, 229, 141, 160, 228, 189, 141, 231,
        172, 166, 231, 154, 132, 32, 83, 81, 76,
        32, 230, 168, 161, 230, 157, 191, 239,
        188, 140, 231, 148, 168, 228, 186, 142,
        230, 143, 146, 229, 133, 165, 230, 136,
        150, 230, 155, 180, 230, 150, 176, 230,
        149, 176, 230, 141, 174, 229, 136, 176,
        32, 67, 97, 115, 115, 97, 110, 100, 114,
        97, 32, 230, 149, 176, 230, 141, 174,
        229, 186, 147>>},
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
  <<68, 97, 116, 97, 32, 116, 111, 32, 67, 97, 115, 115,
    97, 110, 100, 114, 97>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 67, 97, 115, 115, 97,
      110, 100, 114, 97>>},
  types => [backend_cassa]}).

-rule_action(#{category => offline_msgs,
  create => on_action_create_offline_msg,
  description =>
  #{en =>
  <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
    32, 116, 111, 32, 67, 97, 115, 115, 97, 110, 100,
    114, 97>>,
    zh =>
    <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
      129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
      176, 32, 67, 97, 115, 115, 97, 110, 100, 114, 97>>},
  for => '$any', name => offline_msg_to_cassa,
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
    max_returned_count =>
    #{default => 0,
      description =>
      #{en =>
      <<77, 97, 120, 32, 110, 117, 109, 98, 101,
        114, 32, 111, 102, 32, 102, 101, 116, 99,
        104, 32, 111, 102, 102, 108, 105, 110,
        101, 32, 109, 101, 115, 115, 97, 103,
        101, 115>>,
        zh =>
        <<232, 142, 183, 229, 143, 150, 231, 166,
          187, 231, 186, 191, 230, 182, 136, 230,
          129, 175, 231, 154, 132, 230, 156, 128,
          229, 164, 167, 230, 157, 161, 230, 149,
          176>>},
      required => false,
      title =>
      #{en =>
      <<77, 97, 120, 32, 82, 101, 116, 117, 114,
        110, 101, 100, 32, 67, 111, 117, 110,
        116>>,
        zh =>
        <<77, 97, 120, 32, 82, 101, 116, 117, 114,
          110, 101, 100, 32, 67, 111, 117, 110,
          116>>},
      type => number},
    time_range =>
    #{default => <<>>,
      description =>
      #{en =>
      <<84, 105, 109, 101, 32, 82, 97, 110, 103,
        101, 32, 111, 102, 32, 102, 101, 116, 99,
        104, 32, 111, 102, 102, 108, 105, 110,
        101, 32, 109, 101, 115, 115, 97, 103,
        101, 115>>,
        zh =>
        <<232, 142, 183, 229, 143, 150, 230, 156,
          128, 232, 191, 145, 230, 151, 182, 233,
          151, 180, 232, 140, 131, 229, 155, 180,
          229, 134, 133, 231, 154, 132, 231, 166,
          187, 231, 186, 191, 230, 182, 136, 230,
          129, 175>>},
      required => false,
      title =>
      #{en =>
      <<84, 105, 109, 101, 32, 82, 97, 110, 103,
        101>>,
        zh =>
        <<84, 105, 109, 101, 32, 82, 97, 110, 103,
          101>>},
      type => string}},
  title =>
  #{en =>
  <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
    32, 116, 111, 32, 67, 97, 115, 115, 97, 110, 100,
    114, 97>>,
    zh =>
    <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
      129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
      176, 32, 67, 97, 115, 115, 97, 110, 100, 114, 97>>},
  types => [backend_cassa]}).

-rule_action(#{category => server_side_subscription,
  create => on_action_create_lookup_sub,
  description =>
  #{en =>
  <<71, 101, 116, 32, 83, 117, 98, 115, 99, 114, 105,
    112, 116, 105, 111, 110, 32, 76, 105, 115, 116, 32,
    70, 114, 111, 109, 32, 67, 97, 115, 115, 97, 110,
    100, 114, 97>>,
    zh =>
    <<228, 187, 142, 32, 67, 97, 115, 115, 97, 110, 100,
      114, 97, 32, 228, 184, 173, 232, 142, 183, 229, 143,
      150, 232, 174, 162, 233, 152, 133, 229, 136, 151,
      232, 161, 168>>},
  for => '$any', name => lookup_sub_to_cassa,
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
    type => string}},
  title =>
  #{en =>
  <<83, 117, 98, 115, 99, 114, 105, 112, 116, 105, 111,
    110, 32, 76, 105, 115, 116, 32, 70, 114, 111, 109,
    32, 67, 97, 115, 115, 97, 110, 100, 114, 97>>,
    zh =>
    <<228, 187, 142, 32, 67, 97, 115, 115, 97, 110, 100,
      114, 97, 32, 228, 184, 173, 232, 142, 183, 229, 143,
      150, 232, 174, 162, 233, 152, 133, 229, 136, 151,
      232, 161, 168>>},
  types => [backend_cassa]}).

%% 创建资源
on_resource_create(ResId, Config = #{<<"nodes">> := Nodes, <<"keyspace">> := Keysapce}) ->
  begin
    logger:log(info, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Initiating Resource ~p, ResId: ~p", [backend_cassa, ResId]} end, mfa => {emqx_backend_cassa_actions, on_resource_create, 2}, line => 327})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(ecql),
  SslOpts = case maps:get(<<"ssl">>, Config, false) of
              true ->
                [{ssl, emqx_ruls_actions_utils:get_ssl_opts(Config, ResId)}];
              false -> []
            end,
  Options = [{nodes, parse_nodes_str(str(Nodes))},
    {username, str(maps:get(<<"username">>, Config, ""))},
    {password, str(maps:get(<<"password">>, Config, ""))},
    {keyspace, str(Keysapce)},
    {auto_reconnect,
      case maps:get(<<"auto_reconnect">>, Config, true) of
        true -> 2;
        false -> false
      end},
    {pool_size, maps:get(<<"pool_size">>, Config, 8)}],
  PoolName = list_to_atom("cassa:" ++ str(ResId)),
  emqx_rule_actions_utils:start_pool(PoolName, emqx_backend_cassa_actions, Options ++ SslOpts),
  #{<<"pool">> => PoolName}.

%% 查询资源状态
-spec on_get_resource_status(ResId :: binary(), Params :: map()) -> Status :: map().
on_get_resource_status(_ResId,#{<<"pool">> := PoolName}) ->
  #{is_alive => emqx_rule_actions_utils:is_resource_alive(PoolName, fun (Conn) -> {ok, _} = ecql:query(Conn, "SELECT count(1) AS T FROM system.local", ""), ok end)}.

%% 销毁资源
on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
  begin
    logger:log(info, #{},
      #{report_cb => fun (_) -> {logger_header() ++ "Destroying Resource ~p, ResId: ~p", [backend_cassa, ResId]} end, mfa => {emqx_backend_cassa_actions, on_resource_destroy, 2}, line => 355})
  end,
  emqx_rule_actions_utils:stop_pool(PoolName).

%% 创建动作，保存数据
-spec on_action_create_data_to_cassa(Id :: binary(),#{}) -> fun((Msg :: map()) -> any()).
on_action_create_data_to_cassa(ActId,Opts = #{<<"pool">> := PoolName, <<"sql">> := SQL, <<"enable_batch">> := true}) ->
  begin
    logger:log(info, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Initiating Action ~p, SqlTemplate: ~p", [on_action_create_data_to_cassa, SQL]} end,
        mfa => {emqx_backend_cassa_actions,on_action_create_data_to_cassa, 2}, line => 362})
  end,
%%  同步超时
  SyncTimeout = maps:get(<<"sync_timeout">>, Opts, 5000),
%%  插入模块式
  InsertMode = maps:get(<<"insert_mode">>,Opts,<<"sync">>),
%%  批处理名称
  BatcherPName = list_to_atom("cassa_batcher:" ++ str(ActId)),
  emqx_rule_actions_utils:start_batcher_pool(BatcherPName, emqx_backend_cassa_actions, Opts,
    case InsertMode of
      <<"sync">> -> {PoolName,ActId,{sync, SyncTimeout}};
      <<"async">> -> {PoolName, ActId, async}
    end),
  SQLInsert = emqx_rule_utils:preproc_tmpl(SQL),
  SyncTimeout = maps:get(<<"sync_timeout">>, Opts, 5000),
  {[{'Opts', Opts}, {'PoolName', PoolName}, {'SQL', SQL}, {'ActId', ActId}, {'SyncTimeout', SyncTimeout},
    {'InsertMode', InsertMode}, {'BatcherPName', BatcherPName}, {'SQLInsert', SQLInsert}, {'SyncTimeout', SyncTimeout}], Opts#{batcher_pname => BatcherPName}};

on_action_create_data_to_cassa(ActId,Opts = #{<<"pool">> := PoolName, <<"sql">> := SQL}) ->
  begin
    logger:log(info, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Initiating Action ~p, SqlTemplate: ~p", [on_action_create_data_to_cassa, SQL]} end, mfa => {emqx_backend_cassa_actions,on_action_create_data_to_cassa, 2},line => 376})
  end,
  {PrepareStatement, ParamsTokens} = emqx_rule_utils:preproc_sql(SQL, '?'),
  {[{'Opts', Opts}, {'PoolName', PoolName}, {'SQL', SQL}, {'ActId', ActId}, {'ParamsTokens', ParamsTokens}, {'PrepareStatement', PrepareStatement}], Opts}.

on_action_data_to_cassa(Msg, _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'BatcherPName' := BatcherPName,
      'SQLInsert' := SQLInsert,
      'SyncTimeout' := SyncTimeout,
      'InsertMode' := InsertMode}}) ->
  SQLInsert1 = emqx_rule_utils:proc_cql_param_str(SQLInsert, Msg),
  begin
    logger:log(debug, #{}, #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Cassandra Pool: ~p, Msg: ~0p",
          [BatcherPName, SQLInsert1]}
      end,
        mfa => {emqx_backend_cassa_actions, on_action_data_to_cassa, 2}, line => 389})
  end,
  case ecpool:pick_and_do(BatcherPName,
    emqx_rule_actions_batcher:accumulate_mfa(InsertMode,
      SQLInsert1,
      SyncTimeout),
    no_handover)
  of
    ok ->
      begin
        logger:log(debug, #{}, #{report_cb => fun (_) -> {logger_header() ++ "async insert request sent", []} end, mfa => {emqx_backend_cassa_actions, on_action_data_to_cassa, 2}, line => 392})
      end,
      case InsertMode of
        <<"sync">> -> emqx_rule_metrics:inc_actions_success(ActId);
        _ -> ok
      end;
    {error, Reason} ->
      begin
        logger:log(error, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Cassandra insert failed reason: ~p", [Reason]} end, mfa => {emqx_backend_cassa_actions, on_action_data_to_cassa, 2}, line => 398})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason};
    Result ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Cassandra successfully, result: ~p",
              [Result]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              on_action_data_to_cassa,
              2},
            line => 402})
      end,
      emqx_rule_metrics:inc_actions_success(ActId)
  end;
on_action_data_to_cassa(Msg,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'ParamsTokens' := ParamsTokens,
      'PrepareStatement' :=
      PrepareStatement}}) ->
  case cassa_query(PoolName,
    PrepareStatement,
    emqx_rule_utils:proc_sql(ParamsTokens, Msg))
  of
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Cassandra insert failed reason: ~p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              on_action_data_to_cassa,
              2},
            line => 415})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason};
    Result ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Cassandra successfully, result: ~p",
              [Result]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              on_action_data_to_cassa,
              2},
            line => 419})
      end,
      emqx_rule_metrics:inc_actions_success(ActId)
  end.

on_action_destroy_data_to_cassa(_ActId, #{batcher_pname := BatcherPName}) ->
  emqx_rule_actions_utils:stop_batcher_pool(BatcherPName);
on_action_destroy_data_to_cassa(_ActId, _) -> ok.

on_action_create_offline_msg(ActId, Opts = #{<<"pool">> := PoolName, <<"time_range">> := TimeRange0, <<"max_returned_count">> := MaxReturnedCount}) ->
  TimeRange = case to_undefined(TimeRange0) of
                undefined -> undefined;
                TimeRange0 -> cuttlefish_duration:parse(binary_to_list(TimeRange0), s)
              end,
  {[{'TimeRange0', TimeRange0},
    {'Opts', Opts},
    {'MaxReturnedCount', MaxReturnedCount},
    {'PoolName', PoolName},
    {'ActId', ActId},
    {'TimeRange', TimeRange}],
    Opts}.

on_action_offline_msg_to_cassa(Msg = #{event := Event, topic := Topic}, _Envs = #{'__bindings__' := #{'ActId' := ActId, 'PoolName' := PoolName, 'TimeRange' := TimeRange, 'MaxReturnedCount' := MaxReturnedCount}}) ->
  case Event of
    'message.acked' -> delete_message(ActId, Topic, Msg, PoolName);
    'message.publish' -> insert_message(ActId, Topic, Msg, PoolName);
    'session.subscribed' ->
      case lookup_message(ActId, Topic, PoolName, TimeRange, to_undefined(MaxReturnedCount)) of
        {error, Reason} -> {badact, Reason};
        Messages -> [self() ! {deliver, Topic, M} || M <- Messages]
      end
  end.

on_action_create_lookup_sub(ActId, Opts = #{<<"pool">> := PoolName}) ->
  {[{'Opts', Opts}, {'PoolName', PoolName}, {'ActId', ActId}], Opts}.

on_action_lookup_sub_to_cassa(Msg = #{event := Event, clientid := ClientId}, Envs) ->
  ActId = maps:get('ActId', maps:get('__bindings__', Envs, #{})),
  PoolName = maps:get('PoolName', maps:get('__bindings__', Envs, #{})),
  case Event of
    'client.connected' ->
      case lookup_subscribe(ActId, ClientId, PoolName) of
        [] -> ok;
        TopicTable -> self() ! {subscribe, TopicTable}, ok
      end;
    'session.subscribed' ->
      insert_subscribe(ActId, ClientId, Msg, PoolName)
  end.

connect(Opts) -> ecql:connect(Opts).

batcher_flush(Batch, State) ->
  {emqx_backend_cassa_actions:batch_sql_insert(Batch, {emqx_backend_cassa_actions, batch_insert, [State]}), State}.

batch_insert(SQL, _BatchLen, {PoolName, _ActId, {sync, SyncTimeout}}) ->
  ecpool:pick_and_do(PoolName, {ecql, query, [SQL]}, {handover, SyncTimeout});
batch_insert(SQL, BatchLen, {PoolName, ActId, async}) ->
  ecpool:pick_and_do(PoolName, {ecql, query, [SQL]}, {handover_async, {emqx_backend_cassa_actions, async_call_back, [ActId, BatchLen]}}).

cassa_query(PoolName, PrepareStatement, PrepareParams) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Query Cassandra, Pool: ~p, PrepareStatement: "
          "~p, SqlParams: ~p",
          [PoolName, PrepareStatement, PrepareParams]}
      end,
        mfa => {emqx_backend_cassa_actions, cassa_query, 3},
        line => 493})
  end,
  ecpool:pick_and_do(PoolName,
    {emqx_backend_cassa_actions,
      ecql_query,
      [PrepareStatement, PrepareParams]},
    no_handover).

async_cassa_query(PoolName, Query, Params) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Query Cassandra, PoolName: ~p, PrepareStateme"
          "nt: ~p, SqlParams: ~p",
          [PoolName, Query, Params]}
      end,
        mfa =>
        {emqx_backend_cassa_actions, async_cassa_query, 3},
        line => 498})
  end,
  ecpool:pick_and_do(PoolName,
    {ecql, async_query, [Query, Params]},
    no_handover).

delete_message(ActId, Topic, Msg, Pool) ->
  #{id := MsgId} = Msg,
  Del = <<"delete from mqtt_msg where topic = ? and msgid = ?">>,
  case async_cassa_query(Pool, Del, [Topic, MsgId]) of
    {error, Error} ->
      begin
        logger:log(error, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Failed to delete msg: ~p", [Error]} end, mfa => {emqx_backend_cassa_actions, delete_message, 4}, line => 506})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Error};
    Result ->
      emqx_rule_metrics:inc_actions_success(ActId),
      Result
  end.

insert_message(ActId, Topic, Msg, Pool) ->
  #{id := MsgId, qos := Qos, flags := Flags,
    payload := Payload, publish_received_at := Ts,
    clientid := From} =
    Msg,
  Retain = maps:get(retain, Flags, true),
  Params = [Topic,
    MsgId,
    From,
    Qos,
    Payload,
    Ts,
    i(Retain)],
  Insert = <<"insert into mqtt_msg(topic, msgid, sender, "
  "qos, payload, arrived, retain)\n    "
  "            values(?, ?, ?, ?, ?, ?, "
  "?)">>,
  case async_cassa_query(Pool, Insert, Params) of
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "insert into msg fail: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              insert_message,
              4},
            line => 526})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Error};
    Result ->
      emqx_rule_metrics:inc_actions_success(ActId),
      Result
  end.

lookup_message(ActId, Topic, Pool, undefined,
    undefined) ->
  lookup_message(ActId, Pool, [Topic], <<"">>);
lookup_message(ActId, Topic, Pool, TimeRange,
    undefined) ->
  Time = erlang:system_time(seconds) - TimeRange,
  lookup_message(ActId,
    Pool,
    [Topic, Time],
    <<" and arrived >= ? ">>);
lookup_message(ActId, Topic, Pool, undefined,
    MaxReturnedCount) ->
  lookup_message(ActId,
    Pool,
    [Topic, MaxReturnedCount],
    <<" limit ? ALLOW FILTERING">>);
lookup_message(ActId, Topic, Pool, TimeRange,
    MaxReturnedCount) ->
  Time = erlang:system_time(seconds) - TimeRange,
  Append =
    <<" and arrived >= ? limit ? ALLOW FILTERING">>,
  lookup_message(ActId,
    Pool,
    [Topic, Time, MaxReturnedCount],
    Append).

lookup_message(ActId, Pool, Params, Append) ->
  Cql = <<"select topic, msgid, sender, qos, payload, "
  "retain, arrived from mqtt_msg\n     "
  "        where topic = ? ",
    Append/binary>>,
  case cassa_query(Pool, Cql, Params) of
    {ok, {_, Cols, Rows}} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      lists:reverse([begin
                       Msg = emqx_backend_cassa_cli:r2m(Cols, Row),
                       Msg#message{id =
                       emqx_guid:from_hexstr(Msg#message.id)}
                     end
        || Row <- Rows]);
    {error, Error2} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to lookup message: ~p",
              [Error2]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              lookup_message,
              4},
            line => 556})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {error, Error2}
  end.

insert_subscribe(ActId, ClientId, Msg, PoolName) ->
  InsertSub = <<"insert into mqtt_sub(clientid, topic, "
  "qos) values(?, ?, ?)">>,
  #{topic := Topic, qos := QoS} = Msg,
  case async_cassa_query(PoolName,
    InsertSub,
    [ClientId, Topic, QoS])
  of
    {ok, _} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      ok;
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to insert subscribe: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              insert_subscribe,
              4},
            line => 569})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Error}
  end.

lookup_subscribe(ActId, ClientId, PoolName) ->
  SelectSub = <<"select topic, qos from mqtt_sub where "
  "clientid = ?">>,
  case cassa_query(PoolName, SelectSub, [ClientId]) of
    {ok, {_Keyspace, _Cols, Rows}} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      lists:map(fun ([Topic, Qos]) -> {Topic, #{qos => Qos}}
                end,
        Rows);
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to lookup subscriptions: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_cassa_actions,
              lookup_subscribe,
              3},
            line => 583})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Error}
  end.

i(true) -> 1;
i(false) -> 0.

to_undefined(<<>>) -> undefined;
to_undefined(0) -> undefined;
to_undefined(V) -> V.

batch_sql_insert(Batch, InsertFun) ->
  {FromList, SQLList} = lists:unzip(Batch),
  BatchSQL = insert_batch_sql(SQLList),
  Result = emqx_rule_actions_utils:safe_exec(InsertFun,
    [BatchSQL, length(SQLList)]),
  [{From, Result} || From <- FromList].

insert_batch_sql(SQLList) ->
  insert_batch_sql(SQLList, <<"BEGIN BATCH ">>).

insert_batch_sql([], Acc) ->
  <<Acc/binary, "APPLY BATCH;">>;
insert_batch_sql([SQL | SQLList], Acc) ->
  insert_batch_sql(SQLList,
    <<Acc/binary, SQL/binary, ";">>).

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
        mfa => {emqx_backend_cassa_actions, async_call_back, 3},
        line => 609})
  end,
  emqx_rule_metrics:inc_actions_error(ActId, BatchLen);
async_call_back(Result, ActId, BatchLen) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "async insert successfully, result: ~0p",
          [Result]}
      end,
        mfa => {emqx_backend_cassa_actions, async_call_back, 3},
        line => 612})
  end,
  emqx_rule_metrics:inc_actions_success(ActId, BatchLen).

typing(V) when is_boolean(V) ->
  {int,
    if V -> 1;
      true -> 0
    end};
typing(V) when is_binary(V); is_list(V); is_atom(V) ->
  V;
typing(V) when is_integer(V) ->
  case V > 2147483647 orelse V < -2147483647 of
    true -> {bigint, V};
    false -> {int, V}
  end;
typing(V) when is_float(V) -> {double, V}.

parse_nodes_str(Nodes) ->
  [begin
     [Host, Port] = string:tokens(S, ":"),
     {Host, list_to_integer(Port)}
   end
    || S <- string:tokens(Nodes, ",")].

ecql_query(Conn, PrepareStatement, PrepareParams) ->
  ecql:query(Conn,
    PrepareStatement,
    lists:map(fun (I) -> typing(I) end, PrepareParams)).

logger_header() -> "".

