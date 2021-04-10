%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:59
%%%-------------------------------------------------------------------
-module(emqx_backend_mysql_actions).
-author("root").

-export([logger_header/0]).
-behaviour(ecpool_worker).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").


-export([async_call_back/3, batcher_flush/2]).

-export([insert/2,
  batch_insert/3,
  execute/4,
  lookup_message/5,
  prepare_sql_to_conn/2]).

-export([connect/1]).

-import(emqx_rule_utils, [str/1, unsafe_atom_key/1]).

-export([on_resource_create/2,
  on_resource_destroy/2,
  on_get_resource_status/2]).

-export([on_action_create_data_to_mysql/2,
  on_action_destroy_data_to_mysql/2,
  on_action_data_to_mysql/2]).

-export([on_action_create_offline_msg/2,
  on_action_offline_msg_to_mysql/2]).

-export([on_action_create_lookup_sub/2,
  on_action_lookup_sub_to_mysql/2]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en =>
  <<77, 121, 83, 81, 76, 32, 68, 97, 116, 97, 98, 97,
    115, 101>>,
    zh =>
    <<77, 121, 83, 81, 76, 32, 230, 149, 176, 230, 141,
      174, 229, 186, 147>>},
  destroy => on_resource_destroy, name => backend_mysql,
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
      <<77, 121, 83, 81, 76, 32, 232, 191, 158,
        230, 142, 165, 230, 150, 173, 229, 188,
        128, 230, 151, 182, 230, 152, 175, 229,
        144, 166, 233, 135, 141, 232, 191,
        158>>},
    order => 7,
    title =>
    #{en =>
    <<69, 110, 97, 98, 108, 101, 32, 82, 101,
      99, 111, 110, 110, 101, 99, 116>>,
      zh =>
      <<230, 152, 175, 229, 144, 166, 233, 135,
        141, 232, 191, 158>>},
    type => boolean},
    cacertfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<89, 111, 117, 114, 32, 115, 115, 108,
        32, 99, 97, 99, 101, 114, 116, 102,
        105, 108, 101>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 83, 83, 76,
          32, 67, 65, 232, 175, 129, 228, 185,
          166>>},
      order => 9,
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
        <<77, 121, 83, 81, 76, 32, 83, 83, 76,
          32, 232, 175, 129, 228, 185, 166>>},
      order => 11,
      title =>
      #{en => <<83, 83, 76, 32, 67, 101, 114, 116>>,
        zh => <<83, 83, 76, 32, 67, 101, 114, 116>>},
      type => file},
    database =>
    #{description =>
    #{en =>
    <<68, 97, 116, 97, 98, 97, 115, 101, 32,
      110, 97, 109, 101, 32, 102, 111, 114,
      32, 99, 111, 110, 110, 101, 99, 116,
      105, 110, 103, 32, 116, 111, 32, 77,
      121, 83, 81, 76>>,
      zh =>
      <<77, 121, 83, 81, 76, 32, 230, 149, 176,
        230, 141, 174, 229, 186, 147, 229, 144,
        141>>},
      order => 3, required => true,
      title =>
      #{en =>
      <<77, 121, 83, 81, 76, 32, 68, 97, 116,
        97, 98, 97, 115, 101>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 230, 149, 176,
          230, 141, 174, 229, 186, 147, 229, 144,
          141>>},
      type => string},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<89, 111, 117, 114, 32, 115, 115, 108,
        32, 107, 101, 121, 102, 105, 108,
        101>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 83, 83, 76,
          32, 231, 167, 129, 233, 146, 165>>},
      order => 10,
      title =>
      #{en => <<83, 83, 76, 32, 75, 101, 121>>,
        zh => <<83, 83, 76, 32, 75, 101, 121>>},
      type => file},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<80, 97, 115, 115, 119, 111, 114, 100,
        32, 102, 111, 114, 32, 99, 111, 110,
        110, 101, 99, 116, 105, 110, 103, 32,
        116, 111, 32, 77, 121, 83, 81, 76>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 229, 175, 134,
          231, 160, 129>>},
      order => 6,
      title =>
      #{en =>
      <<77, 121, 83, 81, 76, 32, 80, 97, 115,
        115, 119, 111, 114, 100>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 229, 175, 134,
          231, 160, 129>>},
      type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<84, 104, 101, 32, 115, 105, 122, 101,
        32, 111, 102, 32, 99, 111, 110, 110,
        101, 99, 116, 105, 111, 110, 32, 112,
        111, 111, 108, 32, 102, 111, 114, 32,
        77, 121, 83, 81, 76>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 232, 191, 158,
          230, 142, 165, 230, 177, 160, 229, 164,
          167, 229, 176, 143>>},
      order => 4,
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
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 51,
      51, 48, 54>>,
      description =>
      #{en =>
      <<77, 121, 83, 81, 76, 32, 73, 80, 32,
        97, 100, 100, 114, 101, 115, 115, 32,
        111, 114, 32, 104, 111, 115, 116, 110,
        97, 109, 101, 32, 97, 110, 100, 32,
        112, 111, 114, 116>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 230, 156, 141,
          229, 138, 161, 229, 153, 168, 229, 156,
          176, 229, 157, 128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<77, 121, 83, 81, 76, 32, 83, 101, 114,
        118, 101, 114>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 230, 156, 141,
          229, 138, 161, 229, 153, 168>>},
      type => string},
    ssl =>
    #{default => false,
      description =>
      #{en =>
      <<73, 102, 32, 101, 110, 97, 98, 108,
        101, 32, 115, 115, 108>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 188,
          128, 229, 144, 175, 77, 121, 83, 81,
          76, 32, 83, 83, 76>>},
      order => 8,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<229, 188, 128, 229, 144, 175, 83, 83,
          76, 233, 147, 190, 230, 142, 165>>},
      type => boolean},
    user =>
    #{description =>
    #{en =>
    <<85, 115, 101, 114, 110, 97, 109, 101,
      32, 102, 111, 114, 32, 99, 111, 110,
      110, 101, 99, 116, 105, 110, 103, 32,
      116, 111, 32, 77, 121, 83, 81, 76>>,
      zh =>
      <<77, 121, 83, 81, 76, 32, 231, 148, 168,
        230, 136, 183, 229, 144, 141>>},
      order => 5, required => true,
      title =>
      #{en =>
      <<77, 121, 83, 81, 76, 32, 85, 115, 101,
        114, 32, 78, 97, 109, 101>>,
        zh =>
        <<77, 121, 83, 81, 76, 32, 231, 148, 168,
          230, 136, 183, 229, 144, 141>>},
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
      order => 12,
      title =>
      #{en =>
      <<86, 101, 114, 105, 102, 121, 32, 83,
        101, 114, 118, 101, 114, 32, 67, 101,
        114, 116, 102, 105, 108, 101>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 230, 160,
          161, 233, 170, 140, 230, 156, 141, 229,
          138, 161, 229, 153, 168, 232, 175, 129,
          228, 185, 166>>},
      type => boolean}},
  status => on_get_resource_status,
  title =>
  #{en => <<77, 121, 83, 81, 76>>,
    zh => <<77, 121, 83, 81, 76>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_mysql,
  description =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 77, 121, 83, 81,
    76>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 77, 121, 83, 81, 76, 32,
      230, 149, 176, 230, 141, 174, 229, 186, 147>>},
  destroy => on_action_destroy_data_to_mysql,
  for => '$any', name => data_to_mysql,
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
    order => 0, required => true,
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
      116, 111, 32, 77, 121, 83, 81, 76>>,
      zh =>
      <<229, 140, 133, 229, 144, 171, 228, 186,
        134, 229, 141, 160, 228, 189, 141, 231,
        172, 166, 231, 154, 132, 32, 83, 81, 76,
        32, 230, 168, 161, 230, 157, 191, 239,
        188, 140, 231, 148, 168, 228, 187, 165,
        230, 143, 146, 229, 133, 165, 230, 136,
        150, 230, 155, 180, 230, 150, 176, 230,
        149, 176, 230, 141, 174, 229, 136, 176,
        32, 77, 121, 83, 81, 76, 32, 230, 149,
        176, 230, 141, 174, 229, 186, 147>>},
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
  <<68, 97, 116, 97, 32, 116, 111, 32, 77, 121, 83, 81,
    76>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 77, 121, 83, 81, 76>>},
  types => [backend_mysql]}).

-rule_action(#{category => offline_msgs,
  create => on_action_create_offline_msg,
  description =>
  #{en =>
  <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
    32, 116, 111, 32, 77, 121, 83, 81, 76>>,
    zh =>
    <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
      129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
      176, 32, 77, 121, 83, 81, 76>>},
  for => '$any', name => offline_msg_to_mysql,
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
    order => 0, required => true,
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
    32, 116, 111, 32, 77, 121, 83, 81, 76>>,
    zh =>
    <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
      129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
      176, 32, 77, 121, 83, 81, 76>>},
  types => [backend_mysql]}).

-rule_action(#{category => server_side_subscription,
  create => on_action_create_lookup_sub,
  description =>
  #{en =>
  <<71, 101, 116, 32, 83, 117, 98, 115, 99, 114, 105,
    112, 116, 105, 111, 110, 32, 76, 105, 115, 116, 32,
    70, 114, 111, 109, 32, 77, 121, 83, 81, 76>>,
    zh =>
    <<228, 187, 142, 32, 77, 121, 83, 81, 76, 32, 228, 184,
      173, 232, 142, 183, 229, 143, 150, 232, 174, 162,
      233, 152, 133, 229, 136, 151, 232, 161, 168>>},
  for => '$any', name => lookup_sub_to_mysql,
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
    order => 0, required => true,
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
    32, 77, 121, 83, 81, 76>>,
    zh =>
    <<228, 187, 142, 32, 77, 121, 83, 81, 76, 32, 228, 184,
      173, 232, 142, 183, 229, 143, 150, 232, 174, 162,
      233, 152, 133, 229, 136, 151, 232, 161, 168>>},
  types => [backend_mysql]}).

-vsn("4.2.5").

on_resource_create(ResId,
    Config = #{<<"server">> := Server, <<"user">> := User,
      <<"database">> := DB}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_mysql, ResId]}
      end,
        mfa =>
        {emqx_backend_mysql_actions, on_resource_create, 2},
        line => 344})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {Host, Port} =
    emqx_rule_actions_utils:parse_server(Server, 3306),
  SslOpts = case maps:get(<<"ssl">>, Config, false) of
              true ->
                [{ssl,
                  [{server_name_indication, disable}
                    | emqx_rule_actions_utils:get_ssl_opts(Config,
                    ResId)]}];
              false -> []
            end,
  Options = [{host, Host},
    {port, Port},
    {user, str(User)},
    {password, str(maps:get(<<"password">>, Config, ""))},
    {database, str(DB)},
    {auto_reconnect,
      case maps:get(<<"auto_reconnect">>, Config, true) of
        true -> 15;
        false -> false
      end},
    {pool_size, maps:get(<<"pool_size">>, Config, 8)}],
  PoolName = list_to_atom("mysql:" ++ str(ResId)),
  _ = emqx_rule_actions_utils:start_pool(PoolName,
    emqx_backend_mysql_actions,
    Options ++ SslOpts),
  case test_resource_status(PoolName) of
    true -> ok;
    _ -> error({{backend_mysql, ResId}, connection_failed})
  end,
  #{<<"pool">> => PoolName}.

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
          [backend_mysql, ResId]}
      end,
        mfa =>
        {emqx_backend_mysql_actions, on_resource_destroy, 2},
        line => 377})
  end,
  emqx_rule_actions_utils:stop_pool(PoolName).

-spec on_action_create_data_to_mysql(ActId :: binary(),
    #{}) -> fun((Msg :: map()) -> any()).

on_action_create_data_to_mysql(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"sql">> := SQL,
      <<"enable_batch">> := true}) ->
  BatcherPName = list_to_atom("mysql_batcher:" ++
  str(ActId)),
  SyncTimeout = maps:get(<<"sync_timeout">>, Opts, 5000),
  InsertMode = maps:get(<<"insert_mode">>,
    Opts,
    <<"sync">>),
  _ =
    emqx_rule_actions_utils:start_batcher_pool(BatcherPName,
      emqx_backend_mysql_actions,
      Opts,
      case InsertMode of
        <<"sync">> ->
          {PoolName,
            ActId,
            {sync,
              SyncTimeout}};
        <<"async">> ->
          {PoolName,
            ActId,
            async}
      end),
  {ok, {SQLInsertPart0, SQLParamPart0}} =
    emqx_rule_actions_utils:split_insert_sql(SQL),
  SQLInsertPartTks =
    emqx_rule_utils:preproc_tmpl(SQLInsertPart0),
  SQLParamPartTks =
    emqx_rule_utils:preproc_tmpl(SQLParamPart0),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
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
on_action_create_data_to_mysql(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"sql">> := SQL}) ->
  PrepareSqlKey = unsafe_atom_key(ActId),
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Action ~p, SqlTemplate: ~p, "
          "PrepareSqlKey: ~p",
          [on_action_create_data_to_mysql,
            SQL,
            PrepareSqlKey]}
      end,
        mfa =>
        {emqx_backend_mysql_actions,
          on_action_create_data_to_mysql,
          2},
        line => 402})
  end,
  {PrepareStatement, ParamsTokens} =
    emqx_rule_utils:preproc_sql(SQL, '?'),
  prepare_sql(PrepareSqlKey, PrepareStatement, PoolName),
  ecpool:add_reconnect_callback(PoolName,
    {emqx_backend_mysql_actions,
      prepare_sql_to_conn,
      [[{PrepareSqlKey, PrepareStatement}]]}),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
    {'SQL', SQL},
    {'ActId', ActId},
    {'PrepareSqlKey', PrepareSqlKey},
    {'ParamsTokens', ParamsTokens},
    {'PrepareStatement', PrepareStatement}],
    Opts}.

on_action_data_to_mysql(Msg,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'BatcherPName' := BatcherPName,
      'SQLInsertPartTks' := SQLInsertPartTks,
      'SQLParamPartTks' := SQLParamPartTks,
      'InsertMode' := InsertMode,
      'SyncTimeout' := SyncTimeout}}) ->
  SQLInsertPart =
    emqx_rule_utils:proc_tmpl(SQLInsertPartTks, Msg),
  SQLParamPart =
    emqx_rule_utils:proc_sql_param_str(SQLParamPartTks,
      Msg),
  AccumData = {SQLInsertPart, SQLParamPart},
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "MySQL Pool: ~p, Msg: ~0p",
          [BatcherPName, AccumData]}
      end,
        mfa =>
        {emqx_backend_mysql_actions,
          on_action_data_to_mysql,
          2},
        line => 420})
  end,
  case ecpool:pick_and_do(BatcherPName,
    emqx_rule_actions_batcher:accumulate_mfa(InsertMode,
      AccumData,
      SyncTimeout),
    no_handover)
  of
    ok -> ok;
    {ok, _, Resp} ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "MySQL successfully, resp: ~p",
              [Resp]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              on_action_data_to_mysql,
              2},
            line => 425})
      end,
      emqx_rule_metrics:inc_actions_success(ActId);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "MySQL insert failed, reason: ~0p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              on_action_data_to_mysql,
              2},
            line => 428})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason}
  end;
on_action_data_to_mysql(Msg,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'ParamsTokens' := ParamsTokens,
      'PrepareSqlKey' := PrepareSqlKey,
      'PrepareStatement' :=
      PrepareStatement}}) ->
  case mysql_query(PoolName,
    PrepareSqlKey,
    PrepareStatement,
    emqx_rule_utils:proc_sql(ParamsTokens, Msg))
  of
    {error, Reason} ->
      error({data_to_mysql, Reason}),
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason};
    _ -> emqx_rule_metrics:inc_actions_success(ActId)
  end.

on_action_destroy_data_to_mysql(_ActId,
    #{batcher_pname := BatcherPName}) ->
  emqx_rule_actions_utils:stop_batcher_pool(BatcherPName);
on_action_destroy_data_to_mysql(_ActId, _) -> ok.

on_action_create_offline_msg(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"time_range">> := TimeRange0,
      <<"max_returned_count">> :=
      MaxReturnedCount0}) ->
  InsertSql = <<"insert into mqtt_msg(msgid, sender, "
  "topic, qos, retain, payload, arrived) "
  "values (?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?) "
  ")">>,
  DeleteSql = <<"delete from mqtt_msg where msgid = ? "
  "and topic = ?">>,
  SelectSql0 = <<"select id, topic, msgid, sender, qos, "
  "payload, retain, UNIX_TIMESTAMP(arrived) "
  "from mqtt_msg where topic = ? order "
  "by id DESC">>,
  InsertKey = unsafe_atom_key(<<"insert_",
    ActId/binary>>),
  DeleteKey = unsafe_atom_key(<<"delete_",
    ActId/binary>>),
  SelectKey = unsafe_atom_key(<<"select_",
    ActId/binary>>),
  TimeRange = case to_undefined(TimeRange0) of
                undefined -> undefined;
                TimeRange0 ->
                  cuttlefish_duration:parse(binary_to_list(TimeRange0), s)
              end,
  MaxReturnedCount = to_undefined(MaxReturnedCount0),
  {SelectSql, Params} = case {TimeRange, MaxReturnedCount}
                        of
                          {undefined, undefined} -> {SelectSql0, []};
                          {TimeRange, undefined} ->
                            {<<SelectSql0/binary,
                              " and arrived >= FROM_UNIXTIME(?) ">>,
                              [TimeRange div 1000]};
                          {undefined, MaxReturnedCount} ->
                            {<<SelectSql0/binary, " limit ? ">>,
                              [MaxReturnedCount]};
                          {TimeRange, MaxReturnedCount} ->
                            {<<SelectSql0/binary,
                              "  and arrived >= FROM_UNIXTIME(?) limit ? ">>,
                              [TimeRange div 1000, MaxReturnedCount]}
                        end,
  prepare_sql(InsertKey, InsertSql, PoolName),
  prepare_sql(DeleteKey, DeleteSql, PoolName),
  prepare_sql(SelectKey, SelectSql, PoolName),
  ecpool:add_reconnect_callback(PoolName,
    {emqx_backend_mysql_actions,
      prepare_sql_to_conn,
      [[{InsertKey, InsertSql},
        {DeleteKey, DeleteSql},
        {SelectKey, SelectSql}]]}),
  {[{'TimeRange0', TimeRange0},
    {'MaxReturnedCount0', MaxReturnedCount0},
    {'Opts', Opts},
    {'PoolName', PoolName},
    {'ActId', ActId},
    {'InsertSql', InsertSql},
    {'DeleteSql', DeleteSql},
    {'SelectSql0', SelectSql0},
    {'InsertKey', InsertKey},
    {'DeleteKey', DeleteKey},
    {'SelectKey', SelectKey},
    {'TimeRange', TimeRange},
    {'MaxReturnedCount', MaxReturnedCount},
    {'Params', Params},
    {'SelectSql', SelectSql}],
    Opts}.

on_action_offline_msg_to_mysql(Msg = #{event := Event,
  topic := Topic},
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'Params' := Params,
      'InsertKey' := InsertKey,
      'InsertSql' := InsertSql,
      'SelectKey' := SelectKey,
      'SelectSql' := SelectSql,
      'DeleteKey' := DeleteKey,
      'DeleteSql' := DeleteSql}}) ->
  case Event of
    'message.publish' ->
      #{id := MsgId, qos := Qos, flags := Flags,
        payload := Payload, publish_received_at := Ts,
        clientid := From} =
        Msg,
      Retain = maps:get(retain, Flags, true),
      insert_message(PoolName,
        ActId,
        InsertKey,
        InsertSql,
        [MsgId,
          From,
          Topic,
          Qos,
          i(Retain),
          Payload,
          Ts div 1000]);
    'session.subscribed' ->
      case lookup_message(PoolName,
        ActId,
        SelectKey,
        SelectSql,
        [Topic] ++ Params)
      of
        {error, Reason} -> {badact, Reason};
        MsgList ->
          [self() ! {deliver, Topic, M} || M <- MsgList]
      end;
    'message.acked' ->
      #{id := MsgId} = Msg,
      delete_message(PoolName,
        ActId,
        DeleteKey,
        DeleteSql,
        [MsgId, Topic])
  end.

on_action_create_lookup_sub(ActId,
    Opts = #{<<"pool">> := PoolName}) ->
  SelectPrepare =
    <<"select topic, qos from mqtt_sub where "
    "clientid = ?">>,
  InsertPrepare =
    <<"insert into mqtt_sub(clientid, topic, "
    "qos) values(?, ?, ?)">>,
  SelectKey = unsafe_atom_key(<<"select_",
    ActId/binary>>),
  InsertKey = unsafe_atom_key(<<"insert_",
    ActId/binary>>),
  prepare_sql(SelectKey, SelectPrepare, PoolName),
  prepare_sql(InsertKey, InsertPrepare, PoolName),
  ecpool:add_reconnect_callback(PoolName,
    {emqx_backend_mysql_actions,
      prepare_sql_to_conn,
      [[{SelectKey, SelectPrepare},
        {InsertKey, InsertPrepare}]]}),
  {[{'Opts', Opts},
    {'PoolName', PoolName},
    {'ActId', ActId},
    {'SelectPrepare', SelectPrepare},
    {'InsertPrepare', InsertPrepare},
    {'SelectKey', SelectKey},
    {'InsertKey', InsertKey}],
    Opts}.

on_action_lookup_sub_to_mysql(Msg = #{event := Event,
  clientid := ClientId},
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'SelectKey' := SelectKey,
      'SelectPrepare' := SelectPrepare,
      'InsertKey' := InsertKey,
      'InsertPrepare' :=
      InsertPrepare}}) ->
  case Event of
    'client.connected' ->
      case lookup_subscribe(PoolName,
        ActId,
        SelectKey,
        SelectPrepare,
        [ClientId])
      of
        {error, Reason} -> {badact, Reason};
        [] -> ok;
        TopicTable -> self() ! {subscribe, TopicTable}
      end;
    'session.subscribed' ->
      #{topic := Topic, qos := QoS} = Msg,
      insert_subscribe(PoolName,
        ActId,
        InsertKey,
        InsertPrepare,
        [ClientId, Topic, QoS])
  end.

prepare_sql(PrepareSqlKey, PrepareStatement,
    PoolName) ->
  [begin
     {ok, Conn} = ecpool_worker:client(Worker),
     prepare_sql_to_conn(Conn,
       [{PrepareSqlKey, PrepareStatement}])
   end
    || {_WorkerName, Worker} <- ecpool:workers(PoolName)].

prepare_sql_to_conn(Conn, PrepareList) ->
  lists:foreach(fun ({PrepareSqlKey, PrepareStatement}) ->
    begin
      logger:log(info,
        #{},
        #{report_cb =>
        fun (_) ->
          {logger_header() ++
            "Prepare Statement: ~p, PreparedSQL Key: ~p",
            [PrepareStatement,
              PrepareSqlKey]}
        end,
          mfa =>
          {emqx_backend_mysql_actions,
            prepare_sql_to_conn,
            2},
          line => 559})
    end,
    mysql:unprepare(Conn, PrepareSqlKey),
    case mysql:prepare(Conn,
      PrepareSqlKey,
      PrepareStatement)
    of
      {ok, Name} ->
        begin
          logger:log(info,
            #{},
            #{report_cb =>
            fun (_) ->
              {logger_header()
                ++
                "Prepare Statement: ~p, PreparedSQL Key: ~p",
                [PrepareStatement,
                  Name]}
            end,
              mfa =>
              {emqx_backend_mysql_actions,
                prepare_sql_to_conn,
                2},
              line => 563})
        end;
      {error, Reason} ->
        begin
          logger:log(error,
            #{},
            #{report_cb =>
            fun (_) ->
              {logger_header()
                ++
                "Prepare Statement: ~p, ~0p",
                [PrepareStatement,
                  Reason]}
            end,
              mfa =>
              {emqx_backend_mysql_actions,
                prepare_sql_to_conn,
                2},
              line => 565})
        end,
        error(prepare_sql_failed)
    end
                end,
    PrepareList).

test_resource_status(PoolName) ->
  Status = [begin
              case ecpool_worker:client(Worker) of
                {ok, Conn} ->
                  ok ==
                    element(1,
                      mysql:query(Conn,
                        <<"SELECT count(1) AS T">>));
                _ -> false
              end
            end
    || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
  length(Status) > 0 andalso
    lists:all(fun (St) -> St =:= true end, Status).

connect(Options) -> mysql:start_link(Options).

batcher_flush(Batch, State) ->
  {emqx_rule_actions_utils:batch_sql_insert(Batch,
    {emqx_backend_mysql_actions,
      batch_insert,
      [State]}),
    State}.

async_call_back({error, Reason}, ActId, BatchLen) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "insert failed reason: ~p",
          [Reason]}
      end,
        mfa => {emqx_backend_mysql_actions, async_call_back, 3},
        line => 590})
  end,
  emqx_rule_metrics:inc_actions_error(ActId, BatchLen);
async_call_back(Result, ActId, BatchLen) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "insert Successfully: ~p",
          [Result]}
      end,
        mfa => {emqx_backend_mysql_actions, async_call_back, 3},
        line => 593})
  end,
  emqx_rule_metrics:inc_actions_success(ActId, BatchLen).

batch_insert(SQL, _BatchLen,
    {PoolName, _ActId, {sync, SyncTimeout}}) ->
  ecpool:pick_and_do(PoolName,
    {emqx_backend_mysql_actions, insert, [SQL]},
    {handover, SyncTimeout});
batch_insert(SQL, BatchLen, {PoolName, ActId, async}) ->
  ecpool:pick_and_do(PoolName,
    {emqx_backend_mysql_actions, insert, [SQL]},
    {handover_async,
      {emqx_backend_mysql_actions,
        async_call_back,
        [ActId, BatchLen]}}).

insert(Conn, SQL) when is_pid(Conn) ->
  case mysql:query(Conn, SQL) of
    ok -> {ok, ok, []};
    Result -> Result
  end.

mysql_query(Pool, PreparedKey, PrepareStatement,
    Params) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Send to mysql, pool: ~p, prepared_key: "
          "~p, ~0p",
          [Pool, PreparedKey, Params]}
      end,
        mfa => {emqx_backend_mysql_actions, mysql_query, 4},
        line => 609})
  end,
  ecpool:pick_and_do(Pool,
    {emqx_backend_mysql_actions,
      execute,
      [PreparedKey, PrepareStatement, Params]},
    no_handover).

execute(Conn, PreparedKey, PrepareStatement, Params) ->
  case mysql:execute(Conn, PreparedKey, Params) of
    {error, disconnected} -> exit(Conn, restart);
    {error, not_prepared} ->
      prepare_sql_to_conn(Conn,
        [{PreparedKey, PrepareStatement}]),
      mysql:execute(Conn, PreparedKey, Params);
    Res -> Res
  end.

insert_message(Pool, ActId, PrepareSqlKey,
    PrepareStatement, PrepareParams) ->
  case mysql_query(Pool,
    PrepareSqlKey,
    PrepareStatement,
    PrepareParams)
  of
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to store message: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              insert_message,
              5},
            line => 625})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Error};
    _ -> emqx_rule_metrics:inc_actions_success(ActId)
  end.

lookup_message(Pool, ActId, PrepareSqlKey,
    PrepareStatement, PrepareParams) ->
  case mysql_query(Pool,
    PrepareSqlKey,
    PrepareStatement,
    PrepareParams)
  of
    {ok, _Cols, []} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      [];
    {ok, Cols, Rows} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      [begin
         Msg =
           emqx_backend_mysql_cli:record_to_msg(lists:zip(Cols,
             Row)),
         Msg#message{id = emqx_guid:from_hexstr(Msg#message.id)}
       end
        || Row <- lists:reverse(Rows)];
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to lookup message error: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              lookup_message,
              5},
            line => 644})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {error, Error}
  end.

delete_message(Pool, ActId, PrepareSqlKey,
    PrepareStatement, PrepareParams) ->
  case mysql_query(Pool,
    PrepareSqlKey,
    PrepareStatement,
    PrepareParams)
  of
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to delete message: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              delete_message,
              5},
            line => 652})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Error};
    _ ->
      emqx_rule_metrics:inc_actions_success(ActId),
      ok
  end.

insert_subscribe(Pool, ActId, PrepareSqlKey,
    PrepareStatement, PrepareParams) ->
  case mysql_query(Pool,
    PrepareSqlKey,
    PrepareStatement,
    PrepareParams)
  of
    {error, Error} ->
      emqx_rule_metrics:inc_actions_error(ActId),
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to store subscribe: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              insert_subscribe,
              5},
            line => 664})
      end,
      {badact, Error};
    _ ->
      emqx_rule_metrics:inc_actions_success(ActId),
      ok
  end.

lookup_subscribe(Pool, ActId, PrepareSqlKey,
    PrepareStatement, PrepareParams) ->
  case mysql_query(Pool,
    PrepareSqlKey,
    PrepareStatement,
    PrepareParams)
  of
    {ok, _Cols, []} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      [];
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Lookup subscription error: ~p",
              [Error]}
          end,
            mfa =>
            {emqx_backend_mysql_actions,
              lookup_subscribe,
              5},
            line => 677})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {error, Error};
    {ok, _Cols, Rows} ->
      emqx_rule_metrics:inc_actions_success(ActId),
      [{Topic, #{qos => Qos}} || [Topic, Qos] <- Rows]
  end.

i(true) -> 1;
i(false) -> 0.

to_undefined(<<>>) -> undefined;
to_undefined(0) -> undefined;
to_undefined(V) -> V.

logger_header() -> "[RuleEngine MySql] ".

