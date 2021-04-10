%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:59
%%%-------------------------------------------------------------------
-module(emqx_backend_redis_actions).
-author("root").
-export([logger_header/0]).

-behaviour(ecpool_worker).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").
-include("../include/emqx_backend_redis.hrl").


-export([connect/1]).

-export([on_resource_create_for_single/2,
  on_resource_create_for_sentinel/2,
  on_resource_create_for_cluster/2,
  on_get_resource_status/2,
  on_resource_destroy/2]).

-export([on_action_create_data_to_redis/2,
  on_action_data_to_redis/2,
  on_action_create_offline_msg/2,
  on_action_offline_msg_to_redis/2,
  on_action_create_lookup_sub/2,
  on_action_lookup_sub_to_redis/2]).

-resource_type(#{create =>
on_resource_create_for_single,
  description =>
  #{en =>
  <<82, 101, 100, 105, 115, 32, 83, 105, 110, 103, 108,
    101, 32, 77, 111, 100, 101>>,
    zh =>
    <<82, 101, 100, 105, 115, 32, 229, 141, 149, 232,
      138, 130, 231, 130, 185, 230, 168, 161, 229, 188,
      143>>},
  destroy => on_resource_destroy,
  name => backend_redis_single,
  params =>
  #{auto_reconnect =>
  #{default => true,
    description =>
    #{en =>
    <<73, 102, 32, 82, 101, 45, 116, 114,
      121, 32, 119, 104, 101, 110, 32, 116,
      104, 101, 32, 67, 111, 110, 110, 101,
      99, 116, 105, 111, 110, 32, 76, 111,
      115, 116>>,
      zh =>
      <<82, 101, 100, 105, 115, 32, 232, 191,
        158, 230, 142, 165, 230, 150, 173, 229,
        188, 128, 230, 151, 182, 230, 152, 175,
        229, 144, 166, 233, 135, 141, 232, 191,
        158>>},
    order => 5,
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
      <<67, 65, 32, 67, 101, 114, 116, 105,
        102, 105, 99, 97, 116, 101, 32, 102,
        105, 108, 101>>,
        zh =>
        <<67, 65, 32, 232, 175, 129, 228, 185,
          166, 230, 150, 135, 228, 187, 182>>},
      order => 7,
      title =>
      #{en =>
      <<67, 65, 32, 67, 101, 114, 116, 105,
        102, 105, 99, 97, 116, 101, 32, 70,
        105, 108, 101>>,
        zh =>
        <<67, 65, 32, 232, 175, 129, 228, 185,
          166, 230, 150, 135, 228, 187, 182>>},
      type => file},
    certfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 232, 175, 129, 228,
          185, 166, 230, 150, 135, 228, 187,
          182>>},
      order => 9,
      title =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<232, 175, 129, 228, 185, 166, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    database =>
    #{default => 0,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 149,
          176, 230, 141, 174, 229, 186, 147>>},
      order => 2,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 149,
          176, 230, 141, 174, 229, 186, 147>>},
      type => number},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 231, 167, 129, 233,
          146, 165, 230, 150, 135, 228, 187,
          182>>},
      order => 8,
      title =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<231, 167, 129, 233, 146, 165, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 175,
          134, 231, 160, 129>>},
      order => 3,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 175,
          134, 231, 160, 129>>},
      type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        82, 101, 100, 105, 115, 32, 67, 111,
        110, 110, 101, 99, 116, 105, 111, 110,
        32, 80, 111, 111, 108>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 232, 191,
          158, 230, 142, 165, 230, 177, 160, 229,
          164, 167, 229, 176, 143>>},
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
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 54,
      51, 55, 57>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 83, 101,
        114, 118, 101, 114>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 156,
          141, 229, 138, 161, 229, 153, 168, 229,
          156, 176, 229, 157, 128>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 83, 101,
        114, 118, 101, 114>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 156,
          141, 229, 138, 161, 229, 153, 168>>},
      type => string},
    ssl =>
    #{default => false,
      description =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 188,
          128, 229, 144, 175, 32, 83, 83, 76>>},
      order => 6,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<229, 188, 128, 229, 144, 175, 32, 83,
          83, 76>>},
      type => boolean},
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
      order => 10,
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
  #{en =>
  <<82, 101, 100, 105, 115, 32, 83, 105, 110, 103, 108,
    101, 32, 77, 111, 100, 101>>,
    zh =>
    <<82, 101, 100, 105, 115, 32, 229, 141, 149, 232,
      138, 130, 231, 130, 185, 230, 168, 161, 229, 188,
      143>>}}).

-resource_type(#{create =>
on_resource_create_for_sentinel,
  description =>
  #{en =>
  <<82, 101, 100, 105, 115, 32, 83, 101, 110, 116, 105,
    110, 101, 108, 32, 77, 111, 100, 101>>,
    zh =>
    <<82, 101, 100, 105, 115, 32, 83, 101, 110, 116, 105,
      110, 101, 108, 230, 168, 161, 229, 188, 143>>},
  destroy => on_resource_destroy,
  name => backend_redis_sentinel,
  params =>
  #{auto_reconnect =>
  #{default => true,
    description =>
    #{en =>
    <<73, 102, 32, 82, 101, 45, 116, 114,
      121, 32, 119, 104, 101, 110, 32, 116,
      104, 101, 32, 67, 111, 110, 110, 101,
      99, 116, 105, 111, 110, 32, 76, 111,
      115, 116>>,
      zh =>
      <<82, 101, 100, 105, 115, 32, 232, 191,
        158, 230, 142, 165, 230, 150, 173, 229,
        188, 128, 230, 151, 182, 230, 152, 175,
        229, 144, 166, 233, 135, 141, 232, 191,
        158>>},
    order => 6,
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
      <<67, 65, 32, 67, 101, 114, 116, 105,
        102, 105, 99, 97, 116, 101, 32, 102,
        105, 108, 101>>,
        zh =>
        <<67, 65, 32, 232, 175, 129, 228, 185,
          166, 230, 150, 135, 228, 187, 182>>},
      order => 10,
      title =>
      #{en =>
      <<67, 65, 32, 67, 101, 114, 116, 105,
        102, 105, 99, 97, 116, 101, 32, 70,
        105, 108, 101>>,
        zh =>
        <<67, 65, 32, 232, 175, 129, 228, 185,
          166, 230, 150, 135, 228, 187, 182>>},
      type => file},
    certfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 232, 175, 129, 228,
          185, 166, 230, 150, 135, 228, 187,
          182>>},
      order => 9,
      title =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<232, 175, 129, 228, 185, 166, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    database =>
    #{default => 0,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 149,
          176, 230, 141, 174, 229, 186, 147>>},
      order => 3,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 149,
          176, 230, 141, 174, 229, 186, 147>>},
      type => number},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 231, 167, 129, 233,
          146, 165, 230, 150, 135, 228, 187,
          182>>},
      order => 8,
      title =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<231, 167, 129, 233, 146, 165, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 175,
          134, 231, 160, 129>>},
      order => 4,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 175,
          134, 231, 160, 129>>},
      type => string},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        82, 101, 100, 105, 115, 32, 67, 111,
        110, 110, 101, 99, 116, 105, 111, 110,
        32, 80, 111, 111, 108>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 232, 191,
          158, 230, 142, 165, 230, 177, 160, 229,
          164, 167, 229, 176, 143>>},
      order => 5,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    sentinel =>
    #{default =>
    <<109, 121, 115, 101, 110, 116, 105, 110, 101,
      108>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 83, 101,
        110, 116, 105, 110, 101, 108, 32, 78,
        97, 109, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 83, 101,
          110, 116, 105, 110, 101, 108, 32, 229,
          144, 141, 229, 173, 151>>},
      order => 2, required => true,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 83, 101,
        110, 116, 105, 110, 101, 108, 32, 78,
        97, 109, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 83, 101,
          110, 116, 105, 110, 101, 108, 32, 229,
          144, 141, 229, 173, 151>>},
      type => string},
    servers =>
    #{default =>
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 50,
      54, 51, 55, 57, 44, 49, 50, 55, 46, 48, 46,
      48, 46, 50, 58, 50, 54, 51, 55, 57, 44, 49,
      50, 55, 46, 48, 46, 48, 46, 51, 58, 50, 54,
      51, 55, 57>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 83, 101,
        110, 116, 105, 110, 101, 108, 32, 83,
        101, 114, 118, 101, 114, 32, 65, 100,
        100, 114, 101, 115, 115, 44, 32, 77,
        117, 108, 116, 105, 112, 108, 101, 32,
        110, 111, 100, 101, 115, 32, 115, 101,
        112, 97, 114, 97, 116, 101, 100, 32,
        98, 121, 32, 99, 111, 109, 109, 97,
        115>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 83, 101,
          110, 116, 105, 110, 101, 108, 32, 230,
          156, 141, 229, 138, 161, 229, 153, 168,
          229, 156, 176, 229, 157, 128, 44, 32,
          229, 164, 154, 232, 138, 130, 231, 130,
          185, 228, 189, 191, 231, 148, 168, 233,
          128, 151, 229, 143, 183, 229, 136, 134,
          233, 154, 148>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 83, 101,
        110, 116, 105, 110, 101, 108, 32, 83,
        101, 114, 118, 101, 114, 115>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 156,
          141, 229, 138, 161, 229, 153, 168, 229,
          136, 151, 232, 161, 168>>},
      type => string},
    ssl =>
    #{default => false,
      description =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 188,
          128, 229, 144, 175, 32, 83, 83, 76>>},
      order => 7,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<229, 188, 128, 229, 144, 175, 32, 83,
          83, 76>>},
      type => boolean}},
  status => on_get_resource_status,
  title =>
  #{en =>
  <<82, 101, 100, 105, 115, 32, 83, 101, 110, 116, 105,
    110, 101, 108, 32, 77, 111, 100, 101>>,
    zh =>
    <<82, 101, 100, 105, 115, 32, 83, 101, 110, 116, 105,
      110, 101, 108, 230, 168, 161, 229, 188, 143>>}}).

-resource_type(#{create =>
on_resource_create_for_cluster,
  description =>
  #{en =>
  <<82, 101, 100, 105, 115, 32, 67, 108, 117, 115, 116,
    101, 114, 32, 77, 111, 100, 101>>,
    zh =>
    <<82, 101, 100, 105, 115, 32, 67, 108, 117, 115, 116,
      101, 114, 230, 168, 161, 229, 188, 143>>},
  destroy => on_resource_destroy,
  name => backend_redis_cluster,
  params =>
  #{cacertfile =>
  #{default => <<>>,
    description =>
    #{en =>
    <<67, 65, 32, 67, 101, 114, 116, 105,
      102, 105, 99, 97, 116, 101, 32, 102,
      105, 108, 101>>,
      zh =>
      <<67, 65, 32, 232, 175, 129, 228, 185,
        166, 230, 150, 135, 228, 187, 182>>},
    order => 8,
    title =>
    #{en =>
    <<67, 65, 32, 67, 101, 114, 116, 105,
      102, 105, 99, 97, 116, 101, 32, 70,
      105, 108, 101>>,
      zh =>
      <<67, 65, 32, 232, 175, 129, 228, 185,
        166, 230, 150, 135, 228, 187, 182>>},
    type => file},
    certfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 232, 175, 129, 228,
          185, 166, 230, 150, 135, 228, 187,
          182>>},
      order => 7,
      title =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<232, 175, 129, 228, 185, 166, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    database =>
    #{default => 0,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 149,
          176, 230, 141, 174, 229, 186, 147>>},
      order => 2,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 68, 97,
        116, 97, 98, 97, 115, 101>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 230, 149,
          176, 230, 141, 174, 229, 186, 147>>},
      type => number},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 231, 167, 129, 233,
          146, 165, 230, 150, 135, 228, 187,
          182>>},
      order => 6,
      title =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<231, 167, 129, 233, 146, 165, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 175,
          134, 231, 160, 129>>},
      order => 3,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 80, 97,
        115, 115, 119, 111, 114, 100>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 175,
          134, 231, 160, 129>>},
      type => string},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        82, 101, 100, 105, 115, 32, 67, 111,
        110, 110, 101, 99, 116, 105, 111, 110,
        32, 80, 111, 111, 108>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 232, 191,
          158, 230, 142, 165, 230, 177, 160, 229,
          164, 167, 229, 176, 143>>},
      order => 4,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    servers =>
    #{default =>
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 54,
      51, 55, 57, 44, 49, 50, 55, 46, 48, 46, 48,
      46, 50, 58, 54, 51, 55, 57, 44, 49, 50, 55,
      46, 48, 46, 48, 46, 51, 58, 54, 51, 55, 57>>,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 67, 108,
        117, 115, 116, 101, 114, 32, 83, 101,
        114, 118, 101, 114, 32, 65, 100, 100,
        114, 101, 115, 115, 44, 32, 77, 117,
        108, 116, 105, 112, 108, 101, 32, 110,
        111, 100, 101, 115, 32, 115, 101, 112,
        97, 114, 97, 116, 101, 100, 32, 98,
        121, 32, 99, 111, 109, 109, 97, 115>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 67, 108,
          117, 115, 116, 101, 114, 32, 230, 156,
          141, 229, 138, 161, 229, 153, 168, 229,
          156, 176, 229, 157, 128, 44, 32, 229,
          164, 154, 232, 138, 130, 231, 130, 185,
          228, 189, 191, 231, 148, 168, 233, 128,
          151, 229, 143, 183, 229, 136, 134, 233,
          154, 148>>},
      order => 1, required => true,
      title =>
      <<82, 101, 100, 105, 115, 32, 67, 108, 117,
        115, 116, 101, 114, 32, 83, 101, 114, 118,
        101, 114, 115>>,
      type => string},
    ssl =>
    #{default => false,
      description =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 188,
          128, 229, 144, 175, 32, 83, 83, 76>>},
      order => 5,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<229, 188, 128, 229, 144, 175, 32, 83,
          83, 76>>},
      type => boolean}},
  status => on_get_resource_status,
  title =>
  #{en =>
  <<82, 101, 100, 105, 115, 32, 67, 108, 117, 115, 116,
    101, 114, 32, 77, 111, 100, 101>>,
    zh =>
    <<82, 101, 100, 105, 115, 32, 67, 108, 117, 115, 116,
      101, 114, 230, 168, 161, 229, 188, 143>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_redis,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 82, 101, 100, 105, 115>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 82, 101, 100, 105,
      115>>},
  for => '$any', name => data_to_redis,
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
    cmd =>
    #{description =>
    #{en =>
    <<82, 101, 100, 105, 115, 32, 67, 77, 68,
      32, 119, 105, 116, 104, 32, 80, 108, 97,
      99, 101, 104, 111, 108, 100, 101, 114,
      115, 32, 102, 111, 114, 32, 73, 110, 115,
      101, 114, 116, 105, 110, 103, 47, 85,
      112, 100, 97, 116, 105, 110, 103, 32, 68,
      97, 116, 97>>,
      zh =>
      <<82, 101, 100, 105, 115, 32, 229, 145,
        189, 228, 187, 164, 239, 188, 140, 229,
        143, 175, 229, 140, 133, 229, 144, 171,
        229, 141, 160, 228, 189, 141, 231, 172,
        166, 239, 188, 140, 231, 148, 168, 228,
        187, 165, 230, 155, 180, 230, 150, 176,
        230, 136, 150, 230, 143, 146, 229, 133,
        165, 230, 149, 176, 230, 141, 174>>},
      input => textarea, required => true,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 67, 77, 68>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 229, 145,
          189, 228, 187, 164>>},
      type => string}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 82, 101, 100, 105,
    115>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 82, 101, 100, 105,
      115>>},
  types =>
  [backend_redis_single,
    backend_redis_sentinel,
    backend_redis_cluster]}).

-rule_action(#{category => offline_msgs,
  create => on_action_create_offline_msg,
  description =>
  #{en =>
  <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
    32, 116, 111, 32, 82, 101, 100, 105, 115>>,
    zh =>
    <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
      129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
      176, 32, 82, 101, 100, 105, 115>>},
  for => '$any', name => offline_msg_to_redis,
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
    ttl =>
    #{default => 7200,
      description =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 75, 101, 121,
        32, 84, 84, 76, 32, 85, 110, 105, 116,
        32, 40, 115, 101, 99, 111, 110, 100, 115,
        41>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 75, 101, 121,
          32, 232, 182, 133, 230, 156, 159, 230,
          151, 182, 233, 151, 180, 32, 229, 141,
          149, 228, 189, 141, 40, 231, 167, 146,
          41>>},
      required => false,
      title =>
      #{en =>
      <<82, 101, 100, 105, 115, 32, 75, 101, 121,
        32, 84, 84, 76>>,
        zh =>
        <<82, 101, 100, 105, 115, 32, 75, 101, 121,
          32, 232, 182, 133, 230, 156, 159, 230,
          151, 182, 233, 151, 180>>},
      type => number}},
  title =>
  #{en =>
  <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
    32, 116, 111, 32, 82, 101, 100, 105, 115>>,
    zh =>
    <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
      129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
      176, 32, 82, 101, 100, 105, 115>>},
  types =>
  [backend_redis_single,
    backend_redis_sentinel,
    backend_redis_cluster]}).

-rule_action(#{category => server_side_subscription,
  create => on_action_create_lookup_sub,
  description =>
  #{en =>
  <<71, 101, 116, 32, 83, 117, 98, 115, 99, 114, 105,
    112, 116, 105, 111, 110, 32, 76, 105, 115, 116, 32,
    70, 114, 111, 109, 32, 82, 101, 100, 105, 115>>,
    zh =>
    <<228, 187, 142, 32, 82, 101, 100, 105, 115, 32, 228,
      184, 173, 232, 142, 183, 229, 143, 150, 232, 174,
      162, 233, 152, 133, 229, 136, 151, 232, 161, 168>>},
  for => '$any', name => lookup_sub_to_redis,
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
    32, 82, 101, 100, 105, 115>>,
    zh =>
    <<228, 187, 142, 32, 82, 101, 100, 105, 115, 32, 228,
      184, 173, 232, 142, 183, 229, 143, 150, 232, 174,
      162, 233, 152, 133, 229, 136, 151, 232, 161, 168>>},
  types =>
  [backend_redis_single,
    backend_redis_sentinel,
    backend_redis_cluster]}).

-vsn("4.2.5").

on_resource_create_for_single(ResId,
    Config = #{<<"server">> := Server}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_redis_single, ResId]}
      end,
        mfa =>
        {emqx_backend_redis_actions,
          on_resource_create_for_single,
          2},
        line => 388})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(eredis),
  {Host, Port} = format_server(str(Server)),
  Options = [{sentinel, ""},
    {pool_size, maps:get(<<"pool_size">>, Config, 8)},
    {database, maps:get(<<"database">>, Config, 0)},
    {password, str(maps:get(<<"password">>, Config, ""))},
    {auto_reconnect,
      case maps:get(<<"auto_reconnect">>, Config, true) of
        true -> 2;
        false -> false
      end},
    {host, Host},
    {port, Port}],
  PoolName = pool_name(ResId),
  start_resource(ResId,
    PoolName,
    Options ++ get_ssl_options(Config, ResId),
    backend_redis_single),
  #{<<"pool">> => #{single => PoolName}}.

on_resource_create_for_sentinel(ResId,
    Config = #{<<"servers">> := Servers,
      <<"sentinel">> := Sentinel}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_redis_sentinel, ResId]}
      end,
        mfa =>
        {emqx_backend_redis_actions,
          on_resource_create_for_sentinel,
          2},
        line => 407})
  end,
  {ok, _} = application:ensure_all_started(ecpool),
  {ok, _} = application:ensure_all_started(eredis),
  Servers1 = [format_server(Server)
    || Server <- string:tokens(str(Servers), ",")],
  Options = [{sentinel, str(Sentinel)},
    {pool_size, maps:get(<<"pool_size">>, Config, 8)},
    {database, maps:get(<<"database">>, Config, 0)},
    {password, str(maps:get(<<"password">>, Config, ""))},
    {auto_reconnect,
      case maps:get(<<"auto_reconnect">>, Config, true) of
        true -> 2;
        false -> false
      end},
    {servers, Servers1}],
  PoolName = pool_name(ResId),
  start_resource(ResId,
    PoolName,
    Options ++ get_ssl_options(Config, ResId),
    backend_redis_sentinel),
  #{<<"pool">> => #{sentinel => PoolName}}.

start_resource(ResId, PoolName, Options, Type) ->
  case ecpool:start_sup_pool(PoolName,
    emqx_backend_redis_actions,
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
              [Type, ResId]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              start_resource,
              4},
            line => 427})
      end;
    {error, {already_started, _Pid}} ->
      on_resource_destroy(ResId, PoolName),
      start_resource(ResId, PoolName, Options, Type);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiate Resource ~p failed, ResId: "
              "~p, ~0p",
              [Type, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              start_resource,
              4},
            line => 433})
      end,
      error({Type, {ResId, create_failed}})
  end.

on_resource_create_for_cluster(ResId,
    Config = #{<<"servers">> := Servers}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, ResId: ~p",
          [backend_redis_cluster, ResId]}
      end,
        mfa =>
        {emqx_backend_redis_actions,
          on_resource_create_for_cluster,
          2},
        line => 438})
  end,
  {ok, _} =
    application:ensure_all_started(eredis_cluster),
  {ok, _} = application:ensure_all_started(eredis),
  Servers1 = [format_server(Server)
    || Server <- string:tokens(str(Servers), ",")],
  Options = [{pool_size,
    maps:get(<<"pool_size">>, Config, 8)},
    {database, maps:get(<<"database">>, Config, 0)},
    {password, str(maps:get(<<"password">>, Config, ""))},
    {servers, Servers1}],
  PoolName = pool_name(ResId),
  start_cluster_resource(ResId,
    PoolName,
    Options ++ get_ssl_options(Config, ResId)),
  #{<<"pool">> => #{cluster => PoolName}}.

start_cluster_resource(ResId, PoolName, Options) ->
  case eredis_cluster:start_pool(PoolName, Options) of
    {ok, _} ->
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiated Resource ~p Successfully, "
              "ResId: ~p",
              [backend_redis_cluster, ResId]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              start_cluster_resource,
              3},
            line => 453})
      end;
    {error, {already_started, _Pid}} ->
      on_resource_destroy(ResId,
        #{<<"pool">> => #{cluster => PoolName}}),
      start_cluster_resource(ResId, PoolName, Options);
    Error ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Initiate Resource ~p failed, ResId: "
              "~p, ~0p",
              [backend_redis_cluster, ResId, Error]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              start_cluster_resource,
              3},
            line => 459})
      end,
      error(Error)
  end.

-spec on_get_resource_status(ResId :: binary(),
    Params :: map()) -> Status :: map().

on_get_resource_status(_ResId,
    #{<<"pool">> := #{cluster := PoolName}}) ->
  Workers = gen_server:call(whereis(PoolName),
    get_all_workers),
  #{is_alive =>
  length(Workers) > 0 andalso
    lists:all(fun ({_, Pid, _, _}) ->
      eredis_cluster_pool_worker:is_connected(Pid) =:=
        true
              end,
      Workers)};
on_get_resource_status(ResId,
    #{<<"pool">> := #{single := PoolName}}) ->
  on_get_resource_status(ResId, PoolName);
on_get_resource_status(ResId,
    #{<<"pool">> := #{sentinel := PoolName}}) ->
  on_get_resource_status(ResId, PoolName);
on_get_resource_status(_ResId, PoolName) ->
  #{is_alive =>
  emqx_rule_actions_utils:is_resource_alive(PoolName,
    fun (Client) ->
      {R, _} =
        eredis:q(Client,
          ["PING"]),
      R
    end)}.

on_resource_destroy(ResId,
    #{<<"pool">> := #{cluster := PoolName}}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [cluster, ResId]}
      end,
        mfa =>
        {emqx_backend_redis_actions, on_resource_destroy, 2},
        line => 482})
  end,
  case eredis_cluster:stop_pool(PoolName) of
    ok -> ok;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource ~p failed, ResId: ~p, ~p",
              [cluster, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              on_resource_destroy,
              2},
            line => 486})
      end,
      error({{cluster, ResId}, destroy_failed})
  end;
on_resource_destroy(ResId,
    #{<<"pool">> := #{single := PoolName}}) ->
  on_resource_destroy(ResId, PoolName);
on_resource_destroy(ResId,
    #{<<"pool">> := #{sentinel := PoolName}}) ->
  on_resource_destroy(ResId, PoolName);
on_resource_destroy(ResId, PoolName) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++ "Destroying ResId: ~p",
          [ResId]}
      end,
        mfa =>
        {emqx_backend_redis_actions, on_resource_destroy, 2},
        line => 495})
  end,
  case ecpool:stop_sup_pool(PoolName) of
    ok -> ok;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource failed, ResId: ~p, ~p",
              [ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              on_resource_destroy,
              2},
            line => 499})
      end,
      error({ResId, destroy_failed})
  end.

on_action_create_data_to_redis(ActId,
    #{<<"pool">> := PoolName, <<"cmd">> := Cmd} =
      Params) ->
  Tokens = emqx_rule_utils:preproc_cmd(Cmd),
  {[{'Params', Params},
    {'Cmd', Cmd},
    {'PoolName', PoolName},
    {'ActId', ActId},
    {'Tokens', Tokens}],
    Params}.

on_action_data_to_redis(Data,
    #{'__bindings__' :=
    #{'PoolName' := PoolName, 'ActId' := ActId,
      'Tokens' := Tokens}}) ->
  case q(PoolName, emqx_rule_utils:proc_cmd(Tokens, Data))
  of
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Data to redis fail, reason: ~p",
              [Reason]}
          end,
            mfa =>
            {emqx_backend_redis_actions,
              on_action_data_to_redis,
              2},
            line => 515})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason};
    _ -> emqx_rule_metrics:inc_actions_success(ActId)
  end.

on_action_create_offline_msg(ActId,
    #{<<"pool">> := PoolName, <<"ttl">> := TTL} =
      Params) ->
  {[{'Params', Params},
    {'PoolName', PoolName},
    {'TTL', TTL},
    {'ActId', ActId}],
    Params}.

on_action_offline_msg_to_redis(Msg = #{event := Event,
  topic := Topic},
    #{'__bindings__' :=
    #{'PoolName' := PoolName, 'ActId' := ActId,
      'TTL' := TTL}}) ->
  [{Type, Pool}] = maps:to_list(PoolName),
  case Event of
    'message.acked' ->
      MsgId = emqx_guid:from_hexstr(maps:get(id, Msg)),
      emqx_backend_redis_cli:message_acked_for_queue({Type,
        Pool},
        [{topic, Topic},
          {msgid, MsgId}]),
      emqx_rule_metrics:inc_actions_success(ActId);
    'message.publish' ->
      #{id := MsgId, qos := Qos, flags := Flags,
        payload := Payload, publish_received_at := Ts,
        clientid := From} =
        Msg,
      NewMsg = #message{id = emqx_guid:from_hexstr(MsgId),
        from = From, qos = Qos, topic = Topic,
        payload = Payload, timestamp = Ts, flags = Flags},
      emqx_backend_redis_cli:message_publish({Type, Pool},
        NewMsg,
        TTL),
      emqx_rule_metrics:inc_actions_success(ActId);
    'session.subscribed' ->
      MsgList =
        emqx_backend_redis_cli:message_fetch_for_queue({Type,
          Pool},
          [{topic,
            Topic}]),
      [self() ! {deliver, Topic, M} || M <- MsgList],
      emqx_rule_metrics:inc_actions_success(ActId)
  end.

on_action_create_lookup_sub(ActId,
    #{<<"pool">> := PoolName} = Params) ->
  {[{'Params', Params},
    {'PoolName', PoolName},
    {'ActId', ActId}],
    Params}.

on_action_lookup_sub_to_redis(Msg = #{event := Event,
  clientid := Clientid},
    #{'__bindings__' :=
    #{'PoolName' := PoolName,
      'ActId' := ActId}}) ->
  [{Type, Pool}] = maps:to_list(PoolName),
  case Event of
    'client.connected' ->
      case emqx_backend_redis_cli:subscribe_lookup({Type,
        Pool},
        [{clientid, Clientid}])
      of
        [] -> ok;
        TopicTable -> self() ! {subscribe, TopicTable}
      end,
      emqx_rule_metrics:inc_actions_success(ActId);
    'session.subscribed' ->
      #{topic := Topic, qos := QoS} = Msg,
      Key = <<"mqtt:sub:", Clientid/binary>>,
      emqx_backend_redis_cli:q({Type, Pool},
        [<<"HSET">>, Key, Topic, QoS]),
      emqx_rule_metrics:inc_actions_success(ActId)
  end.

connect(Opts) ->
  Sentinel = proplists:get_value(sentinel, Opts),
  case Sentinel =:= "" of
    true -> start(Opts);
    false ->
      eredis_sentinel:start_link(proplists:get_value(servers,
        Opts)),
      start(Sentinel, Opts)
  end.

start(Opts) ->
  eredis:start_link(proplists:get_value(host, Opts),
    proplists:get_value(port, Opts),
    proplists:get_value(database, Opts),
    proplists:get_value(password, Opts),
    3000,
    5000,
    proplists:get_value(options, Opts, [])).

start(Sentinel, Opts) ->
  eredis:start_link("sentinel:" ++ Sentinel,
    proplists:get_value(port, Opts, 6379),
    proplists:get_value(database, Opts),
    proplists:get_value(password, Opts),
    3000,
    5000,
    proplists:get_value(options, Opts, [])).

q(Params, Cmd) -> do_q(Params, Cmd).

do_q(#{cluster := Pool}, Cmd) ->
  logger:debug("redis cmd:~p", [Cmd]),
  eredis_cluster:q(Pool, Cmd);
do_q(#{sentinel := Pool}, Cmd) ->
  logger:debug("redis Pool:~p, cmd:~p", [Pool, Cmd]),
  ecpool:with_client(Pool,
    fun (C) -> eredis:q(C, Cmd) end);
do_q(#{single := Pool}, Cmd) ->
  logger:debug("redis Pool:~p, cmd:~p", [Pool, Cmd]),
  ecpool:with_client(Pool,
    fun (C) -> eredis:q(C, Cmd) end).

format_server(Servers) ->
  case string:tokens(Servers, ":") of
    [Domain] -> {Domain, 6379};
    [Domain, Port] -> {Domain, list_to_integer(Port)}
  end.

pool_name(ResId) ->
  list_to_atom("backend_redis:" ++ str(ResId)).

str(List) when is_list(List) -> List;
str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(Atom) when is_atom(Atom) -> atom_to_list(Atom).

get_ssl_options(Config, ResId) ->
  case maps:get(<<"ssl">>, Config, false) of
    true ->
      [{options,
        [{ssl_options,
          emqx_rule_actions_utils:get_ssl_opts(Config, ResId)}]}];
    _ -> [{options, []}]
  end.

logger_header() -> "".

