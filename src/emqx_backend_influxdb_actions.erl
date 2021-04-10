%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午6:58
%%%-------------------------------------------------------------------
-module(emqx_backend_influxdb_actions).
-author("root").
-export([logger_header/0]).

-behaviour(ecpool_worker).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").


-export([connect/1]).

-import(emqx_rule_utils, [str/1]).

-export([on_udp_resource_create/2,
  on_http_resource_create/2,
  on_resource_destroy/2,
  on_get_resource_status/2]).

-export([on_action_create_data_to_influxdb/2,
  on_action_data_to_influxdb/2]).

-resource_type(#{create => on_udp_resource_create,
  description =>
  #{en =>
  <<87, 114, 105, 116, 101, 32, 100, 97, 116, 97, 32,
    112, 111, 105, 110, 116, 115, 32, 116, 111, 32, 73,
    110, 102, 108, 117, 120, 68, 66, 32, 117, 115, 105,
    110, 103, 32, 85, 68, 80, 32, 112, 114, 111, 116,
    111, 99, 111, 108>>,
    zh =>
    <<228, 189, 191, 231, 148, 168, 32, 85, 68, 80, 32,
      229, 141, 143, 232, 174, 174, 229, 176, 134, 230,
      149, 176, 230, 141, 174, 231, 130, 185, 229, 134,
      153, 229, 133, 165, 32, 73, 110, 102, 108, 117,
      120, 68, 66>>},
  destroy => on_resource_destroy,
  name => backend_influxdb_udp,
  params =>
  #{batch_size =>
  #{default => 1000,
    description =>
    #{en =>
    <<84, 104, 101, 32, 110, 117, 109, 98,
      101, 114, 32, 111, 102, 32, 100, 97,
      116, 97, 32, 112, 111, 105, 110, 116,
      115, 32, 116, 111, 32, 99, 111, 108,
      108, 101, 99, 116, 32, 105, 110, 32,
      97, 32, 98, 97, 116, 99, 104>>,
      zh =>
      <<230, 137, 185, 233, 135, 143, 230, 148,
        182, 233, 155, 134, 231, 154, 132, 230,
        149, 176, 230, 141, 174, 231, 130, 185,
        230, 149, 176, 233, 135, 143>>},
    order => 3,
    title =>
    #{en =>
    <<66, 97, 116, 99, 104, 32, 83, 105, 122,
      101>>,
      zh =>
      <<230, 137, 185, 233, 135, 143, 229, 134,
        153, 229, 133, 165, 229, 164, 167, 229,
        176, 143>>},
    type => number},
    host =>
    #{default => <<49, 50, 55, 46, 48, 46, 48, 46, 49>>,
      description =>
      #{en =>
      <<85, 68, 80, 32, 97, 100, 100, 114, 101,
        115, 115, 32, 111, 102, 32, 73, 110,
        102, 108, 117, 120, 68, 66>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 85, 68, 80, 32, 230, 156, 141, 229,
          138, 161, 229, 153, 168, 229, 156, 176,
          229, 157, 128>>},
      order => 1,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 72, 111, 115, 116>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 228, 184, 187, 230, 156, 186>>},
      type => string},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        73, 110, 102, 108, 117, 120, 68, 66,
        32, 119, 114, 105, 116, 105, 110, 103,
        32, 112, 114, 111, 99, 101, 115, 115,
        32, 112, 111, 111, 108>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 229, 134, 153, 232, 191, 155, 231,
          168, 139, 230, 177, 160, 229, 164, 167,
          229, 176, 143>>},
      order => 4,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 155, 231, 168, 139, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    port =>
    #{default => 8089,
      description =>
      #{en =>
      <<80, 111, 114, 116, 32, 116, 111, 32,
        99, 111, 110, 110, 101, 99, 116, 32,
        116, 111>>,
        zh =>
        <<232, 166, 129, 232, 191, 158, 230, 142,
          165, 231, 154, 132, 231, 171, 175, 229,
          143, 163>>},
      order => 2,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 80, 111, 114, 116>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 231, 171, 175, 229, 143, 163>>},
      type => number}},
  status => on_get_resource_status,
  title =>
  #{en =>
  <<73, 110, 102, 108, 117, 120, 68, 66, 32, 85, 68,
    80, 32, 83, 101, 114, 118, 105, 99, 101>>,
    zh =>
    <<73, 110, 102, 108, 117, 120, 68, 66, 32, 85, 68,
      80, 32, 230, 156, 141, 229, 138, 161>>}}).

-resource_type(#{create => on_http_resource_create,
  description =>
  #{en =>
  <<87, 114, 105, 116, 101, 32, 100, 97, 116, 97, 32,
    112, 111, 105, 110, 116, 115, 32, 116, 111, 32, 73,
    110, 102, 108, 117, 120, 68, 66, 32, 117, 115, 105,
    110, 103, 32, 72, 84, 84, 80, 32, 112, 114, 111,
    116, 111, 99, 111, 108>>,
    zh =>
    <<228, 189, 191, 231, 148, 168, 32, 72, 84, 84, 80,
      32, 229, 141, 143, 232, 174, 174, 229, 176, 134,
      230, 149, 176, 230, 141, 174, 231, 130, 185, 229,
      134, 153, 229, 133, 165, 32, 73, 110, 102, 108,
      117, 120, 68, 66>>},
  destroy => on_resource_destroy,
  name => backend_influxdb_http,
  params =>
  #{batch_size =>
  #{default => 1000,
    description =>
    #{en =>
    <<84, 104, 101, 32, 110, 117, 109, 98,
      101, 114, 32, 111, 102, 32, 100, 97,
      116, 97, 32, 112, 111, 105, 110, 116,
      115, 32, 116, 111, 32, 99, 111, 108,
      108, 101, 99, 116, 32, 105, 110, 32,
      97, 32, 98, 97, 116, 99, 104>>,
      zh =>
      <<230, 137, 185, 233, 135, 143, 230, 148,
        182, 233, 155, 134, 231, 154, 132, 230,
        149, 176, 230, 141, 174, 231, 130, 185,
        230, 149, 176, 233, 135, 143>>},
    order => 7,
    title =>
    #{en =>
    <<66, 97, 116, 99, 104, 32, 83, 105, 122,
      101>>,
      zh =>
      <<230, 137, 185, 233, 135, 143, 229, 134,
        153, 229, 133, 165, 229, 164, 167, 229,
        176, 143>>},
    type => number},
    cacertfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<67, 65, 32, 67, 101, 114, 116, 105,
        102, 105, 99, 97, 116, 101, 115, 32,
        70, 105, 108, 101>>,
        zh =>
        <<67, 65, 32, 232, 175, 129, 228, 185,
          166, 230, 150, 135, 228, 187, 182>>},
      order => 11,
      title =>
      #{en =>
      <<67, 65, 32, 67, 101, 114, 116, 105,
        102, 105, 99, 97, 116, 101, 115, 32,
        70, 105, 108, 101>>,
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
        <<232, 175, 129, 228, 185, 166, 230, 150,
          135, 228, 187, 182>>},
      order => 13,
      title =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<232, 175, 129, 228, 185, 166, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    database =>
    #{default => <<109, 121, 100, 98>>,
      description =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 68, 97, 116, 97, 98, 97, 115,
        101>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 230, 149, 176, 230, 141, 174, 229,
          186, 147, 229, 144, 141>>},
      order => 3,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 68, 97, 116, 97, 98, 97, 115,
        101>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 230, 149, 176, 230, 141, 174, 229,
          186, 147, 229, 144, 141>>},
      type => string},
    host =>
    #{default => <<49, 50, 55, 46, 48, 46, 48, 46, 49>>,
      description =>
      #{en =>
      <<72, 84, 84, 80, 47, 72, 84, 84, 80, 83,
        32, 97, 100, 100, 114, 101, 115, 115,
        32, 111, 102, 32, 73, 110, 102, 108,
        117, 120, 68, 66>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 72, 84, 84, 80, 47, 72, 84, 84, 80,
          83, 32, 230, 156, 141, 229, 138, 161,
          229, 153, 168, 229, 156, 176, 229, 157,
          128>>},
      order => 1,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 72, 111, 115, 116>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 228, 184, 187, 230, 156, 186>>},
      type => string},
    https_enabled =>
    #{default => false,
      description =>
      #{en =>
      <<68, 101, 116, 101, 114, 109, 105, 110,
        101, 115, 32, 119, 104, 101, 116, 104,
        101, 114, 32, 72, 84, 84, 80, 83, 32,
        105, 115, 32, 101, 110, 97, 98, 108,
        101, 100>>,
        zh =>
        <<231, 161, 174, 229, 174, 154, 230, 152,
          175, 229, 144, 166, 229, 144, 175, 231,
          148, 168, 32, 72, 84, 84, 80, 83>>},
      order => 9,
      title =>
      #{en =>
      <<72, 84, 84, 80, 83, 32, 69, 110, 97,
        98, 108, 101, 100>>,
        zh =>
        <<229, 144, 175, 231, 148, 168, 32, 72,
          84, 84, 80, 83>>},
      type => boolean},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<8, 229, 175, 134, 233, 146, 165, 230,
          150, 135, 228, 187, 182>>},
      order => 12,
      title =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<229, 175, 134, 233, 146, 165, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<80, 97, 115, 115, 119, 111, 114, 100,
        32, 116, 111, 32, 99, 111, 110, 110,
        101, 99, 116, 32, 116, 111, 32, 73,
        110, 102, 108, 117, 120, 68, 66>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 229, 136,
          176, 32, 73, 110, 102, 108, 117, 120,
          68, 66, 32, 231, 154, 132, 229, 175,
          134, 231, 160, 129>>},
      order => 5,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 80, 97, 115, 115, 119, 111, 114,
        100>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 229, 175, 134, 231, 160, 129>>},
      type => password},
    pool_size =>
    #{default => 8,
      description =>
      #{en =>
      <<83, 105, 122, 101, 32, 111, 102, 32,
        73, 110, 102, 108, 117, 120, 68, 66,
        32, 119, 114, 105, 116, 105, 110, 103,
        32, 112, 114, 111, 99, 101, 115, 115,
        32, 112, 111, 111, 108>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 229, 134, 153, 232, 191, 155, 231,
          168, 139, 230, 177, 160, 229, 164, 167,
          229, 176, 143>>},
      order => 8,
      title =>
      #{en =>
      <<80, 111, 111, 108, 32, 83, 105, 122,
        101>>,
        zh =>
        <<232, 191, 155, 231, 168, 139, 230, 177,
          160, 229, 164, 167, 229, 176, 143>>},
      type => number},
    port =>
    #{default => 8086,
      description =>
      #{en =>
      <<80, 111, 114, 116, 32, 116, 111, 32,
        99, 111, 110, 110, 101, 99, 116, 32,
        116, 111>>,
        zh =>
        <<232, 166, 129, 232, 191, 158, 230, 142,
          165, 231, 154, 132, 231, 171, 175, 229,
          143, 163>>},
      order => 2,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 80, 111, 114, 116>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 231, 171, 175, 229, 143, 163>>},
      type => number},
    precision =>
    #{default => <<109, 115>>,
      description =>
      #{en =>
      <<80, 114, 101, 99, 105, 115, 105, 111,
        110, 32, 111, 102, 32, 116, 105, 109,
        101, 115, 116, 97, 109, 112>>,
        zh =>
        <<230, 151, 182, 233, 151, 180, 230, 136,
          179, 231, 178, 190, 229, 186, 166>>},
      enum =>
      [<<110, 115>>,
        <<117, 115>>,
        <<109, 115>>,
        <<115>>,
        <<109>>,
        <<104>>],
      order => 6,
      title =>
      #{en =>
      <<80, 114, 101, 99, 105, 115, 105, 111,
        110, 32, 111, 102, 32, 116, 105, 109,
        101, 115, 116, 97, 109, 112>>,
        zh =>
        <<230, 151, 182, 233, 151, 180, 230, 136,
          179, 231, 178, 190, 229, 186, 166>>},
      type => string},
    tls_version =>
    #{default => <<116, 108, 115, 118, 49, 46, 50>>,
      description =>
      #{en =>
      <<84, 76, 83, 32, 112, 114, 111, 116,
        111, 99, 111, 108, 32, 118, 101, 114,
        115, 105, 111, 110>>,
        zh =>
        <<8, 84, 76, 83, 32, 229, 141, 143, 232,
          174, 174, 231, 137, 136, 230, 156,
          172>>},
      enum =>
      [<<116, 108, 115, 118, 49>>,
        <<116, 108, 115, 118, 49, 46, 49>>,
        <<116, 108, 115, 118, 49, 46, 50>>,
        <<116, 108, 115, 118, 49, 46, 51>>],
      order => 10,
      title =>
      #{en =>
      <<84, 76, 83, 32, 80, 114, 111, 116, 111,
        99, 111, 108, 32, 86, 101, 114, 115,
        105, 111, 110>>,
        zh =>
        <<84, 76, 83, 32, 229, 141, 143, 232,
          174, 174, 231, 137, 136, 230, 156,
          172>>},
      type => string},
    username =>
    #{default => <<>>,
      description =>
      #{en =>
      <<85, 115, 101, 114, 110, 97, 109, 101,
        32, 116, 111, 32, 99, 111, 110, 110,
        101, 99, 116, 32, 116, 111, 32, 73,
        110, 102, 108, 117, 120, 68, 66>>,
        zh =>
        <<232, 191, 158, 230, 142, 165, 229, 136,
          176, 32, 73, 110, 102, 108, 117, 120,
          68, 66, 32, 231, 154, 132, 231, 148,
          168, 230, 136, 183, 229, 144, 141>>},
      order => 4,
      title =>
      #{en =>
      <<73, 110, 102, 108, 117, 120, 68, 66,
        32, 85, 115, 101, 114, 110, 97, 109,
        101>>,
        zh =>
        <<73, 110, 102, 108, 117, 120, 68, 66,
          32, 231, 148, 168, 230, 136, 183, 229,
          144, 141>>},
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
      order => 14,
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
  #{en =>
  <<73, 110, 102, 108, 117, 120, 68, 66, 32, 72, 84,
    84, 80, 32, 83, 101, 114, 118, 105, 99, 101>>,
    zh =>
    <<73, 110, 102, 108, 117, 120, 68, 66, 32, 72, 84,
      84, 80, 32, 230, 156, 141, 229, 138, 161>>}}).

-rule_action(#{category => data_persist,
  create => on_action_create_data_to_influxdb,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 73, 110, 102, 108, 117, 120, 68, 66>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 73, 110, 102, 108, 117,
      120, 68, 66>>},
  for => 'message.publish', name => data_to_influxdb,
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
    fields =>
    #{default => #{},
      description =>
      #{en =>
      <<70, 105, 101, 108, 100, 115, 44, 32, 115,
        112, 97, 114, 97, 116, 101, 100, 32, 98,
        121, 32, 99, 111, 109, 109, 97, 115>>,
        zh =>
        <<70, 105, 101, 108, 100, 115, 44, 32, 233,
          128, 154, 232, 191, 135, 233, 128, 151,
          229, 143, 183, 229, 136, 134, 233, 154,
          148>>},
      order => 3, required => true, schema => #{},
      title =>
      #{en => <<70, 105, 101, 108, 100, 115>>,
        zh => <<70, 105, 101, 108, 100, 115>>},
      type => object},
    measurement =>
    #{description =>
    #{en =>
    <<77, 101, 97, 115, 117, 114, 101, 109,
      101, 110, 116>>,
      zh =>
      <<77, 101, 97, 115, 117, 114, 101, 109,
        101, 110, 116>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<77, 101, 97, 115, 117, 114, 101, 109,
        101, 110, 116>>,
        zh =>
        <<77, 101, 97, 115, 117, 114, 101, 109,
          101, 110, 116>>},
      type => string},
    tags =>
    #{default => #{},
      description =>
      #{en =>
      <<84, 97, 103, 115, 44, 32, 115, 112, 97,
        114, 97, 116, 101, 100, 32, 98, 121, 32,
        99, 111, 109, 109, 97, 115>>,
        zh =>
        <<84, 97, 103, 115, 44, 32, 233, 128, 154,
          232, 191, 135, 233, 128, 151, 229, 143,
          183, 229, 136, 134, 233, 154, 148>>},
      order => 4, schema => #{},
      title =>
      #{en => <<84, 97, 103, 115>>,
        zh => <<84, 97, 103, 115>>},
      type => object},
    timestamp =>
    #{default => <<>>,
      description =>
      #{en =>
      <<84, 105, 109, 101, 115, 116, 97, 109,
        112>>,
        zh =>
        <<84, 105, 109, 101, 115, 116, 97, 109,
          112>>},
      order => 2,
      title =>
      #{en =>
      <<84, 105, 109, 101, 115, 116, 97, 109,
        112>>,
        zh =>
        <<84, 105, 109, 101, 115, 116, 97, 109,
          112>>},
      type => string}},
  title =>
  #{en =>
  <<68, 97, 116, 97, 32, 116, 111, 32, 73, 110, 102, 108,
    117, 120, 68, 66>>,
    zh =>
    <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 73, 110, 102, 108, 117,
      120, 68, 66>>},
  types =>
  [backend_influxdb_udp, backend_influxdb_http]}).

-vsn("4.2.5").

on_udp_resource_create(ResId, Params) ->
  on_resource_create(backend_influxdb_udp, ResId, Params).

on_http_resource_create(ResId, Params) ->
  on_resource_create(backend_influxdb_http,
    ResId,
    Params).

on_resource_create(Type, ResId, Params) ->
  {ok, _} = application:ensure_all_started(ecpool),
  PoolName = pool_name(ResId),
  Options = options(Type, ResId, Params),
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Initiating Resource ~p, Resource ID: ~p",
          [Type, ResId]}
      end,
        mfa =>
        {emqx_backend_influxdb_actions, on_resource_create, 3},
        line => 401})
  end,
  start_resource(ResId,
    Type,
    PoolName,
    [{pool_type, hash} | Options]),
  case test_resource_status(ResId, Type) of
    true -> ok;
    false -> error({{Type, ResId}, connection_failed})
  end,
  #{<<"pool">> => PoolName, <<"type">> => Type}.

start_resource(ResId, Type, PoolName, Options) ->
  case ecpool:start_sup_pool(PoolName,
    emqx_backend_influxdb_actions,
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
            {emqx_backend_influxdb_actions,
              start_resource,
              4},
            line => 412})
      end;
    {error, {already_started, _Pid}} ->
      on_resource_destroy(ResId,
        #{<<"pool">> => PoolName, <<"type">> => Type}),
      start_resource(ResId, Type, PoolName, Options);
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
            {emqx_backend_influxdb_actions,
              start_resource,
              4},
            line => 417})
      end,
      error({{Type, ResId}, create_failed})
  end.

-spec on_get_resource_status(ResId :: binary(),
    Params :: map()) -> Status :: map().

on_get_resource_status(ResId, #{<<"type">> := Type}) ->
  case Type of
    backend_influxdb_udp -> #{is_alive => true};
    backend_influxdb_http ->
      #{is_alive => test_resource_status(ResId, Type)}
  end.

on_resource_destroy(ResId,
    #{<<"pool">> := PoolName, <<"type">> := Type}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [Type, ResId]}
      end,
        mfa =>
        {emqx_backend_influxdb_actions,
          on_resource_destroy,
          2},
        line => 431})
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
              [Type, ResId]}
          end,
            mfa =>
            {emqx_backend_influxdb_actions,
              on_resource_destroy,
              2},
            line => 434})
      end;
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Destroy Resource ~p failed, ResId: ~p, ~p",
              [Type, ResId, Reason]}
          end,
            mfa =>
            {emqx_backend_influxdb_actions,
              on_resource_destroy,
              2},
            line => 436})
      end,
      error({{Type, ResId}, destroy_failed})
  end.

-spec on_action_create_data_to_influxdb(ActId ::
binary(),
    #{}) -> fun((Msg :: map()) -> any()).

on_action_create_data_to_influxdb(ActId,
    Opts = #{<<"pool">> := PoolName,
      <<"measurement">> := Measurement0,
      <<"tags">> := Tags0,
      <<"fields">> := Fields0,
      <<"timestamp">> := Timestamp0}) ->
  ParseFun = fun ([]) -> [];
    (Pairs) ->
      maps:fold(fun (Key, ValueOrPlaceholder, Acc) ->
        [{Key,
          parse_value_or_placeholder(ValueOrPlaceholder)}
          | Acc]
                end,
        [],
        Pairs)
             end,
  Measurement = parse_value_or_placeholder(Measurement0),
  Tags = ParseFun(Tags0),
  Fields = ParseFun(Fields0),
  Timestamp = parse_value_or_placeholder(Timestamp0),
  {[{'Measurement0', Measurement0},
    {'Tags0', Tags0},
    {'Fields0', Fields0},
    {'Timestamp0', Timestamp0},
    {'Opts', Opts},
    {'PoolName', PoolName},
    {'ActId', ActId},
    {'ParseFun', ParseFun},
    {'Measurement', Measurement},
    {'Tags', Tags},
    {'Fields', Fields},
    {'Timestamp', Timestamp}],
    Opts}.

on_action_data_to_influxdb(Msg,
    _Envs = #{'__bindings__' :=
    #{'ActId' := ActId,
      'PoolName' := PoolName,
      'Tags' := Tags, 'Fields' := Fields,
      'Timestamp' := Timestamp,
      'Measurement' := Measurement}}) ->
  ToGetPoint = to_get_point(Measurement,
    Tags,
    Fields,
    Timestamp),
  ToGetMeasurement = fun (Data) ->
    get_var(Measurement, Data)
                     end,
  influxdb_write(PoolName,
    ActId,
    ToGetMeasurement(Msg),
    [ToGetPoint(Msg)]).

connect(Options) -> influxdb:start_link(Options).

to_get_point(Measurement, Tags, Fields, Timestamp) ->
  fun (Data) ->
    Point = #{measurement => get_var(Measurement, Data),
      tags =>
      case lists:foldl(fun ({Key, ValueOrPlaceholder},
          AccIn) ->
        maps:put(Key,
          format_tag_value(get_var(ValueOrPlaceholder,
            Data)),
          AccIn)
                       end,
        #{},
        Tags)
      of
        Empty when map_size(Empty) =:= 0 -> undefined;
        NTags -> NTags
      end,
      fields =>
      case lists:foldl(fun ({Key, ValueOrPlaceholder},
          AccIn) ->
        maps:put(Key,
          get_var(ValueOrPlaceholder,
            Data),
          AccIn)
                       end,
        #{},
        Fields)
      of
        Empty when map_size(Empty) =:= 0 -> undefined;
        NFields -> NFields
      end,
      timestamp => get_var(Timestamp, Data)},
    maps:filter(fun (_K, V) -> V =/= undefined end, Point)
  end.

parse_value_or_placeholder(ValueOrPlaceholder) ->
  case re:run(ValueOrPlaceholder, "^\\${.+}$") of
    nomatch -> ValueOrPlaceholder;
    {match, [{0, Length}]} ->
      Placeholder = binary:part(ValueOrPlaceholder,
        2,
        Length - 3),
      {from, Placeholder}
  end.

get_var(<<>>, _Data) -> undefined;
get_var({from, Placeholder}, Data) ->
  emqx_rule_maps:nested_get({var, Placeholder},
    Data,
    undefined);
get_var(Value, _Data) -> Value.

format_tag_value(Value) when is_integer(Value) ->
  erlang:integer_to_binary(Value);
format_tag_value(Value) when is_float(Value) ->
  erlang:float_to_binary(Value,
    [compact, {decimals, 12}]);
format_tag_value(Value) -> Value.

influxdb_write(PoolName, ActId, Measurement, Points) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "InfluxDB Pool: ~p, Points: ~p",
          [PoolName, Points]}
      end,
        mfa =>
        {emqx_backend_influxdb_actions, influxdb_write, 4},
        line => 532})
  end,
  case ecpool:with_client(PoolName,
    Measurement,
    fun (C) -> influxdb:write(C, Points) end)
  of
    ok -> emqx_rule_metrics:inc_actions_success(ActId);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Write to influxdb failed, Action Id: "
              "~p, reason: ~p",
              [ActId, Reason]}
          end,
            mfa =>
            {emqx_backend_influxdb_actions,
              influxdb_write,
              4},
            line => 538})
      end,
      emqx_rule_metrics:inc_actions_error(ActId),
      {badact, Reason}
  end.

pool_name(ResId) ->
  list_to_atom("influxdb:" ++ str(ResId)).

options(Type, ResId, Options) ->
  Get = fun (Key) -> maps:get(Key, Options, undefined)
        end,
  Filter = fun (Opts) ->
    [{K, V}
      || {K, V} <- Opts,
      V =/= undefined andalso V =/= <<>> andalso V =/= []]
           end,
  CommonOpts = Filter([{batch_size,
    Get(<<"batch_size">>)},
    {pool_size, Get(<<"pool_size">>)}]),
  case Type of
    backend_influxdb_udp ->
      UDPOpts = Filter([{host, to_string(Get(<<"host">>))},
        {port, Get(<<"port">>)}]),
      [{write_protocol, udp}, {udp_opts, UDPOpts}
        | CommonOpts];
    _ ->
      HTTPSEnabled = maps:get(<<"https_enabled">>,
        Options,
        false),
      HTTPOpts = Filter([{host, to_string(Get(<<"host">>))},
        {port, Get(<<"port">>)},
        {database, to_string(Get(<<"database">>))},
        {username, to_string(Get(<<"username">>))},
        {password, to_string(Get(<<"password">>))},
        {precision, Get(<<"precision">>)},
        {https_enabled, HTTPSEnabled}])
        ++
        case HTTPSEnabled of
          false -> [];
          true ->
            Version =
              binary_to_atom(Get(<<"tls_version">>),
                utf8),
            [{ssl,
                [{versions, [Version]},
                  {ciphers, ciphers(Version)}]
                ++
                emqx_rule_actions_utils:get_ssl_opts(Options,
                  ResId)}]
        end,
      [{write_protocol, http}, {http_opts, HTTPOpts}
        | CommonOpts]
  end.

ciphers(Version) ->
  ssl:cipher_suites(all, Version, openssl).

to_string(Bin) when is_binary(Bin) ->
  binary_to_list(Bin);
to_string(Str) when is_list(Str) -> Str.

test_resource_status(_ResId, backend_influxdb_udp) ->
  true;
test_resource_status(ResId, _) ->
  PoolName = pool_name(ResId),
  Workers = ecpool:workers(PoolName),
  length(Workers) > 0 andalso
    lists:all(fun ({_Name, Worker}) ->
      {ok, Pid} = ecpool_worker:client(Worker),
      influxdb:is_running(Pid)
              end,
      Workers).

logger_header() -> "".

