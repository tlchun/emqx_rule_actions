%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午7:00
%%%-------------------------------------------------------------------
-module(emqx_bridge_kafka_actions).
-author("root").
-export([logger_header/0]).

-include("../include/emqx_bridge_kafka.hrl").
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/rule_actions.hrl").


-export([on_resource_create/2,
  on_resource_destroy/2,
  on_resource_status/2]).

-export([on_action_create_data_to_kafka/2,
  on_action_data_to_kafka/2,
  on_action_destroy_data_to_kafka/2]).

-export([wolff_callback/3]).

-resource_type(#{create => on_resource_create,
  description =>
  #{en => <<75, 97, 102, 107, 97>>,
    zh => <<75, 97, 102, 107, 97>>},
  destroy => on_resource_destroy, name => bridge_kafka,
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
    order => 11,
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
      order => 13,
      title =>
      #{en =>
      <<67, 101, 114, 116, 105, 102, 105, 99,
        97, 116, 101, 32, 70, 105, 108, 101>>,
        zh =>
        <<232, 175, 129, 228, 185, 166, 230, 150,
          135, 228, 187, 182>>},
      type => file},
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
        <<115, 110, 97, 112, 112, 121>>,
        <<103, 122, 105, 112>>],
      order => 7,
      title =>
      #{en =>
      <<67, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110>>,
        zh => <<229, 142, 139, 231, 188, 169>>},
      type => string},
    keyfile =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<83, 83, 76, 32, 231, 167, 129, 233,
          146, 165, 230, 150, 135, 228, 187,
          182>>},
      order => 12,
      title =>
      #{en =>
      <<75, 101, 121, 32, 70, 105, 108, 101>>,
        zh =>
        <<231, 167, 129, 233, 146, 165, 230, 150,
          135, 228, 187, 182>>},
      type => file},
    max_batch_bytes =>
    #{default => <<49, 48, 50, 52, 75, 66>>,
      description =>
      #{en =>
      <<77, 97, 120, 32, 66, 97, 116, 99, 104,
        32, 66, 121, 116, 101, 115>>,
        zh =>
        <<230, 156, 128, 229, 164, 167, 230, 137,
          185, 229, 164, 132, 231, 144, 134, 229,
          173, 151, 232, 138, 130, 230, 149,
          176>>},
      order => 6,
      title =>
      #{en =>
      <<77, 97, 120, 32, 66, 97, 116, 99, 104,
        32, 66, 121, 116, 101, 115>>,
        zh =>
        <<230, 156, 128, 229, 164, 167, 230, 137,
          185, 229, 164, 132, 231, 144, 134, 229,
          173, 151, 232, 138, 130, 230, 149,
          176>>},
      type => string},
    min_metadata_refresh_interval =>
    #{default => <<51, 115>>,
      description =>
      #{en =>
      <<77, 105, 110, 32, 77, 101, 116, 97,
        100, 97, 116, 97, 32, 82, 101, 102,
        114, 101, 115, 104, 32, 73, 110, 116,
        101, 114, 118, 97, 108>>,
        zh =>
        <<230, 156, 128, 229, 176, 143, 32, 77,
          101, 116, 97, 100, 97, 116, 97, 32,
          230, 155, 180, 230, 150, 176, 233, 151,
          180, 233, 154, 148>>},
      order => 2,
      title =>
      #{en =>
      <<77, 101, 116, 97, 100, 97, 116, 97, 32,
        82, 101, 102, 114, 101, 115, 104>>,
        zh =>
        <<77, 101, 116, 97, 100, 97, 116, 97, 32,
          230, 155, 180, 230, 150, 176, 233, 151,
          180, 233, 154, 148>>},
      type => string},
    password =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 65, 117, 116,
        104, 32, 80, 97, 115, 115, 119, 111,
        114, 100>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 232, 174,
          164, 232, 175, 129, 229, 175, 134, 231,
          160, 129>>},
      order => 4,
      title =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 80, 97, 115,
        115, 119, 111, 114, 100>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 229, 175,
          134, 231, 160, 129>>},
      type => password},
    query_api_versions =>
    #{default => true,
      description =>
      #{en =>
      <<81, 117, 101, 114, 121, 32, 65, 80, 73,
        32, 86, 101, 114, 115, 105, 111, 110,
        115>>,
        zh =>
        <<230, 152, 175, 229, 144, 166, 229, 188,
          128, 229, 144, 175, 230, 159, 165, 232,
          175, 162, 32, 65, 80, 73, 32, 231, 137,
          136, 230, 156, 172, 229, 143, 183>>},
      order => 9,
      title =>
      #{en =>
      <<81, 117, 101, 114, 121, 32, 65, 80, 73,
        32, 86, 101, 114, 115, 105, 111, 110,
        115>>,
        zh =>
        <<230, 159, 165, 232, 175, 162, 32, 65,
          80, 73, 32, 231, 137, 136, 230, 156,
          172, 229, 143, 183>>},
      type => boolean},
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
      order => 8,
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
    <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 57,
      48, 57, 50>>,
      description =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 83, 101, 114,
        118, 101, 114, 32, 65, 100, 100, 114,
        101, 115, 115, 44, 32, 77, 117, 108,
        116, 105, 112, 108, 101, 32, 110, 111,
        100, 101, 115, 32, 115, 101, 112, 97,
        114, 97, 116, 101, 100, 32, 98, 121,
        32, 99, 111, 109, 109, 97, 115>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 230, 156,
          141, 229, 138, 161, 229, 153, 168, 229,
          156, 176, 229, 157, 128, 44, 32, 229,
          164, 154, 232, 138, 130, 231, 130, 185,
          228, 189, 191, 231, 148, 168, 233, 128,
          151, 229, 143, 183, 229, 136, 134, 233,
          154, 148>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 83, 101, 114,
        118, 101, 114>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 230, 156,
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
      order => 10,
      title =>
      #{en =>
      <<69, 110, 97, 98, 108, 101, 32, 83, 83,
        76>>,
        zh =>
        <<229, 188, 128, 229, 144, 175, 32, 83,
          83, 76>>},
      type => boolean},
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
      order => 5,
      title =>
      #{en =>
      <<83, 121, 110, 99, 32, 84, 105, 109,
        101, 111, 117, 116>>,
        zh =>
        <<229, 144, 140, 230, 173, 165, 232, 176,
          131, 231, 148, 168, 232, 182, 133, 230,
          151, 182, 230, 151, 182, 233, 151,
          180>>},
      type => string},
    username =>
    #{default => <<>>,
      description =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 65, 117, 116,
        104, 32, 85, 115, 101, 114, 110, 97,
        109, 101>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 232, 174,
          164, 232, 175, 129, 231, 148, 168, 230,
          136, 183, 229, 144, 141>>},
      order => 3,
      title =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 85, 115, 101,
        114, 110, 97, 109, 101>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 231, 148,
          168, 230, 136, 183, 229, 144, 141>>},
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
        <<230, 152, 175, 229, 144, 166, 230, 160,
          161, 233, 170, 140, 230, 156, 141, 229,
          138, 161, 229, 153, 168, 232, 175, 129,
          228, 185, 166>>},
      type => boolean}},
  status => on_resource_status,
  title =>
  #{en => <<75, 97, 102, 107, 97>>,
    zh => <<75, 97, 102, 107, 97>>}}).

-rule_action(#{category => data_forward,
  create => on_action_create_data_to_kafka,
  description =>
  #{en =>
  <<83, 116, 111, 114, 101, 32, 68, 97, 116, 97, 32, 116,
    111, 32, 75, 97, 102, 107, 97>>,
    zh =>
    <<230, 161, 165, 230, 142, 165, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 75, 97, 102, 107, 97>>},
  destroy => on_action_destroy_data_to_kafka,
  for => '$any', name => data_to_kafka,
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
    cache_mode =>
    #{default => <<77, 101, 109, 111, 114, 121>>,
      description =>
      #{en =>
      <<67, 97, 99, 104, 101, 32, 77, 111, 100,
        101>>,
        zh =>
        <<231, 188, 147, 229, 173, 152, 230, 168,
          161, 229, 188, 143>>},
      enum =>
      [<<77, 101, 109, 111, 114, 121>>,
        <<68, 105, 115, 107>>,
        <<77, 101, 109, 111, 114, 121, 43, 68, 105, 115,
          107>>],
      order => 6,
      title =>
      #{en =>
      <<67, 97, 99, 104, 101, 32, 77, 111, 100,
        101>>,
        zh =>
        <<231, 188, 147, 229, 173, 152, 230, 168,
          161, 229, 188, 143>>},
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
      order => 5,
      title =>
      #{en =>
      <<83, 116, 114, 97, 116, 101, 103, 121, 32,
        75, 101, 121>>,
        zh =>
        <<83, 116, 114, 97, 116, 101, 103, 121, 32,
          75, 101, 121>>},
      type => string},
    max_total_bytes =>
    #{default => <<50, 71, 66>>,
      description =>
      #{en =>
      <<77, 97, 120, 32, 67, 97, 99, 104, 101,
        32, 66, 121, 116, 101, 115>>,
        zh =>
        <<231, 188, 147, 229, 173, 152, 231, 154,
          132, 230, 156, 128, 229, 164, 167, 229,
          173, 151, 232, 138, 130, 230, 149, 176,
          227, 128, 130, 229, 189, 147, 231, 188,
          147, 229, 173, 152, 230, 149, 176, 230,
          141, 174, 232, 182, 133, 229, 135, 186,
          229, 144, 142, 239, 188, 140, 228, 188,
          154, 228, 184, 162, 229, 188, 131, 233,
          152, 159, 229, 164, 180, 231, 154, 132,
          230, 149, 176, 230, 141, 174, 239, 188,
          140, 230, 150, 176, 231, 154, 132, 230,
          149, 176, 230, 141, 174, 230, 143, 146,
          229, 133, 165, 233, 152, 159, 229, 176,
          190, 227, 128, 130>>},
      order => 7,
      title =>
      #{en =>
      <<77, 97, 120, 32, 67, 97, 99, 104, 101,
        32, 66, 121, 116, 101, 115>>,
        zh =>
        <<231, 188, 147, 229, 173, 152, 231, 154,
          132, 230, 156, 128, 229, 164, 167, 229,
          173, 151, 232, 138, 130, 230, 149,
          176>>},
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
      input => textarea, order => 9, required => false,
      title =>
      #{en =>
      <<80, 97, 121, 108, 111, 97, 100, 32, 84,
        101, 109, 112, 108, 97, 116, 101>>,
        zh =>
        <<230, 182, 136, 230, 129, 175, 229, 134,
          133, 229, 174, 185, 230, 168, 161, 230,
          157, 191>>},
      type => string},
    required_acks =>
    #{default => <<97, 108, 108, 95, 105, 115, 114>>,
      description =>
      #{en =>
      <<80, 114, 111, 100, 117, 99, 101, 32, 82,
        101, 113, 117, 105, 114, 101, 100, 32,
        65, 99, 107, 115>>,
        zh =>
        <<80, 114, 111, 100, 117, 99, 101, 32, 65,
          67, 75, 231, 173, 150, 231, 149, 165>>},
      enum =>
      [<<97, 108, 108, 95, 105, 115, 114>>,
        <<108, 101, 97, 100, 101, 114, 95, 111, 110,
          108, 121>>,
        <<110, 111, 110, 101>>],
      order => 4,
      title =>
      #{en =>
      <<80, 114, 111, 100, 117, 99, 101, 32, 82,
        101, 113, 117, 105, 114, 101, 100, 32,
        65, 99, 107, 115>>,
        zh =>
        <<80, 114, 111, 100, 117, 99, 101, 32, 65,
          67, 75, 231, 173, 150, 231, 149, 165>>},
      type => string},
    segments_bytes =>
    #{default => <<49, 48, 48, 77, 66>>,
      description =>
      #{en =>
      <<109, 101, 109, 111, 114, 121, 43, 100,
        105, 115, 107, 32, 109, 111, 100, 101,
        44, 32, 109, 101, 109, 111, 114, 121, 32,
        66, 121, 116, 101, 115, 46, 32, 82, 101,
        99, 111, 109, 109, 101, 110, 100, 101,
        100, 32, 109, 97, 120, 105, 109, 117,
        109, 32, 99, 111, 110, 102, 105, 103,
        117, 114, 97, 116, 105, 111, 110, 32,
        105, 115, 32, 49, 71, 66>>,
        zh =>
        <<109, 101, 109, 111, 114, 121, 43, 100,
          105, 115, 107, 230, 168, 161, 229, 188,
          143, 228, 184, 139, 239, 188, 140, 229,
          134, 133, 229, 173, 152, 231, 137, 135,
          229, 164, 167, 229, 176, 143, 227, 128,
          130, 229, 187, 186, 232, 174, 174, 230,
          156, 128, 229, 164, 167, 233, 133, 141,
          231, 189, 174, 229, 156, 168, 49, 71,
          66>>},
      order => 8,
      title =>
      #{en =>
      <<83, 101, 103, 109, 101, 110, 116, 115,
        32, 66, 121, 116, 101, 115>>,
        zh =>
        <<83, 101, 103, 109, 101, 110, 116, 115,
          32, 66, 121, 116, 101, 115>>},
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
    <<75, 97, 102, 107, 97, 32, 84, 111, 112,
      105, 99>>,
      zh =>
      <<75, 97, 102, 107, 97, 32, 228, 184, 187,
        233, 162, 152>>},
      order => 1, required => true,
      title =>
      #{en =>
      <<75, 97, 102, 107, 97, 32, 84, 111, 112,
        105, 99>>,
        zh =>
        <<75, 97, 102, 107, 97, 32, 228, 184, 187,
          233, 162, 152>>},
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
    116, 111, 32, 75, 97, 102, 107, 97>>,
    zh =>
    <<230, 161, 165, 230, 142, 165, 230, 149, 176, 230,
      141, 174, 229, 136, 176, 32, 75, 97, 102, 107, 97>>},
  types => [bridge_kafka]}).

-vsn("4.2.5").

on_resource_create(ResId,
    Config = #{<<"servers">> := Servers}) ->
  {ok, _} = application:ensure_all_started(wolff),
  Interval =
    cuttlefish_duration:parse(str(maps:get(<<"min_metadata_refresh_interval">>,
      Config))),
  Timeout =
    cuttlefish_duration:parse(str(maps:get(<<"sync_timeout">>,
      Config))),
  MaxBatchBytes =
    cuttlefish_bytesize:parse(str(maps:get(<<"max_batch_bytes">>,
      Config))),
  ConnStrategy = atom(maps:get(<<"connection_strategy">>,
    Config,
    <<"per_partition">>)),
  Compression = atom(maps:get(<<"compression">>, Config)),
  Sndbuf =
    cuttlefish_bytesize:parse(str(maps:get(<<"send_buffer">>,
      Config))),
  ClientId = client_id(ResId),
  Servers1 = format_servers(Servers),
  ClientCfg = #{extra_sock_opts => [{sndbuf, Sndbuf}],
    connection_strategy => ConnStrategy,
    min_metadata_refresh_interval => Interval},
  ClientCfg1 = case
                 atom(maps:get(<<"query_api_versions">>, Config, true))
               of
                 true -> ClientCfg;
                 false -> ClientCfg#{query_api_versions => false}
               end,
  ClientCfg2 = case maps:get(<<"username">>, Config, <<>>)
               of
                 <<>> -> ClientCfg1;
                 Username ->
                   ClientCfg1#{sasl =>
                   {plain,
                     Username,
                     maps:get(<<"password">>,
                       Config,
                       <<>>)}}
               end,
  ClientCfg3 = maybe_append_ssl_options(ClientCfg2,
    Config,
    ResId),
  start_resource(ClientId, Servers1, ClientCfg3),
  #{<<"client_id">> => ClientId, <<"timeout">> => Timeout,
    <<"max_batch_bytes">> => MaxBatchBytes,
    <<"compression">> => Compression,
    <<"servers">> => Servers1,
    <<"client_cfg">> => ClientCfg3}.

start_resource(ClientId, Servers, ClientCfg) ->
  case ensure_server_list(Servers, ClientCfg) of
    ok ->
      {ok, _Pid} = wolff:ensure_supervised_client(ClientId,
        Servers,
        ClientCfg);
    {error, _} -> error(connect_kafka_server_fail)
  end.

on_resource_destroy(ResId,
    #{<<"client_id">> := ClientId}) ->
  begin
    logger:log(info,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Destroying Resource ~p, ResId: ~p",
          [bridge_kafka, ResId]}
      end,
        mfa =>
        {emqx_bridge_kafka_actions, on_resource_destroy, 2},
        line => 313})
  end,
  ok = wolff:stop_and_delete_supervised_client(ClientId).

on_resource_status(_ResId,
    #{<<"servers">> := Servers,
      <<"query_api_versions">> := QueryApiVersions}) ->
  case ensure_server_list(Servers, QueryApiVersions) of
    ok -> #{is_alive => true};
    {error, _} -> #{is_alive => false}
  end;
on_resource_status(_ResId,
    #{<<"servers">> := Servers,
      <<"client_cfg">> := ClientCfg}) ->
  case ensure_server_list(Servers, ClientCfg) of
    ok -> #{is_alive => true};
    {error, _} -> #{is_alive => false}
  end.

on_action_create_data_to_kafka(ActId,
    Params = #{<<"client_id">> := ClientId,
      <<"strategy">> := Strategy,
      <<"type">> := Type,
      <<"topic">> := Topic,
      <<"timeout">> := Timeout,
      <<"payload_tmpl">> := PayloadTmpl}) ->
  MaxTotalBytes =
    cuttlefish_bytesize:parse(str(maps:get(<<"max_total_bytes">>,
      Params,
      <<"2GB">>))),
  ReplayqCfg = case maps:get(<<"disk_cache">>,
    Params,
    undefined)
               of
                 undefined ->
                   case maps:get(<<"cache_mode">>, Params, <<"Memory">>)
                   of
                     <<"Memory">> ->
                       [{replayq_dir, false},
                         {replayq_max_total_bytes, MaxTotalBytes}];
                     <<"Disk">> ->
                       ReplayqDir =
                         filename:join([emqx:get_env(data_dir),
                           replayq,
                           node(),
                           ActId]),
                       [{replayq_dir, ReplayqDir},
                         {replayq_max_total_bytes, MaxTotalBytes}];
                     <<"Memory+Disk">> ->
                       ReplayqDir =
                         filename:join([emqx:get_env(data_dir),
                           replayq,
                           node(),
                           ActId]),
                       [{replayq_dir, ReplayqDir},
                         {replayq_offload_mode, true},
                         {replayq_seg_bytes,
                           cuttlefish_bytesize:parse(str(maps:get(<<"segments_bytes">>,
                             Params,
                             <<"100MB">>)))},
                         {replayq_max_total_bytes, MaxTotalBytes}];
                     _ -> [{replayq_dir, false}]
                   end;
                 <<"on">> ->
                   RamOffload = maps:get(<<"ram_offload">>,
                     Params,
                     <<"off">>),
                   ReplayqDir = filename:join([emqx:get_env(data_dir),
                     replayq,
                     node(),
                     ActId]),
                   [{replayq_dir, ReplayqDir},
                     {replayq_offload_mode, RamOffload =:= <<"on">>},
                     {replayq_max_total_bytes, MaxTotalBytes}];
                 <<"off">> ->
                   [{replayq_dir, false},
                     {replayq_max_total_bytes, MaxTotalBytes}]
               end,
  PayloadTks = emqx_rule_utils:preproc_tmpl(PayloadTmpl),
  Key = maps:get(<<"key">>, Params),
  ProducerCfg = [{partitioner, atom(Strategy)},
    {name, binary_to_atom(ActId, utf8)},
    {required_acks,
      binary_to_atom(maps:get(<<"required_acks">>,
        Params,
        <<"all_isr">>),
        utf8)},
    {max_batch_bytes,
      maps:get(<<"max_batch_bytes">>, Params, 1048576)},
    {compression,
      maps:get(<<"compression">>, Params, no_compression)}]
    ++ ReplayqCfg,
  ok = check_kafka_topic(ClientId, Topic),
  {ok, Producers} =
    wolff:ensure_supervised_producers(ClientId,
      Topic,
      maps:from_list(ProducerCfg)),
  {[{'Params', Params},
    {'Topic', Topic},
    {'ClientId', ClientId},
    {'Timeout', Timeout},
    {'Type', Type},
    {'Strategy', Strategy},
    {'PayloadTmpl', PayloadTmpl},
    {'ActId', ActId},
    {'MaxTotalBytes', MaxTotalBytes},
    {'ReplayqCfg', ReplayqCfg},
    {'PayloadTks', PayloadTks},
    {'Key', Key},
    {'ProducerCfg', ProducerCfg},
    {'Producers', Producers}],
    Params#{<<"producers">> => Producers}}.

on_action_data_to_kafka(Msg,
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

on_action_destroy_data_to_kafka(_Id,
    #{<<"producers">> := Producers}) ->
  ok =
    wolff:stop_and_delete_supervised_producers(Producers).

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

format_data([], Msg) -> emqx_json:encode(Msg);
format_data(Tokens, Msg) ->
  emqx_rule_utils:proc_tmpl(Tokens, Msg).

produce(ActId, Producers, Key, JsonMsg, Type,
    Timeout) ->
  begin
    logger:log(debug,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "Produce key:~p, payload:~p",
          [Key, JsonMsg]}
      end,
        mfa => {emqx_bridge_kafka_actions, produce, 6},
        line => 404})
  end,
  case Type of
    <<"sync">> ->
      {Partition, BaseOffset} = wolff:send_sync(Producers,
        [#{key => Key,
          value => JsonMsg}],
        Timeout),
      wolff_callback(Partition, BaseOffset, ActId);
    <<"async">> ->
      wolff:send(Producers,
        [#{key => Key, value => JsonMsg}],
        {fun wolff_callback/3, [ActId]})
  end.

wolff_callback(_Partition, BaseOffset, ActId)
  when is_integer(BaseOffset) ->
  emqx_rule_metrics:inc_actions_success(ActId);
wolff_callback(_Partition, _Failed, ActId) ->
  emqx_rule_metrics:inc_actions_error(ActId).

format_servers(Servers) when is_binary(Servers) ->
  format_servers(str(Servers));
format_servers(Servers) ->
  ServerList = string:tokens(Servers, ", "),
  lists:map(fun (Server) ->
    case string:tokens(Server, ":") of
      [Domain] -> {Domain, 9092};
      [Domain, Port] -> {Domain, list_to_integer(Port)}
    end
            end,
    ServerList).

client_id(ResId) ->
  binary:replace(list_to_binary("bridge_kafka_" ++
  str(ResId)),
    <<":">>,
    <<"_">>,
    [global]).

str(List) when is_list(List) -> List;
str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(Atom) when is_atom(Atom) -> atom_to_list(Atom).

atom(B) when is_binary(B) ->
  erlang:binary_to_atom(B, utf8);
atom(L) when is_list(L) -> erlang:list_to_atom(L);
atom(A) -> A.

check_kafka_topic(ClientId, Topic) ->
  case wolff_client_sup:find_client(ClientId) of
    {ok, ClientPid} ->
      case wolff_client:get_leader_connections(ClientPid,
        Topic)
      of
        {ok, _Conns} -> ok;
        {error, _Reason} ->
          erlang:error({error, kafka_topic_not_found})
      end;
    {error, Restarting} -> erlang:error({error, Restarting})
  end.

ensure_server_list(Servers, ClientCfg) ->
  ensure_server_list(Servers, ClientCfg, []).

ensure_server_list([], _ClientCfg, Errors) ->
  {error, Errors};
ensure_server_list([Host | Rest], ClientCfg, Errors) ->
  ClientCfg1 = case is_boolean(ClientCfg) of
                 true ->
                   case ClientCfg of
                     true -> #{};
                     false -> #{query_api_versions => false}
                   end;
                 false -> ClientCfg
               end,
  case kpro:connect(Host, ClientCfg1) of
    {ok, Pid} ->
      _ = spawn(fun () -> do_close_connection(Pid) end),
      ok;
    {error, Reason} ->
      ensure_server_list(Rest,
        ClientCfg1,
        [{Host, Reason} | Errors])
  end.

do_close_connection(Pid) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, stop}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {connection_down, Reason}}
  after 5000 -> exit(Pid, kill)
  end.

maybe_append_ssl_options(ClientCfg, Config, ResId) ->
  case maps:get(<<"ssl">>, Config, false) of
    true ->
      ClientCfg#{ssl =>
      emqx_rule_actions_utils:get_ssl_opts(Config, ResId)};
    _ -> ClientCfg
  end.

logger_header() -> "".

