%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 下午7:01
%%%-------------------------------------------------------------------
-module(emqx_rule_actions_batcher).
-author("root").

-behaviour(gen_statem).
-behaviour(ecpool_worker).

-export([connect/1]).

-export([start_link/3,
  accumulate/2,
  accumulate/3,
  accumulate_async/2,
  get_cbst/1,
  exec/2,
  accumulate_mfa/3]).

-export([callback_mode/0, init/1]).

-export([do/3]).
-type data() :: term().
-type result() :: term().

-callback batcher_flush(Acc :: [{From :: pid(),
  data()}],
    CbSt :: term()) -> {{From :: pid(), result()},
  NewCbSt :: term()}.


callback_mode() -> [state_functions].

start_link(CbSt, CbMod, Opts) ->
  gen_statem:start_link(emqx_rule_actions_batcher,
    {CbSt, CbMod, Opts},
    []).

-spec accumulate(pid(), data()) -> ok.

accumulate(Pid, Data) ->
  gen_statem:call(Pid, {do_acc, Data}).

-spec accumulate(pid(), data(), timeout()) -> ok.

accumulate(Pid, Data, Timeout) ->
  gen_statem:call(Pid, {do_acc, Data}, Timeout).

-spec accumulate_async(pid(), data()) -> ok.

accumulate_async(Pid, Data) ->
  gen_statem:cast(Pid, {do_acc, Data}).

get_cbst(Pid) -> gen_statem:call(Pid, get_cbst).

exec(Pid, Fun) -> gen_statem:call(Pid, {exec, Fun}).

init({CbSt, CbMod, Opts}) ->
  BatchSize = maps:get(batch_size, Opts, 100),
  BatchTime = maps:get(batch_time, Opts, 10),
  Mode = maps:get(mode, Opts),
  St = #{batch_size => BatchSize, batch_time => BatchTime,
    cb_mod => CbMod, cb_st => CbSt, acc => [],
    acc_left => BatchSize, tref => undefined, mode => Mode},
  {ok, do, St}.

do(cast, {do_acc, Data}, State) ->
  do_acc(nil, Data, State);
do({call, From}, {do_acc, Data}, State) ->
  do_acc(From, Data, State);
do({call, From}, get_cbst, #{cb_st := CbSt}) ->
  {keep_state_and_data, [{reply, From, CbSt}]};
do({call, From}, {exec, Fun}, #{cb_st := CbSt}) ->
  {keep_state_and_data,
    [{reply,
      From,
      emqx_rule_actions_utils:safe_exec(Fun, [CbSt])}]};
do(info, {flush, Ref}, St = #{tref := {_TRef, Ref}}) ->
  {keep_state, flush(St#{tref := undefined})};
do(info, {flush, _Ref}, _St) -> keep_state_and_data.

flush(#{acc := []} = St) -> St;
flush(#{acc := Acc, batch_size := Size, cb_st := CbSt,
  cb_mod := CbMod, mode := Mode} =
  St) ->
  {Results, NewCbSt} = CbMod:batcher_flush(Acc, CbSt),
  Mode == <<"sync">> andalso reply(Results),
  cancel_flush_timer(St#{cb_st := NewCbSt,
    acc_left := Size, acc := []}).

reply(Results) ->
  lists:foreach(fun ({From, Result}) ->
    gen_statem:reply(From, Result)
                end,
    Results).

connect(Options) ->
  start_link(proplists:get_value(batch_state, Options),
    proplists:get_value(batch_cbmod, Options),
    proplists:get_value(batch_opts, Options, #{})).

do_acc(From, Data,
    #{acc := Acc, acc_left := Left} = St0) ->
  St = St0#{acc := [{From, Data} | Acc],
    acc_left := Left - 1},
  case Left =< 1 of
    true -> {keep_state, flush(St)};
    false -> {keep_state, ensure_flush_timer(St)}
  end.

ensure_flush_timer(St = #{tref := undefined,
  batch_time := T}) ->
  Ref = make_ref(),
  TRef = erlang:send_after(T, self(), {flush, Ref}),
  St#{tref => {TRef, Ref}};
ensure_flush_timer(St) -> St.

cancel_flush_timer(St = #{tref := undefined}) -> St;
cancel_flush_timer(St = #{tref := {TRef, _Ref}}) ->
  _ = erlang:cancel_timer(TRef),
  St#{tref => undefined}.

accumulate_mfa(InsertMode, Data, SyncTimeout) ->
  {emqx_rule_actions_batcher,
    accumulate_fun(InsertMode),
    accumulate_args(InsertMode, Data, SyncTimeout)}.

accumulate_fun(<<"sync">>) -> accumulate;
accumulate_fun(<<"async">>) -> accumulate_async.

accumulate_args(<<"sync">>, Data, SyncTimeout) ->
  [Data, SyncTimeout];
accumulate_args(<<"async">>, Data, _) -> [Data].

