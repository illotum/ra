%% This Source Code Form is subject to the terms of the Mozilla Public
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_voter_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("src/ra.hrl").
-include("src/ra_server.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        leader_server_maybe_join
    ].

-define(MACFUN, fun (E, _) -> E end).
-define(N1, {n1, node()}).
-define(N2, {n2, node()}).
-define(N3, {n3, node()}).
-define(N4, {n4, node()}).
-define(N5, {n5, node()}).

groups() ->
    [ {tests, [], all()} ].

init_per_suite(Config) ->
    ok = logger:set_primary_config(level, all),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    ok = logger:set_primary_config(level, all),
    ok = setup_log(),
    [{test_case, TestCase} | Config].

end_per_testcase(_TestCase, Config) ->
    meck:unload(),
    Config.

setup_log() ->
    ok = meck:new(ra_log, []),
    ok = meck:new(ra_snapshot, [passthrough]),
    ok = meck:new(ra_machine, [passthrough]),
    meck:expect(ra_log, init, fun(C) -> ra_log_memory:init(C) end),
    meck:expect(ra_log_meta, store, fun (_, U, K, V) ->
                                            put({U, K}, V), ok
                                    end),
    meck:expect(ra_log_meta, store_sync, fun (_, U, K, V) ->
                                                 put({U, K}, V), ok
                                         end),
    meck:expect(ra_log_meta, fetch, fun(_, U, K) ->
                                            get({U, K})
                                    end),
    meck:expect(ra_log_meta, fetch, fun (_, U, K, D) ->
                                            ra_lib:default(get({U, K}), D)
                                    end),
    meck:expect(ra_snapshot, begin_accept,
                fun(_Meta, SS) ->
                        {ok, SS}
                end),
    meck:expect(ra_snapshot, accept_chunk,
                fun(_Data, _OutOf, _Flag, SS) ->
                        {ok, SS}
                end),
    meck:expect(ra_snapshot, abort_accept, fun(SS) -> SS end),
    meck:expect(ra_snapshot, accepting, fun(_SS) -> undefined end),
    meck:expect(ra_log, snapshot_state, fun (_) -> snap_state end),
    meck:expect(ra_log, set_snapshot_state, fun (_, Log) -> Log end),
    meck:expect(ra_log, install_snapshot, fun (_, _, Log) -> {Log, []} end),
    meck:expect(ra_log, recover_snapshot, fun ra_log_memory:recover_snapshot/1),
    meck:expect(ra_log, snapshot_index_term, fun ra_log_memory:snapshot_index_term/1),
    meck:expect(ra_log, fold, fun ra_log_memory:fold/5),
    meck:expect(ra_log, release_resources, fun ra_log_memory:release_resources/3),
    meck:expect(ra_log, append_sync,
                fun({Idx, Term, _} = E, L) ->
                        L1 = ra_log_memory:append(E, L),
                        {LX, _} = ra_log_memory:handle_event({written, {Idx, Idx, Term}}, L1),
                        LX
                end),
    meck:expect(ra_log, write_config, fun ra_log_memory:write_config/2),
    meck:expect(ra_log, next_index, fun ra_log_memory:next_index/1),
    meck:expect(ra_log, append, fun ra_log_memory:append/2),
    meck:expect(ra_log, write, fun ra_log_memory:write/2),
    meck:expect(ra_log, handle_event, fun ra_log_memory:handle_event/2),
    meck:expect(ra_log, last_written, fun ra_log_memory:last_written/1),
    meck:expect(ra_log, last_index_term, fun ra_log_memory:last_index_term/1),
    meck:expect(ra_log, set_last_index, fun ra_log_memory:set_last_index/2),
    meck:expect(ra_log, fetch_term, fun ra_log_memory:fetch_term/2),
    meck:expect(ra_log, needs_cache_flush, fun (_) -> false end),
    meck:expect(ra_log, exists,
                fun ({Idx, Term}, L) ->
                        case ra_log_memory:fetch_term(Idx, L) of
                            {Term, Log} -> {true, Log};
                            {_, Log} -> {false, Log}
                        end
                end),
    meck:expect(ra_log, update_release_cursor,
                fun ra_log_memory:update_release_cursor/5),
    ok.

leader_server_maybe_join(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3})},
    State0 = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    % raft servers should switch to the new configuration after log append
    % and further cluster changes should be disallowed
    {leader, #{cluster := #{N1 := _, N2 := _, N3 := _, N4 := _},
               cluster_change_permitted := false} = _State1, Effects} =
        ra_server:handle_leader({command, {'$ra_maybe_join', meta(),
                                           N4, await_consensus}}, State0),
    % new member should join as non-voter
    {no, #{round := Round, target := Target}} = ra_voter:new_nonvoter(State0),
    [
     {send_rpc, N4,
      #append_entries_rpc{entries =
                          [_, _, _, {4, 5, {'$ra_cluster_change', _,
                                            #{N1 := _, N2 := _,
                                              N3 := _, N4 := #{voter := {no, #{round := Round,
                                                                                  target := Target,
                                                                                  ts := _}}}},
                                            await_consensus}}]}},
     {send_rpc, N3,
      #append_entries_rpc{entries =
                          [{4, 5, {'$ra_cluster_change', _,
                                   #{N1 := _, N2 := _, N3 := _, N4 := #{voter := {no, #{round := Round,
                                                                                  target := Target,
                                                                                  ts := _}}}},
                                   await_consensus}}],
                          term = 5, leader_id = N1,
                          prev_log_index = 3,
                          prev_log_term = 5,
                          leader_commit = 3}},
     {send_rpc, N2,
      #append_entries_rpc{entries =
                          [{4, 5, {'$ra_cluster_change', _,
                                   #{N1 := _, N2 := _, N3 := _, N4 := #{voter := {no, #{round := Round,
                                                                                  target := Target,
                                                                                  ts := _}}}},
                                   await_consensus}}],
                          term = 5, leader_id = N1,
                          prev_log_index = 3,
                          prev_log_term = 5,
                          leader_commit = 3}}
     | _] = Effects,
    ok.


% %%% helpers

ra_server_init(Conf) ->
    ra_server:recover(ra_server:init(Conf)).

init_servers(ServerIds, Machine) ->
    lists:foldl(fun (ServerId, Acc) ->
                        Args = #{cluster_name => some_id,
                                 id => ServerId,
                                 uid => atom_to_binary(element(1, ServerId), utf8),
                                 initial_members => ServerIds,
                                 log_init_args => #{uid => <<>>},
                                 machine => Machine},
                        Acc#{ServerId => {follower, ra_server_init(Args), []}}
                end, #{}, ServerIds).

list(L) when is_list(L) -> L;
list(L) -> [L].

entry(Idx, Term, Data) ->
    {Idx, Term, {'$usr', meta(), Data, after_log_append}}.

empty_state(NumServers, Id) ->
    Servers = lists:foldl(fun(N, Acc) ->
                                [{list_to_atom("n" ++ integer_to_list(N)), node()}
                                 | Acc]
                        end, [], lists:seq(1, NumServers)),
    ra_server_init(#{cluster_name => someid,
                     id => {Id, node()},
                     uid => atom_to_binary(Id, utf8),
                     initial_members => Servers,
                     log_init_args => #{uid => <<>>},
                     machine => {simple, fun (E, _) -> E end, <<>>}}). % just keep last applied value

base_state(NumServers, MacMod) ->
    Log0 = lists:foldl(fun(E, L) ->
                               ra_log:append(E, L)
                       end, ra_log:init(#{system_config => ra_system:default_config(),
                                          uid => <<>>}),
                       [{1, 1, usr(<<"hi1">>)},
                       {2, 3, usr(<<"hi2">>)},
                        {3, 5, usr(<<"hi3">>)}]),
    {Log, _} = ra_log:handle_event({written, {1, 3, 5}}, Log0),

    Servers = lists:foldl(fun(N, Acc) ->
                                Name = {list_to_atom("n" ++ integer_to_list(N)), node()},
                                Acc#{Name =>
                                     new_peer_with(#{next_index => 4,
                                                     match_index => 3})}
                        end, #{}, lists:seq(1, NumServers)),
    mock_machine(MacMod),
    Cfg = #cfg{id = ?N1,
               uid = <<"n1">>,
               log_id = <<"n1">>,
               metrics_key = n1,
               machine = {machine, MacMod, #{}}, % just keep last applied value
               machine_version = 0,
               machine_versions = [{0, 0}],
               effective_machine_version = 0,
               effective_machine_module = MacMod,
               system_config = ra_system:default_config()
              },
    #{cfg => Cfg,
      leader_id => ?N1,
      cluster => Servers,
      cluster_index_term => {0, 0},
      cluster_change_permitted => true,
      machine_state => <<"hi3">>, % last entry has been applied
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      log => Log,
      query_index => 0,
      queries_waiting_heartbeats => queue:new(),
      pending_consistent_queries => []}.

mock_machine(Mod) ->
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    %% just keep the latest command as the state
    meck:expect(Mod, apply, fun (_, Cmd, _) -> {Cmd, ok} end),
    ok.

usr_cmd(Data) ->
    {command, usr(Data)}.

usr(Data) ->
    {'$usr', meta(), Data, after_log_append}.

meta() ->
    #{from => {self(), make_ref()},
      ts => os:system_time(millisecond)}.

dump(T) ->
    ct:pal("DUMP: ~p", [T]),
    T.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      query_index => 0,
      commit_index_sent => 0,
      status => normal,
      voter => yes}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

snap_meta(Idx, Term) ->
    snap_meta(Idx, Term, []).

snap_meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 0}.

