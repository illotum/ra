%% This Source Code Form is subject to the terms of the Mozilla Public
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_voter_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("src/ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PROCESS_COMMAND_TIMEOUT, 6000).
-define(SYS, default).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

all_tests() ->
    [
     maybe_join
    ].

suite() -> [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    ok = logger:set_primary_config(level, warning),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

restart_ra(DataDir) ->
    ok = application:set_env(ra, segment_max_entries, 128),
    {ok, _} = ra:start_in(DataDir),
    ok.

init_per_group(_G, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join([PrivDir, "data"]),
    ok = restart_ra(DataDir),
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    [{test_name, ra_lib:to_list(TestCase)} | Config].

end_per_testcase(_TestCase, Config) ->
    ra_server_sup_sup:remove_all(default),
    Config.

%%% Tests

maybe_join(Config) ->
    % form a cluster
    [N1, N2] = start_local_cluster(2, ?config(test_name, Config), add_machine()),
    _ = issue_op(N1, 5),
    validate_state_on_node(N1, 5),
    % add maybe member
    N3 = nth_server_name(Config, 3),
    ok = start_and_maybe_join(N1, N3),
    _ = issue_op(N3, 5),
    validate_state_on_node(N3, 10),
    % validate all are voters after catch-up
    All = [N1, N2, N3],
    lists:map(fun(O) ->
                ?assertEqual(All, voters(O)),
                ?assertEqual([], nonvoters(O))
              end, overviews(N1)),
    terminate_cluster(All).

%%% Helpers

nth_server_name(Config, N) when is_integer(N) ->
    {ra_server:name(?config(test_name, Config), erlang:integer_to_list(N)), node()}.

add_machine() ->
    {module, ?MODULE, #{}}.

terminate_cluster(Nodes) ->
    [ra:stop_server(?SYS, P) || P <- Nodes].

new_server(Name, Config) ->
    ClusterName = ?config(test_name, Config),
    ok = ra:start_server(default, ClusterName, Name, add_machine(), []),
    ok.

start_and_join({ClusterName, _} = ServerRef, {_, _} = New) ->
    {ok, _, _} = ra:add_member(ServerRef, New),
    ok = ra:start_server(default, ClusterName, New, add_machine(), [ServerRef]),
    ok.

start_and_maybe_join({ClusterName, _} = ServerRef, {_, _} = New) ->
    {ok, _, _} = ra:maybe_add_member(ServerRef, New),
    ok = ra:start_server(default, ClusterName, New, add_machine(), [ServerRef]),
    ok.

start_local_cluster(Num, ClusterName, Machine) ->
    Nodes = [{ra_server:name(ClusterName, integer_to_list(N)), node()}
             || N <- lists:seq(1, Num)],

    {ok, _, Failed} = ra:start_cluster(default, ClusterName, Machine, Nodes),
    ?assert(length(Failed) == 0),
    Nodes.

remove_member(Name) ->
    {ok, _IdxTerm, _Leader} = ra:remove_member(Name, Name),
    ok.

validate_state_on_node(Name, Expected) ->
    {ok, Expected, _} = ra:consistent_query(Name, fun(X) -> X end).

dump(T) ->
    ct:pal("DUMP: ~p", [T]),
    T.

issue_op(Name, Op) ->
    {ok, _, Leader} = ra:process_command(Name, Op, ?PROCESS_COMMAND_TIMEOUT),
    Leader.

overviews(Node) ->
    {ok, Members, _From} = ra:members(Node),
    [ ra:member_overview(P) || {_, _} = P <- Members ].

voters({ok, #{cluster := Peers}, _} = _Overview) ->
    [ Id || {Id, Status} <- maps:to_list(Peers), maps:get(voter, Status) =:= yes ].

nonvoters({ok, #{cluster := Peers}, _} = _Overview) ->
    [ Id || {Id, Status} <- maps:to_list(Peers), maps:get(voter, Status) /= yes ].


%% machine impl
init(_) -> 0.
apply(_Meta, Num, State) ->
    {Num + State, Num + State}.
