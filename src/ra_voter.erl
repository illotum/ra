%% This Source Code Form is subject to the terms of the Mozilla Public
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_voter).

-export([
         new_nonvoter/1,
         status/1,
         status/2,
         maybe_promote/3
        ]).

-define(DEFAULT_MAX_ROUNDS, 4).

new_nonvoter(State) ->
    Target = maps:get(commit_index, State),
    {no, #{round => 0, target => Target , ts => os:system_time(millisecond)}}.

maybe_promote(PeerID,
              #{commit_index := CI, cluster := Cluster} = _State,
              Effects) ->
    #{PeerID := #{match_index := MI, voter := OldStatus} = _Peer} = Cluster,
    case evaluate_voter(OldStatus, MI, CI) of
        OldStatus ->
            Effects;
        Change ->
            [{next_event,
                {command, {'$ra_join',
                #{ts => os:system_time(millisecond)},
                PeerID,
                noreply}}} |
             Effects]
    end.

evaluate_voter({no, #{round := Round, target := Target , ts := RoundStart}}, MI, CI)
  when MI >= Target ->
    AtenPollInt = application:get_env(aten, poll_interval, 1000),
    Now = os:system_time(millisecond),
    case (Now - RoundStart) =< AtenPollInt of
        true ->
            yes;
        false when Round > ?DEFAULT_MAX_ROUNDS ->
            {no, permanent};
        false ->
            {no, #{round => Round+1, target => CI, ts => Now}}
    end;
evaluate_voter(Permanent, _, _) ->
    Permanent.

status(#{cluster := Cluster} = State) ->
    Id = ra_server:id(State),
    status(Cluster, Id).

status(Cluster, PeerId) ->
    case maps:get(PeerId, Cluster, undefined) of
        undefined ->
            throw(not_a_cluster_member);
        Peer ->
            maps:get(voter, Peer, yes)
    end.
