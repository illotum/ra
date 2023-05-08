%% This Source Code Form is subject to the terms of the Mozilla Public
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_voter).

-export([
         new_nonvoter/1,
         status/1,
         peer_status/2
        ]).

new_nonvoter(State) ->
    TargetIdx = maps:get(commit_index, State),
    {no, #{round => 0, target => TargetIdx , ts => os:system_time(millisecond)}}.

status(State) ->
    case maps:get(voter, State) of
        undefined ->
            MyId = ra_server:id(State),
            #{cluster := Cluster} = State,
            peer_status(MyId, Cluster);
        Voter -> Voter
    end.

peer_status(PeerId, Cluster) ->
    Peer = maps:get(PeerId, Cluster, undefined),
    maps:get(voter, Peer, yes).
