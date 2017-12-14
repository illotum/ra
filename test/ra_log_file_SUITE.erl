-module(ra_log_file_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%
%%

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     handle_overwrite,
     receive_segment,
     read_one,
     take_after_overwrite_and_init,
     validate_sequential_reads,
     validate_reads_for_overlapped_writes,
     cache_overwrite_then_take,
     last_written_overwrite,
     recovery,
     resend_write,
     wal_down_read_availability,
     wal_down_append_throws,
     wal_down_write_returns_error_wal_down,

     detect_lost_written_range,
     snapshot_recovery,
     snapshot_installation,
     update_release_cursor,
     update_release_cursor_after_recovery,
     transient_writer_is_handled
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, ?config(priv_dir, Config)),
    ok = application:set_env(ra, segment_max_entries, 128),
    application:ensure_all_started(ra),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

init_per_group(tests, Config) ->
    Config.

end_per_group(tests, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    register(TestCase, self()),
    application:stop(ra),
    application:start(ra),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].

end_per_testcase(_, _Config) ->
    ok.

handle_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {queued, Log1} = ra_log_file:write([{1, 1, "value"}, {2, 1, "value"}], Log0),
    receive
        {ra_log_event, {written, {1, 2, 1}}} -> ok
    after 2000 ->
              exit(written_timeout)
    end,
    {queued, Log3} = ra_log_file:write([{1, 2, "value"}], Log1),
    % ensure immediate truncation
    {1, 2} = ra_log_file:last_index_term(Log3),
    {queued, Log4} = ra_log_file:write([{2, 2, "value"}], Log3),
    % simulate the first written event coming after index 20 has already
    % been written in a new term
    Log = ra_log_file:handle_event({written, {1, 2, 1}}, Log4),
    % ensure last written has not been incremented
    {0, 0} = ra_log_file:last_written(Log),
    {2, 2} = ra_log_file:last_written(
                ra_log_file:handle_event({written, {1, 2, 2}}, Log)),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    _ = deliver_all_log_events(Log, 1000),
    ra_log_file:close(Log),
    ok.

receive_segment(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % write a few entries
    Entries = [{I, 1, <<"value_", I:32/integer>>} || I <- lists:seq(1, 3)],

    Log1 = lists:foldl(fun(E, Acc0) ->
                               {queued, Acc} =
                                   ra_log_file:append(E, Acc0),
                               Acc
                       end, Log0, Entries),
    Log2 = deliver_all_log_events(Log1, 500),
    {3, 1} = ra_log_file:last_written(Log2),
    [MemTblTid] = [Tid || {receive_segment, _, _, Tid}
                          <- ets:tab2list(ra_log_open_mem_tables)],
    % force wal roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log3 = deliver_all_log_events(Log2, 1500),
    % validate ets table has been recovered
    ?assert(lists:member(MemTblTid, ets:all()) =:= false),
    [] = ets:tab2list(ra_log_open_mem_tables),
    [] = ets:tab2list(ra_log_closed_mem_tables),
    % validate reads
    {Entries, FinalLog} = ra_log_file:take(1, 3, Log3),
    ra_log_file:close(FinalLog),
    ok.

read_one(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    Log1 = append_n(1, 2, 1, Log0),
    % ensure the written event is delivered
    Log2 = deliver_all_log_events(Log1, 200),
    {[_], Log} = ra_log_file:take(1, 5, Log2),
    % read out of range
    {[], Log} = ra_log_file:take(5, 5, Log2),
    [{_, M1, M2, M3, M4} = M] = ets:lookup(ra_log_file_metrics, Self),
    ct:pal("M ~p", [M]),
    % read two entries
    ?assert(M1 + M2 + M3 + M4 =:= 1),
    ra_log_file:close(Log),
    ok.

take_after_overwrite_and_init(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    Log1 = write_and_roll_no_deliver(1, 5, 1, Log0),
    Log2 = deliver_written_log_events(Log1, 200),
    {[_, _, _, _], Log3} = ra_log_file:take(1, 5, Log2),
    Log4 = write_and_roll_no_deliver(1, 2, 2, Log3),
    % fake lost segments event
    Log5 = deliver_written_log_events(Log4, 200),
    % ensure we cannot take stale entries
    ct:pal("closed ~p", [ets:tab2list(ra_log_closed_mem_tables)]),
    {[{1, 2, _}], Log6} = ra_log_file:take(1, 5, Log5),
    _ = ra_log_file:close(Log6),
    Log = ra_log_file:init(#{directory => Dir, id => Self}),
    {[{1, 2, _}], _} = ra_log_file:take(1, 5, Log),
    ok.


validate_sequential_reads(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % write a few entries
    Log1 = append_and_roll(1, 100, 1, Log0),
    Log2 = append_and_roll(100, 200, 1, Log1),
    Log3 = append_and_roll(200, 400, 1, Log2),
    Log4 = append_and_roll(400, 500, 1, Log3),
    Log = append_and_roll(500, 1001, 1, Log4),
    {ColdTaken, {ColdReds, FinLog}} =
        timer:tc(fun () ->
                         {_, Reds0} = process_info(self(), reductions),
                         L = validate_read(1, 1001, 1, Log),
                         {_, Reds} = process_info(self(), reductions),
                         {Reds - Reds0, L}
                 end),
    [{_, M1, M2, M3, M4}] = Metrics = ets:lookup(ra_log_file_metrics, Self),
    ?assert(M1 + M2 + M3 + M4 =:= 1000),

    ct:pal("validate_sequential_reads COLD took ~pms Reductions: ~p~nMetrics: ~p",
           [ColdTaken/1000, ColdReds, Metrics]),
    % we'd like to know if we regress beyond this
    % some of the reductions are spent validating the reads
    ?assert(ColdReds < 100000),
    {WarmTaken, {WarmReds, FinLog2}} =
        timer:tc(fun () ->
                         {_, R0} = process_info(self(), reductions),
                         L = validate_read(1, 1001, 1, FinLog),
                         {_, R} = process_info(self(), reductions),
                         {R - R0, L}
                 end),
    ct:pal("validate_sequential_reads WARM took ~pms Reductions: ~p~n",
           [WarmTaken/1000, WarmReds]),
    % we'd like to know if we regress beyond this
    ?assert(WarmReds < 75000),
    ra_log_file:close(FinLog2),
    ok.

validate_reads_for_overlapped_writes(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % write a segment and roll 1 - 299 - term 1
    Log1 = write_and_roll(1, 300, 1, Log0),
    % write 300 - 399 in term 1 - no roll
    Log2 = write_n(300, 400, 1, Log1),
    % write 200 - 350 in term 2 and roll
    Log3 = write_and_roll(200, 350, 2, Log2),
    % write 350 - 500 in term 2
    Log4 = write_and_roll(350, 500, 2, Log3),
    Log5 = write_n(500, 551, 2, Log4),
    Log6 = deliver_all_log_events(Log5, 200),

    Log7 = validate_read(1, 200, 1, Log6),
    Log8 = validate_read(200, 551, 2, Log7),

    [{_, M1, M2, M3, M4}] = Metrics = ets:lookup(ra_log_file_metrics, Self),
    ct:pal("Metrics: ~p", [Metrics]),
    ?assert(M1 + M2 + M3 + M4 =:= 550),
    ra_log_file:close(Log8),
    ok.

cache_overwrite_then_take(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    Log1 = write_n(1, 5, 1, Log0),
    Log2 = write_n(3, 4, 2, Log1),
    % validate only 3 entries can be read even if requested range is greater
    {[_, _, _], _} = ra_log_file:take(1, 5, Log2),
    ok.

last_written_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    Log1 = write_n(1, 5, 1, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    {4, 1} = ra_log_file:last_written(Log2),
    % write an event for a prior index
    {queued, Log3} = ra_log_file:write([{3, 2, <<3:64/integer>>}], Log2),
    Log4 = deliver_all_log_events(Log3, 200),
    {3, 2} = ra_log_file:last_written(Log4),
    ok.

recovery(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = write_and_roll(1, 10, 1, Log0),
    {9, 1} = ra_log_file:last_index_term(Log1),
    Log2 = write_and_roll(5, 15, 2, Log1),
    {14, 2} = ra_log_file:last_index_term(Log2),
    Log3 = write_n(15, 21, 3, Log2),
    {20, 3} = ra_log_file:last_index_term(Log3),
    Log4 = deliver_all_log_events(Log3, 200),
    {20, 3} = ra_log_file:last_index_term(Log4),
    ra_log_file:close(Log4),
    application:stop(ra),
    application:ensure_all_started(ra),
    % % TODO how to avoid sleep
    timer:sleep(2000),
    Log5 = ra_log_file:init(#{directory => Dir, id => Self}),
    {20, 3} = ra_log_file:last_index_term(Log5),
    Log6 = validate_read(1, 5, 1, Log5),
    Log7 = validate_read(5, 15, 2, Log6),
    Log8 = validate_read(15, 21, 3, Log7),
    ra_log_file:close(Log8),

    ok.

resend_write(Config) ->
    % simulate lost messages requiring the ra node to resend in flight
    % writes
    meck:new(ra_log_wal, [passthrough]),
    meck:expect(ra_log_wal, write, fun (_, _, 10, _, _) -> ok;
                                       (A, B, C, D, E) ->
                                           meck:passthrough([A, B, C, D, E])
                                   end),
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    % fake missing entry
    Log2b = append_n(10, 11, 2, Log2),
    Log3 = append_n(11, 13, 2, Log2b),
    Log4 = receive
               {ra_log_event, {resend_write, 10} = Evt} ->
                   ra_log_file:handle_event(Evt, Log3)
           after 500 ->
                     throw(resend_write_timeout)
           end,
    {queued, Log5} = ra_log_file:append({13, 2, banana}, Log4),
    Log6 = deliver_all_log_events(Log5, 500),
    {[_, _, _, _, _], _} = ra_log_file:take(9, 5, Log6),

    meck:unload(ra_log_wal),
    ok.

wal_down_read_availability(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),

    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 200),
    ok = supervisor:terminate_child(ra_log_wal_sup, ra_log_wal),
    {Entries, _} = ra_log_file:take(0, 10, Log2),
    ?assert(length(Entries) =:= 10),
    ok.

wal_down_append_throws(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),

    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    ?assert(ra_log_file:can_write(Log0)),
    ok = supervisor:terminate_child(ra_log_wal_sup, ra_log_wal),
    ?assert(not ra_log_file:can_write(Log0)),
    ?assertExit(wal_down, ra_log_file:append({1,1,hi}, Log0)),
    ok.

wal_down_write_returns_error_wal_down(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    ok = supervisor:terminate_child(ra_log_wal_sup, ra_log_wal),
    {error, wal_down} = ra_log_file:write([{1,1,hi}], Log0),
    ok.

detect_lost_written_range(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    meck:new(ra_log_wal, [passthrough]),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self,
                              wal => ra_log_wal}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    % write some entries
    Log1 = append_and_roll(1, 10, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    % WAL rolls over and WAL file is deleted
    % simulate wal outage
    meck:expect(ra_log_wal, write, fun (_, _, _, _, _) -> ok end),

    % append some messages that will be lost
    Log3 = append_n(10, 15, 2, Log2),

    % restart WAL to ensure lose the transient state keeping track of
    % each writer's last written index
    ok = supervisor:terminate_child(ra_log_wal_sup, ra_log_wal),
    {ok, _} = supervisor:restart_child(ra_log_wal_sup, ra_log_wal),

    % WAL recovers
    meck:unload(ra_log_wal),

    % append some more stuff
    Log4 = append_n(15, 20, 2, Log3),
    Log5 = deliver_all_log_events(Log4, 2000),

    % validate no writes were lost and can be recovered
    {Entries, _} = ra_log_file:take(0, 20, Log5),
    ra_log_file:close(Log5),
    Log = ra_log_file:init(#{directory => Dir, id => Self}),
    {RecoveredEntries, _} = ra_log_file:take(0, 20, Log),
    ?assert(length(Entries) =:= 20),
    ?assert(length(RecoveredEntries) =:= 20),
    Entries = RecoveredEntries,
    ok.

snapshot_recovery(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = append_and_roll(1, 10, 2, Log0),
    Snapshot = {9, 2, #{n1 => #{}}, <<"9">>},
    Log2 = ra_log_file:write_snapshot(Snapshot, Log1),
    Log3 = deliver_all_log_events(Log2, 500),
    ra_log_file:close(Log3),
    Log = ra_log_file:init(#{directory => Dir, id => Self}),
    Snapshot = ra_log_file:read_snapshot(Log),
    {9, 2} = ra_log_file:last_index_term(Log),
    {[], _} = ra_log_file:take(1, 9, Log),
    ok.

snapshot_installation(Config) ->
    % write a few entries
    % simulate outage/ message loss
    % write snapshot for entry not seen
    % then write entries
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = write_n(1, 10, 2, Log0),
    Snapshot = {15, 2, #{n1 => #{}}, <<"9">>},
    Log2 = ra_log_file:write_snapshot(Snapshot, Log1),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log3 = write_n(16, 20, 2, Log2),
    Log = deliver_all_log_events(Log3, 500),
    {19, 2} = ra_log_file:last_index_term(Log),
    {[], _} = ra_log_file:take(1, 9, Log),
    {[_, _], _} = ra_log_file:take(16, 2, Log),
    ok.

update_release_cursor(Config) ->
    % ra_log_file should initiate shapshot if segments can be released
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % beyond 128 limit - should create two segments
    Log1 = append_and_roll(1, 150, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    % assert there are two segments at this point
    [_, _] = filelib:wildcard(filename:join(Dir, "*.segment")),
    % leave one entry in the current segment
    Log3 = ra_log_file:update_release_cursor(150, #{n1 => #{}, n2 => #{}},
                                             initial_state, Log2),
    Log4 = deliver_all_log_events(Log3, 500),
    % no segments
    [] = filelib:wildcard(filename:join(Dir, "*.segment")),
    % append a few more items
    Log5 = append_and_roll(150, 155, 2, Log4),
    _Log6 = deliver_all_log_events(Log5, 500),
    % assert there is only one segment - the current
    % snapshot has been confirmed.
    [_] = filelib:wildcard(filename:join(Dir, "*.segment")),

    ok.

update_release_cursor_after_recovery(Config) ->
    % ct:pal("All ets before: ~p", [ets:all()]),
    % ra_log_file should initiate shapshot if segments can be released
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % assert there are no segments at this point
    [] = find_segments(Dir),

    % record ets tables before test
    EtsBefore = ets:all(),
    % create a segment
    Log1 = deliver_all_log_events(append_and_roll(1, 130, 2, Log0), 500),
    % and another but don't notify ra_node
    Log2 = append_n(130, 150, 2, Log1),
    Log3 = deliver_all_log_events(Log2, 500),
    empty_mailbox(),
    % ok = ra_log_wal:force_roll_over(ra_log_wal),

    % recover WAL
    ok = supervisor:terminate_child(ra_log_wal_sup, ra_log_wal),
    {ok, _} = supervisor:restart_child(ra_log_wal_sup, ra_log_wal),
    timer:sleep(1000),

    % then deliver all log events

    % assert there is one segment at this point
    Log4 = append_and_roll(150, 155, 2, Log3),
    % leave one entry in the current segment
    Log5 = ra_log_file:update_release_cursor(150, #{n1 => #{}, n2 => #{}},
                                             initial_state, Log4),
    % Log6 = append_n(155, 160, 2, Log5),
    Log = deliver_all_log_events(Log5, 500),

    [] = ets:tab2list(ra_log_closed_mem_tables),
    [] = ets:tab2list(ra_log_open_mem_tables),

    % assert no ets tables were left behind
    [] = ets:all() -- EtsBefore,

    % validate reads are ok
    {149, 2, _, initial_state} = ra_log_file:read_snapshot(Log),
    validate_read(150, 155, 2, Log),

    % validate there is only one segments left
    [_] = find_segments(Dir),
    ok.

transient_writer_is_handled(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, _Self} = erlang:process_info(self(), registered_name),
    _Pid = spawn(fun () ->
                         erlang:register(sub_proc, self()),
                         Log0 = ra_log_file:init(#{directory => Dir, id => sub_proc}),
                         Log1 = append_n(1, 10, 2, Log0),
                         % ignore events
                         Log2 = deliver_all_log_events(Log1, 500),
                         ra_log_file:close(Log2)
                 end),
    application:stop(ra),
    application:start(ra),
    timer:sleep(2000),
    ok.

validate_read(To, To, _Term, Log0) ->
    Log0;
validate_read(From, To, Term, Log0) ->
    End = min(From + 5, To),
    {Entries, Log} = ra_log_file:take(From, End - From, Log0),
    % validate entries are correctly read
    Expected = [ {I, Term, <<I:64/integer>>} ||
                 I <- lists:seq(From, End - 1) ],
    Expected = Entries,
    validate_read(End, To, Term, Log).


append_and_roll(From, To, Term, Log0) ->
    Log1 = append_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    deliver_all_log_events(Log1, 200).

write_and_roll(From, To, Term, Log0) ->
    Log1 = write_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    deliver_all_log_events(Log1, 200).

write_and_roll_no_deliver(From, To, Term, Log0) ->
    Log1 = write_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log1.

% not inclusivw
append_n(To, To, _Term, Log) ->
    Log;
append_n(From, To, Term, Log0) ->
    {queued, Log} = ra_log_file:append({From, Term,
                                        <<From:64/integer>>}, Log0),
    append_n(From+1, To, Term, Log).

write_n(From, To, Term, Log0) ->
    Entries = [{X, Term, <<X:64/integer>>} ||
               X <- lists:seq(From, To - 1)],
    {queued, Log} = ra_log_file:write(Entries, Log0),
    Log.

%% Utility functions

deliver_all_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            Log = ra_log_file:handle_event(Evt, Log0),
            deliver_all_log_events(Log, 100)
    after Timeout ->
              Log0
    end.

deliver_written_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, {written, _} = Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            Log = ra_log_file:handle_event(Evt, Log0),
            deliver_written_log_events(Log, 100)
    after Timeout ->
              Log0
    end.

validate_rolled_reads(_Config) ->
    % 1. configure WAL to low roll over limit
    % 2. append enough entries to ensure it has rolled over
    % 3. pass all log events received to ra_log_file
    % 4. validate all entries can be read
    % 5. check there is only one .wal file
    exit(not_implemented).

find_segments(Dir) ->
    filelib:wildcard(filename:join(Dir, "*.segment")).

empty_mailbox() ->
    receive
        _ ->
            empty_mailbox()
    after 100 ->
              ok
    end.
