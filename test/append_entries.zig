const std = @import("std");
const utils = @import("utils.zig");

const testing = std.testing;

const TestRPCs = utils.TestRPCs;
const TestRaft = utils.TestRaft;
const setupTestRaft = utils.setupTestRaft;

fn test_converge_logs(leader_log: []const TestRaft.LogEntry, leader_term: u32, follower_log: []const TestRaft.LogEntry, first_common: u32) !void {
    // first_common is a log index (so starts at 1), not array index!
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();

    const leader_id = 1;

    for (follower_log) |entry| try raft.log.append(entry);
    raft.current_term = follower_log[follower_log.len - 1].term;

    // we start with empty AppendEntries
    var idx: usize = leader_log.len + 1;
    while (idx > first_common) : (idx -= 1) {
        const ae = TestRaft.AppendEntries{
            .leader_id = leader_id,
            .term = leader_term,
            .prev_log_index = @intCast(idx - 1),
            .prev_log_term = leader_log[idx - 2].term,
            .entries = leader_log[(idx - 1)..],
            .leader_commit = 2,
        };
        raft.handleRPC(.{ .append_entries = ae });

        switch (rpcs[leader_id]) {
            .append_entries_response => |aer| {
                const valid = if (idx == first_common + 1) aer.success else !aer.success;
                try testing.expect(valid);
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }

    // check if the follower log is equal to leaders log
    try testing.expect(leader_log.len == raft.log.items.len);
    for (leader_log, raft.log.items) |l_entry, f_entry| {
        try testing.expect(l_entry.term == f_entry.term);
        try testing.expect(l_entry.entry == f_entry.entry);
    }

    try testing.expect(raft.state == .follower);
    try testing.expect(raft.current_term == leader_term);
    try testing.expect(raft.commit_index == 2);
    try testing.expect(raft.last_applied == 2);
}

test "follower adds new entries from AppendEntries" {
    // from Figure 7 in the Raft paper
    const leader_log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
    };

    const follower_log1 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log1, 9);

    const follower_log2 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log2, 4);
}

test "follower replaces conflicting entires based on AppendEntries" {
    // from Figure 7 in the Raft paper
    const leader_log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
    };

    // entries with value 99 should be removed
    const follower_log1 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
        .{ .term = 6, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log1, 10);

    const follower_log2 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
        .{ .term = 7, .entry = 99 },
        .{ .term = 7, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log2, 10);

    const follower_log3 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 4, .entry = 99 },
        .{ .term = 4, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log3, 5);

    const follower_log4 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 2, .entry = 99 },
        .{ .term = 2, .entry = 99 },
        .{ .term = 2, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log4, 3);
}

test "stale AppendEntries does not shorten followers log" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();

    const leader_id = 1;
    const leader_term = 6;

    const log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 2, .entry = 3 },
        .{ .term = 2, .entry = 4 },
        .{ .term = 3, .entry = 5 },
        .{ .term = 4, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
    };

    for (log[0..6]) |entry| try raft.log.append(entry);
    raft.current_term = leader_term;

    // new leader has 2 new entries: log[6] and log[7]
    // leader send the mesages one after another, without waiting for the answer
    // but the AppendEntries messages came in invalid order
    // even though, the final state should be vaild
    const ae = TestRaft.AppendEntries{
        .leader_id = leader_id,
        .term = leader_term,
        .prev_log_index = 6,
        .prev_log_term = 4,
        .entries = log[6..],
        .leader_commit = 0,
    };
    raft.handleRPC(.{ .append_entries = ae });

    switch (rpcs[leader_id]) {
        .append_entries_response => |aer| {
            try testing.expect(aer.success);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    // log should be already in the final state
    try testing.expect(log.len == raft.log.items.len);
    for (log, raft.log.items) |l_entry, f_entry| {
        try testing.expect(l_entry.term == f_entry.term);
        try testing.expect(l_entry.entry == f_entry.entry);
    }

    // now, the stale AppendEntries comes
    const ae2 = TestRaft.AppendEntries{
        .leader_id = leader_id,
        .term = leader_term,
        .prev_log_index = 6,
        .prev_log_term = 4,
        .entries = log[6..7],
        .leader_commit = 0,
    };
    raft.handleRPC(.{ .append_entries = ae2 });

    switch (rpcs[leader_id]) {
        .append_entries_response => |aer| {
            try testing.expect(aer.success);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    // nothing should change in the log, but the response should be successful
    try testing.expect(log.len == raft.log.items.len);
    for (log, raft.log.items) |l_entry, f_entry| {
        try testing.expect(l_entry.term == f_entry.term);
        try testing.expect(l_entry.entry == f_entry.entry);
    }
}

test "leader sends AppendEntries on initial entry" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    const entries = [_]u32{ 1, 2, 3 };
    for (entries) |entry| {
        const idx = try raft.appendEntry(entry);
        // appendEntries returns the index of the newly appended entry
        try testing.expect(idx == entry);
    }

    // initial AppendEntries heartbeat
    for (rpcs[1..]) |rpc| {
        switch (rpc) {
            .append_entries => |ae| {
                try testing.expect(ae.term == initial_term);
                try testing.expect(ae.leader_id == 0);
                try testing.expect(ae.prev_log_index == 0);
                try testing.expect(ae.prev_log_term == 0);
                try testing.expect(ae.leader_commit == 0);

                try testing.expect(ae.entries.len == 3);
                for (ae.entries, entries) |entry, entry_val| {
                    try testing.expect(entry.term == initial_term);
                    try testing.expect(entry.entry == entry_val);
                }
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }
}

test "leader handles AppendEntriesResponse" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();

    const follower_id = 1;

    const log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 2, .entry = 3 },
        .{ .term = 2, .entry = 4 },
        .{ .term = 3, .entry = 5 },
        .{ .term = 4, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
    };

    // pretend as if leader started with log in this state
    for (log) |entry| try raft.log.append(entry);
    raft.current_term = 6;
    for (raft.next_index) |*ni| ni.* = 9;
    for (raft.match_index) |*mi| mi.* = 0;

    // this the leader's match index
    raft.match_index[0] = 8;

    // lets assume the last entry conflicts
    const aer = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = false,
        .next_prev_index = 7,
        .responder_id = follower_id,
    };
    raft.handleRPC(.{ .append_entries_response = aer });

    switch (rpcs[follower_id]) {
        .append_entries => |ae| {
            try testing.expect(ae.prev_log_index == 7);
            try testing.expect(ae.prev_log_term == 5);
            try testing.expect(ae.entries.len == 1);
            try testing.expect(ae.entries[0].term == 6);
            try testing.expect(ae.entries[0].entry == 8);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    try testing.expect(raft.next_index[follower_id] == 8);
    try testing.expect(raft.match_index[follower_id] == 0);

    // it was conflicting once again
    const aer2 = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = false,
        .next_prev_index = 6,
        .responder_id = follower_id,
    };
    raft.handleRPC(.{ .append_entries_response = aer2 });

    switch (rpcs[follower_id]) {
        .append_entries => |ae| {
            try testing.expect(ae.prev_log_index == 6);
            try testing.expect(ae.prev_log_term == 4);
            try testing.expect(ae.entries.len == 2);
            for (log[6..], ae.entries) |log_entry, ae_entry| {
                try testing.expect(ae_entry.term == log_entry.term);
                try testing.expect(ae_entry.entry == log_entry.entry);
            }
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    // some dummy value so we can later assert that the raft did not sent another AE
    rpcs[follower_id] = .{ .request_vote_response = TestRaft.RequestVoteResponse{ .term = 0, .vote_granted = false, .responder_id = 0 } };

    try testing.expect(raft.next_index[follower_id] == 7);
    try testing.expect(raft.match_index[follower_id] == 0);

    const aer3 = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = true,
        .next_prev_index = 8,
        .responder_id = follower_id,
    };
    raft.handleRPC(.{ .append_entries_response = aer3 });

    switch (rpcs[follower_id]) {
        .append_entries => return error.TestUnexpectedResult,
        else => {},
    }

    try testing.expect(raft.next_index[follower_id] == 9);
    try testing.expect(raft.match_index[follower_id] == 8);
}

test "leader commits entries after replicating on majority of servers" {
    // we have 5 servers
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();

    try testing.expect(raft.commit_index == 0);

    _ = try raft.appendEntry(5);

    var aer = TestRaft.AppendEntriesResponse{
        .success = true,
        .term = raft.current_term,
        .next_prev_index = 1,
        .responder_id = 1,
    };
    raft.handleRPC(.{ .append_entries_response = aer });
    try testing.expect(raft.commit_index == 0);

    // duplicate message, should be ignored
    aer.responder_id = 1;
    raft.handleRPC(.{ .append_entries_response = aer });
    try testing.expect(raft.commit_index == 0);

    aer.responder_id = 2;
    raft.handleRPC(.{ .append_entries_response = aer });
    try testing.expect(raft.commit_index == 1);

    // the rest of responsed shouldn't change anything
    for (3..5) |id| {
        aer.responder_id = @intCast(id);
        raft.handleRPC(.{ .append_entries_response = aer });
    }

    try testing.expect(raft.commit_index == 1);
    for (0..5) |id| try testing.expect(raft.match_index[id] == 1);
    for (0..5) |id| try testing.expect(raft.next_index[id] == 2);
}

test "leader does not commit entires from previous terms" {
    // we have 5 servers
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();

    const log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 5, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
    };

    // pretend as if leader started with log in this state
    for (log) |entry| try raft.log.append(entry);
    raft.current_term = 6;
    raft.commit_index = 5;
    for (raft.next_index) |*ni| ni.* = 6;
    for (raft.match_index) |*mi| mi.* = 5;

    // leader's match and next index
    raft.next_index[0] = 11;
    raft.match_index[0] = 10;

    // the last follower replicated up to 7th entry somehow
    const aer1 = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = true,
        .next_prev_index = 7,
        .responder_id = 4,
    };
    raft.handleRPC(.{ .append_entries_response = aer1 });

    // nothing should change so far
    try testing.expect(raft.next_index[4] == 8);
    try testing.expect(raft.match_index[4] == 7);
    try testing.expect(raft.commit_index == 5);

    const aer2 = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = true,
        .next_prev_index = 9,
        .responder_id = 1,
    };
    raft.handleRPC(.{ .append_entries_response = aer2 });

    // now the 6th entry should be replicated by majority of the servers
    // but (because of old term) it should not be commited just yet
    try testing.expect(raft.next_index[1] == 10);
    try testing.expect(raft.match_index[1] == 9);
    try testing.expect(raft.commit_index == 5);

    const aer3 = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = true,
        .next_prev_index = 10,
        .responder_id = 2,
    };
    raft.handleRPC(.{ .append_entries_response = aer3 });

    // now 9th entry is replicated on majority of the servers
    // and it's term is equal to the current term
    // so it should be commited together with all of the previous entries
    try testing.expect(raft.next_index[2] == 11);
    try testing.expect(raft.match_index[2] == 10);
    try testing.expect(raft.commit_index == 9);
    try testing.expect(raft.last_applied == 9);

    const aer4 = TestRaft.AppendEntriesResponse{
        .term = raft.current_term,
        .success = true,
        .next_prev_index = 10,
        .responder_id = 3,
    };
    raft.handleRPC(.{ .append_entries_response = aer4 });

    try testing.expect(raft.next_index[3] == 11);
    try testing.expect(raft.match_index[3] == 10);
    try testing.expect(raft.commit_index == 10);
    try testing.expect(raft.last_applied == 10);
}
