const std = @import("std");
const utils = @import("utils.zig");

const testing = std.testing;

const TestRPCs = utils.TestRPCs;
const TestRaft = utils.TestRaft;
const setupTestRaft = utils.setupTestRaft;
const getTime = utils.getTime;

// all of the tests in the `/test` folder test the Raft struct API
// small unit tests can be kept close to the source

test {
    _ = @import("election.zig");
    _ = @import("append_entries.zig");
}

test "converts to follower after receiving RPC with greater term" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .leader);

    const rv = TestRaft.RequestVote{
        .term = initial_term + 1,
        .candidate_id = 1,
        // TODO: dummy values
        .last_log_term = initial_term + 1,
        .last_log_index = 50,
    };
    raft.handleRPC(1, .{ .request_vote = rv });

    try testing.expect(raft.state == .follower);
    try testing.expect(raft.current_term == initial_term + 1);
}

test "leader sends heartbeats" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .leader);

    // initial AppendEntries heartbeat
    for (rpcs[1..]) |rpc| {
        switch (rpc) {
            .append_entries => |ae| {
                try testing.expect(ae.term == initial_term);
                try testing.expect(ae.leader_id == 0);
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }

    // clear messages (so we don't inspect stale messages later on)
    rpcs = undefined;

    // artificially skip timeout
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    for (rpcs[1..]) |rpc| {
        switch (rpc) {
            .append_entries => |ae| {
                try testing.expect(ae.term == initial_term);
                try testing.expect(ae.leader_id == 0);
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }
}

test "follower respects heartbeats" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();

    // skip timeout
    raft.timeout = getTime() - 1;

    const ae = TestRaft.AppendEntries{
        .leader_id = 1,
        .term = raft.current_term,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };
    raft.handleRPC(1, .{ .append_entries = ae });

    try testing.expect(raft.state == .follower);
    _ = raft.tick();

    // append entries from leader => we should still be a follower after the tick
    try testing.expect(raft.state == .follower);
}
