const std = @import("std");
const utils = @import("utils.zig");

const testing = std.testing;

const TestUserData = utils.TestUserData;
const TestRaft = utils.TestRaft;

// all of the tests in the `/test` folder test the Raft struct API
// small unit tests can be kept close to the source

test {
    _ = @import("election.zig");
    _ = @import("append_entries.zig");
}

test "converts to follower after receiving RPC with greater term" {
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .leader);

    const rv = TestRaft.RequestVote{
        .term = initial_term + 1,
        .candidate_id = 1,
        .last_log_term = initial_term + 1,
        .last_log_index = 50,
    };
    raft.handleRPC(.{ .request_vote = rv });

    try testing.expect(raft.state == .follower);
    try testing.expect(raft.current_term == initial_term + 1);
}

test "leader sends heartbeats" {
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .leader);

    // initial AppendEntries heartbeat
    for (ud.rpcs[1..]) |rpc| {
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
    ud.rpcs = undefined;

    // artificially skip timeout
    raft.timeout = utils.getTime() - 1;
    _ = raft.tick();

    for (ud.rpcs[1..]) |rpc| {
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
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, false);
    defer raft.deinit();

    // skip timeout
    raft.timeout = utils.getTime() - 1;

    const ae = TestRaft.AppendEntries{
        .leader_id = 1,
        .term = raft.current_term,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };
    raft.handleRPC(.{ .append_entries = ae });

    try testing.expect(raft.state == .follower);
    _ = raft.tick();

    // append entries from leader => we should still be a follower after the tick
    try testing.expect(raft.state == .follower);
}
