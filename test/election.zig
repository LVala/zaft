const std = @import("std");
const utils = @import("utils.zig");

const testing = std.testing;

const TestRPCs = utils.TestRPCs;
const TestRaft = utils.TestRaft;
const setupTestRaft = utils.setupTestRaft;
const getTime = utils.getTime;

test "successful election" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();

    const initial_term = raft.current_term;
    // nothing should happen after initial tick, it's to early for election timeout
    _ = raft.tick();

    try testing.expect(raft.state == .follower);

    // we forcefully overwrite the timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // new election should start
    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 1);
    for (rpcs[1..]) |rpc| {
        // candidate should send RequestVote to all other servers
        switch (rpc) {
            .request_vote => |rv| {
                try testing.expect(rv.term == initial_term + 1);
                try testing.expect(rv.candidate_id == 0);
            },
            else => {
                // is there a nicer way to do that?
                return error.TestUnexpectedResult;
            },
        }
    }

    // simulate other servers giving the vote to the candidate
    const rvr1 = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(1, TestRaft.RPC{ .request_vote_response = rvr1 });
    // no majority => still a candidate
    try testing.expect(raft.state == .candidate);

    raft.handleRPC(1, TestRaft.RPC{ .request_vote_response = rvr1 });
    // received response from the same server twice => still a candidate
    try testing.expect(raft.state == .candidate);

    const rvr2 = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = false };
    raft.handleRPC(2, TestRaft.RPC{ .request_vote_response = rvr2 });
    // vote not granted => still a candidate
    try testing.expect(raft.state == .candidate);

    raft.handleRPC(4, TestRaft.RPC{ .request_vote_response = rvr1 });
    try testing.expect(raft.state == .leader);

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // the leader itself won't ever stop being the leader without outside action
    try testing.expect(raft.state == .leader);
}

test "failed election (timeout)" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 1);

    const rvr1 = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(1, TestRaft.RPC{ .request_vote_response = rvr1 });
    try testing.expect(raft.state == .candidate);

    // candidate did not receive majority of the votes
    // => new election should start after the timeout
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 2);
}

test "failed election (other server became the leader)" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 1);

    const rvr = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(2, TestRaft.RPC{ .request_vote_response = rvr });
    try testing.expect(raft.state == .candidate);

    // received AppendEntries => sombody else become the leader
    const ae = TestRaft.AppendEntries{
        .term = initial_term + 1,
        .leader_id = 3,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };
    raft.handleRPC(3, TestRaft.RPC{ .append_entries = ae });
    try testing.expect(raft.state == .follower);
    try testing.expect(raft.current_term == initial_term + 1);
}

test "follower grants a vote" {
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    // so our log is not empty and candidate log is up-to-date
    // normally, one should use Raft.appendEntry
    try raft.log.append(.{ .term = initial_term, .entry = 5 });

    // skip timeout, but do not tick yet
    raft.timeout = getTime() - 1;

    // TODO: is it possible for the candidate to have the same term as the followers?
    // candidate increases its term when it becomes the candidate (stale candidate?)

    const cand1 = 3;
    const rv1 = TestRaft.RequestVote{
        .term = initial_term,
        .candidate_id = cand1,
        .last_log_term = 0,
        .last_log_index = 0,
    };
    raft.handleRPC(cand1, .{ .request_vote = rv1 });

    // shouldn't grant vote, log is out of date
    switch (rpcs[cand1]) {
        .request_vote_response => |rvr| {
            try testing.expect(!rvr.vote_granted);
            try testing.expect(rvr.term == initial_term);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    const rv2 = TestRaft.RequestVote{
        .term = initial_term,
        .candidate_id = cand1,
        .last_log_term = initial_term,
        .last_log_index = 50,
    };
    raft.handleRPC(cand1, .{ .request_vote = rv2 });

    switch (rpcs[cand1]) {
        .request_vote_response => |rvr| {
            try testing.expect(rvr.vote_granted);
            try testing.expect(rvr.term == initial_term);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    _ = raft.tick();

    // its after the first timeout, but it should be extended by the RequestVote
    try testing.expect(raft.state == .follower);
    try testing.expect(raft.voted_for == cand1);

    const cand2 = 1;
    const rv3 = TestRaft.RequestVote{
        .term = initial_term,
        .candidate_id = cand2,
        // TODO: dummy values
        .last_log_term = initial_term,
        .last_log_index = 50,
    };
    raft.handleRPC(cand2, .{ .request_vote = rv3 });

    // already voted => this vote shouldn't be granted
    switch (rpcs[cand2]) {
        .request_vote_response => |rvr| {
            try testing.expect(!rvr.vote_granted);
            try testing.expect(rvr.term == initial_term);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    try testing.expect(raft.state == .follower);
    try testing.expect(raft.voted_for == cand1);
}
