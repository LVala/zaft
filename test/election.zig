const std = @import("std");
const utils = @import("utils.zig");

const testing = std.testing;

const TestUserData = utils.TestUserData;
const TestRaft = utils.TestRaft;

test "successful election" {
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, false);
    defer raft.deinit();

    const initial_term = raft.current_term;
    // nothing should happen after initial tick, it's to early for election timeout
    _ = raft.tick();

    try testing.expect(raft.state == .follower);

    // we forcefully overwrite the timeout for testing purposes
    raft.timeout = utils.getTime() - 1;
    _ = raft.tick();

    // new election should start
    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 1);
    for (ud.rpcs[1..]) |rpc| {
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
    var rvr1 = TestRaft.RequestVoteResponse{
        .term = initial_term + 1,
        .vote_granted = true,
        .responder_id = 1,
    };
    raft.handleRPC(.{ .request_vote_response = rvr1 });
    // no majority => still a candidate
    try testing.expect(raft.state == .candidate);

    raft.handleRPC(.{ .request_vote_response = rvr1 });
    // received response from the same server twice => still a candidate
    try testing.expect(raft.state == .candidate);

    const rvr2 = TestRaft.RequestVoteResponse{
        .term = initial_term + 1,
        .vote_granted = false,
        .responder_id = 2,
    };
    raft.handleRPC(.{ .request_vote_response = rvr2 });
    // vote not granted => still a candidate
    try testing.expect(raft.state == .candidate);

    rvr1.responder_id = 4;
    raft.handleRPC(.{ .request_vote_response = rvr1 });
    try testing.expect(raft.state == .leader);

    raft.timeout = utils.getTime() - 1;
    _ = raft.tick();

    // the leader itself won't ever stop being the leader without outside action
    try testing.expect(raft.state == .leader);
}

test "failed election (timeout)" {
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = utils.getTime() - 1;
    _ = raft.tick();

    // first election should start
    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 1);

    const rvr1 = TestRaft.RequestVoteResponse{
        .term = initial_term + 1,
        .vote_granted = true,
        .responder_id = 1,
    };
    raft.handleRPC(.{ .request_vote_response = rvr1 });
    try testing.expect(raft.state == .candidate);

    // candidate did not receive majority of the votes
    // => new election should start after the timeout
    raft.timeout = utils.getTime() - 1;
    _ = raft.tick();

    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 2);
}

test "failed election (other server became the leader)" {
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = utils.getTime() - 1;
    _ = raft.tick();

    // first election should start
    try testing.expect(raft.state == .candidate);
    try testing.expect(raft.current_term == initial_term + 1);

    const rvr = TestRaft.RequestVoteResponse{
        .term = initial_term + 1,
        .vote_granted = true,
        .responder_id = 2,
    };
    raft.handleRPC(.{ .request_vote_response = rvr });
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
    raft.handleRPC(TestRaft.RPC{ .append_entries = ae });
    try testing.expect(raft.state == .follower);
    try testing.expect(raft.current_term == initial_term + 1);
}

test "follower grants a vote" {
    var ud = TestUserData.init(std.testing.allocator);
    defer ud.deinit();
    var raft = utils.setupTestRaft(&ud, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    // so our log is not empty and candidate log is up-to-date
    // normally, one should use Raft.appendEntry
    try utils.appendTestEntry(&raft, .{ .term = initial_term, .entry = 5 });

    // skip timeout, but do not tick yet
    raft.timeout = utils.getTime() - 1;

    const cand1 = 3;
    const rv1 = TestRaft.RequestVote{
        .term = initial_term,
        .candidate_id = cand1,
        .last_log_term = 0,
        .last_log_index = 0,
    };
    raft.handleRPC(.{ .request_vote = rv1 });

    // shouldn't grant vote, log is out of date
    switch (ud.rpcs[cand1]) {
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
    raft.handleRPC(.{ .request_vote = rv2 });

    switch (ud.rpcs[cand1]) {
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
        .last_log_term = initial_term,
        .last_log_index = 50,
    };
    raft.handleRPC(.{ .request_vote = rv3 });

    // already voted => this vote shouldn't be granted
    switch (ud.rpcs[cand2]) {
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
