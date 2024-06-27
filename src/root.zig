const std = @import("std");

pub const AppendEntries = struct {
    term: u32,
    leader_id: u32,
    // prev_log_index: u32,
    // prev_log_term: u32,
    // entries: []u32,
    // leader_commit: u32,
};

pub const AppendEntriesResponse = struct {
    term: u32,
    success: bool,
};

pub const RequestVote = struct {
    term: u32,
    candidate_id: u32,
    last_log_index: u32,
    last_log_term: u32,
};

pub const RequestVoteResponse = struct {
    term: u32,
    vote_granted: bool,
};

pub const RPC = union(enum) {
    append_entries: AppendEntries,
    append_entries_response: AppendEntriesResponse,
    request_vote: RequestVote,
    request_vote_response: RequestVoteResponse,
};

pub fn Callbacks(UserData: type) type {
    return struct {
        user_data: *UserData,
        makeRPC: *const fn (user_data: *UserData, id: u32, rpc: RPC) anyerror!void,
    };
}

pub fn Raft(UserData: type) type {
    return struct {
        const State = enum { follower, candidate, leader };
        // TODO: u32 is temporary
        const LogEntry = struct { term: u32, entry: u32 };
        const Self = @This();

        const default_timeout = 1000;
        const heartbeat_timeout = 500;

        callbacks: Callbacks(UserData),
        allocator: std.mem.Allocator,
        id: u32,
        server_no: u32,

        state: State,
        timeout: u64,

        // TODO: these need to be persisted after change
        current_term: u32 = 0,
        voted_for: ?u32 = null,
        log: std.ArrayList(LogEntry),

        commit_index: u32 = 0,
        last_applied: u32 = 0,

        // leader-specific
        next_index: []u32,
        match_index: []u32,

        // candidate-specific
        received_votes: []bool,

        pub fn init(id: u32, server_no: u32, callbacks: Callbacks(UserData), allocator: std.mem.Allocator) !Self {
            std.log.info("Initializing Raft, id: {d}, number of servers: {d}\n", .{ id, server_no });

            const time = getTime();
            return Self{
                .callbacks = callbacks,
                .allocator = allocator,
                .id = id,
                .server_no = server_no,
                .state = .follower,
                .timeout = newElectionTimeout(time),
                .log = std.ArrayList(LogEntry).init(allocator),
                .next_index = try allocator.alloc(u32, server_no),
                .match_index = try allocator.alloc(u32, server_no),
                .received_votes = try allocator.alloc(bool, server_no),
            };
        }

        pub fn deinit(self: *Self) void {
            self.log.deinit();
            self.allocator.free(self.next_index);
            self.allocator.free(self.match_index);
            self.allocator.free(self.received_votes);
        }

        pub fn tick(self: *Self) u64 {
            const time = getTime();
            if (self.timeout > time) {
                // too early!
                const remaining = self.timeout - time;
                return if (self.state == .follower) remaining else @min(remaining, heartbeat_timeout);
            }

            switch (self.state) {
                .leader => self.sendHeartbeat(time),
                else => self.convertToCandidate(time),
            }

            // return min(remaining time, hearteat_timeout), even when
            // we aren't the leader (we might get elected quickly, and thus cannot
            // wait for the whole election_timeout to start sending hearbeats)
            return @min(self.timeout - time, heartbeat_timeout);
        }

        pub fn appendEntry(self: *Self, entry: u32) void {
            // TODO
            // 1. If not a leader => redirect
            // 2. add the entry to the log
            // 3. Send appendEntries to others

            self.log.append(.{ .term = self.current_term, .entry = entry }) catch unreachable;
        }

        fn sendHeartbeat(self: *Self, time: u64) void {
            std.log.info("Sending heartbeat\n", .{});

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                const request_vote = AppendEntries{
                    .term = self.current_term,
                    .leader_id = self.id,
                };
                const rpc = .{ .append_entries = request_vote };

                self.callbacks.makeRPC(self.callbacks.user_data, @intCast(idx), rpc) catch |err| {
                    std.log.warn("Sending heartbeat to server {d} failed: {any}\n", .{ idx, err });
                };
            }

            self.timeout = newHeartbeatTimeout(time);
        }

        fn convertToCandidate(self: *Self, time: u64) void {
            self.current_term += 1;
            self.state = .candidate;
            self.voted_for = self.id;
            for (self.received_votes) |*vote| {
                vote.* = false;
            }
            self.received_votes[self.id] = true;

            std.log.info("Converted to candidate, starting new election, term: {d}\n", .{self.current_term});

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                const request_vote = RequestVote{
                    .term = self.current_term,
                    .candidate_id = self.id,
                    .last_log_index = @intCast(self.log.items.len),
                    // TODO: should we send 0 when theres not entries in the log?
                    .last_log_term = if (self.log.getLastOrNull()) |last| last.term else 0,
                };
                const rpc = .{ .request_vote = request_vote };

                self.callbacks.makeRPC(self.callbacks.user_data, @intCast(idx), rpc) catch |err| {
                    std.log.warn("Sending RequestVote to server {d} failed: {any}\n", .{ idx, err });
                };
            }

            self.timeout = newElectionTimeout(time);
        }

        fn convertToLeader(self: *Self) void {
            std.log.info("Converted to leader\n", .{});

            self.state = .leader;
            for (self.next_index, self.match_index) |*ni, *mi| {
                ni.* = @as(u32, @intCast(self.log.items.len)) + 1;
                mi.* = 0;
            }

            self.sendHeartbeat(getTime());
        }

        fn convertToFollower(self: *Self) void {
            std.log.info("Converted to follower\n", .{});

            self.state = .follower;
            self.voted_for = null;
            self.timeout = newElectionTimeout(getTime());
        }

        pub fn handleRPC(self: *Self, id: u32, rpc: RPC) void {
            switch (rpc) {
                .request_vote => |msg| self.handleRequestVote(msg),
                .request_vote_response => |msg| self.handleRequestVoteResponse(msg, id),
                .append_entries => |msg| self.handleAppendEntries(msg, id),
                .append_entries_response => |msg| self.handleAppendEntriesResponse(msg, id),
            }
        }

        fn handleRequestVote(self: *Self, msg: RequestVote) void {
            self.handleNewerTerm(msg.term);

            const vote_granted = blk: {
                if (msg.term < self.current_term) {
                    break :blk false;
                }

                // ensure candidate's log is up-to-date
                const last_term = if (self.log.getLastOrNull()) |last| last.term else 0;
                if (msg.last_log_term < last_term) {
                    break :blk false;
                }

                if (msg.last_log_term == last_term and msg.last_log_index < self.log.items.len) {
                    break :blk false;
                }

                // ensure we did not vote previously
                // this should be false if we are not a follower
                // as the leader and a candidate would vote for themselves already
                if (self.voted_for != null and self.voted_for != msg.candidate_id) {
                    break :blk false;
                }

                self.voted_for = msg.candidate_id;
                break :blk true;
            };

            const response = RequestVoteResponse{
                .term = self.current_term,
                .vote_granted = vote_granted,
            };

            std.log.info("Received RequestVote from server {d} with term {d}, vote_granted: {any}\n", .{ msg.candidate_id, msg.term, vote_granted });

            const rpc = .{ .request_vote_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, msg.candidate_id, rpc) catch |err| {
                std.log.warn("Sending RequestVoteResponse to server {d}, failed: {any}\n", .{ msg.candidate_id, err });
            };

            self.timeout = newElectionTimeout(getTime());
        }

        fn handleRequestVoteResponse(self: *Self, msg: RequestVoteResponse, id: u32) void {
            self.handleNewerTerm(msg.term);
            if (self.state != .candidate) return;

            self.received_votes[id] = msg.vote_granted;

            var total_votes: u32 = 0;
            for (self.received_votes) |vote| {
                if (vote) total_votes += 1;
            }
            std.log.info("Received RequestVoteResponse, vote_granted: {any}, total votes: {d}\n", .{ msg.vote_granted, total_votes });

            if (total_votes >= self.server_no / 2 + 1) self.convertToLeader();
        }

        fn handleAppendEntries(self: *Self, msg: AppendEntries, _: u32) void {
            self.handleNewerTerm(msg.term);

            std.log.info("Received AppendEntries from server {d}\n", .{msg.leader_id});
            // TODO
            self.convertToFollower();
        }

        fn handleAppendEntriesResponse(self: *Self, msg: AppendEntriesResponse, _: u32) void {
            self.handleNewerTerm(msg.term);

            std.log.info("Received AppendEntriesResponse\n", .{});
            // TODO
        }

        fn handleNewerTerm(self: *Self, msg_term: u32) void {
            if (msg_term > self.current_term) {
                self.current_term = msg_term;
                self.convertToFollower();
            }
        }

        fn newElectionTimeout(time: u64) u64 {
            const start = default_timeout;
            const stop = 2 * default_timeout;
            const interval = std.crypto.random.intRangeAtMost(u64, start, stop);
            return time + interval;
        }

        fn newHeartbeatTimeout(time: u64) u64 {
            return time + heartbeat_timeout;
        }
    };
}

fn getTime() u64 {
    const time_ms = std.time.milliTimestamp();
    return @intCast(time_ms);
}

// TESTS

fn setupTestRaft(comptime len: usize, rpcs: *[len]RPC, elect_leader: bool) Raft([len]RPC) {
    const callbacks = Callbacks([len]RPC){ .user_data = rpcs, .makeRPC = struct {
        pub fn makeRPC(ud: *[len]RPC, id: u32, rpc: RPC) !void {
            ud.*[id] = rpc;
        }
    }.makeRPC };

    // our Raft always gets the id 0
    var raft = Raft([len]RPC).init(0, len, callbacks, std.testing.allocator) catch unreachable;

    if (!elect_leader) return raft;

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    for (1..len) |id| {
        const rvr = RequestVoteResponse{ .term = raft.current_term, .vote_granted = true };
        raft.handleRPC(@intCast(id), .{ .request_vote_response = rvr });
    }

    return raft;
}

test "successful election" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    // nothing should happen after initial tick, it's to early for election timeout
    _ = raft.tick();

    try std.testing.expect(raft.state == .follower);

    // we forcefully overwrite the timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // new election should start
    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 1);
    for (rpcs[1..]) |rpc| {
        // candidate should send RequestVote to all other servers
        switch (rpc) {
            .request_vote => |rv| {
                try std.testing.expect(rv.term == initial_term + 1);
                try std.testing.expect(rv.candidate_id == 0);
            },
            else => {
                // is there a nicer way to do that?
                return error.TestUnexpectedResult;
            },
        }
    }

    // simulate other servers giving the vote to the candidate
    const rvr1 = RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(1, RPC{ .request_vote_response = rvr1 });
    // no majority => still a candidate
    try std.testing.expect(raft.state == .candidate);

    raft.handleRPC(1, RPC{ .request_vote_response = rvr1 });
    // received response from the same server twice => still a candidate
    try std.testing.expect(raft.state == .candidate);

    const rvr2 = RequestVoteResponse{ .term = initial_term + 1, .vote_granted = false };
    raft.handleRPC(2, RPC{ .request_vote_response = rvr2 });
    // vote not granted => still a candidate
    try std.testing.expect(raft.state == .candidate);

    raft.handleRPC(4, RPC{ .request_vote_response = rvr1 });
    try std.testing.expect(raft.state == .leader);

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // the leader itself won't ever stop being the leader without outside action
    try std.testing.expect(raft.state == .leader);
}

test "failed election (timeout)" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 1);

    const rvr1 = RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(1, RPC{ .request_vote_response = rvr1 });
    try std.testing.expect(raft.state == .candidate);

    // candidate did not receive majority of the votes
    // => new election should start after the timeout
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 2);
}

test "failed election (other server became the leader)" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 1);

    const rvr = RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(2, RPC{ .request_vote_response = rvr });
    try std.testing.expect(raft.state == .candidate);

    // received AppendEntries => sombody else become the leader
    const ae = AppendEntries{ .term = initial_term + 1, .leader_id = 3 };
    raft.handleRPC(3, RPC{ .append_entries = ae });
    try std.testing.expect(raft.state == .follower);
    try std.testing.expect(raft.current_term == initial_term + 1);
}

test "follower grants a vote" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    // so our log is not empty
    raft.appendEntry(5);

    // skip timeout, but do not tick yet
    raft.timeout = getTime() - 1;

    // TODO: is it possible for the candidate to have the same term as the followers?
    // candidate increases its term when it becomes the candidate (stale candidate?)

    const cand1 = 3;
    const rv1 = RequestVote{
        .term = initial_term,
        .candidate_id = cand1,
        .last_log_term = 0,
        .last_log_index = 0,
    };
    raft.handleRPC(cand1, .{ .request_vote = rv1 });

    // shouldn't grant vote, log is out of date
    switch (rpcs[cand1]) {
        .request_vote_response => |rvr| {
            try std.testing.expect(!rvr.vote_granted);
            try std.testing.expect(rvr.term == initial_term);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    const rv2 = RequestVote{
        .term = initial_term,
        .candidate_id = cand1,
        .last_log_term = initial_term,
        .last_log_index = 50,
    };
    raft.handleRPC(cand1, .{ .request_vote = rv2 });

    switch (rpcs[cand1]) {
        .request_vote_response => |rvr| {
            try std.testing.expect(rvr.vote_granted);
            try std.testing.expect(rvr.term == initial_term);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    _ = raft.tick();

    // its after the first timeout, but it should be extended by the RequestVote
    try std.testing.expect(raft.state == .follower);
    try std.testing.expect(raft.voted_for == cand1);

    const cand2 = 1;
    const rv3 = RequestVote{
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
            try std.testing.expect(!rvr.vote_granted);
            try std.testing.expect(rvr.term == initial_term);
        },
        else => {
            return error.TestUnexpectedResult;
        },
    }

    try std.testing.expect(raft.state == .follower);
    try std.testing.expect(raft.voted_for == cand1);
}

test "leader sends heartbeats" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .leader);

    // initial AppendEntries heartbeat
    for (rpcs[1..]) |rpc| {
        switch (rpc) {
            .append_entries => |ae| {
                try std.testing.expect(ae.term == initial_term);
                try std.testing.expect(ae.leader_id == 0);
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
                try std.testing.expect(ae.term == initial_term);
                try std.testing.expect(ae.leader_id == 0);
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }
}

test "follower respects heartbeats" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, false);
    defer raft.deinit();

    // skip timeout
    raft.timeout = getTime() - 1;

    const ae = AppendEntries{ .leader_id = 1, .term = raft.current_term };
    raft.handleRPC(1, .{ .append_entries = ae });

    try std.testing.expect(raft.state == .follower);
    _ = raft.tick();

    // append entries from leader => we should still be a follower after the tick
    try std.testing.expect(raft.state == .follower);
}

test "converts to follower after receiving RPC with greater term" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .leader);

    const rv = RequestVote{
        .term = initial_term + 1,
        .candidate_id = 1,
        // TODO: dummy values
        .last_log_term = initial_term + 1,
        .last_log_index = 50,
    };
    raft.handleRPC(1, .{ .request_vote = rv });

    try std.testing.expect(raft.state == .follower);
    try std.testing.expect(raft.current_term == initial_term + 1);
}
