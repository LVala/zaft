const std = @import("std");

pub const AppendEntries = struct {
    term: u32,
    leader_id: u32,
};

pub const AppendEntriesResponse = struct {
    term: u32,
    success: bool,
};

pub const RequestVote = struct {
    term: u32,
    candidate_id: u32,
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
        const Self = @This();

        const default_timeout = 1000;
        const heartbeat_timeout = 500;

        callbacks: Callbacks(UserData),
        id: u32,
        server_no: u32,

        state: State,
        timeout: u64,

        current_term: u32 = 0,
        voted_for: ?u32 = null,

        // candidate-specific
        votes: u32 = 0,

        pub fn init(id: u32, server_no: u32, callbacks: Callbacks(UserData)) Self {
            std.log.info("Initializing Raft, id: {d}, number of servers: {d}\n", .{ id, server_no });

            const time = getTime();
            return Self{
                .callbacks = callbacks,
                .id = id,
                .server_no = server_no,
                .state = .follower,
                .timeout = newElectionTimeout(time),
            };
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
            self.votes = 1;

            std.log.info("Starting new election, term: {d}\n", .{self.current_term});

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                const request_vote = RequestVote{
                    .term = self.current_term,
                    .candidate_id = self.id,
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
            self.sendHeartbeat(getTime());
        }

        pub fn handleRPC(self: *Self, rpc: RPC) void {
            // TODO: if term in the rpc is > self.current_term
            // revert back to follower
            switch (rpc) {
                .request_vote => |msg| self.handleRequestVote(msg),
                .request_vote_response => |msg| self.handleRequestVoteResponse(msg),
                .append_entries => |msg| self.handleAppendEntries(msg),
                .append_entries_response => |msg| self.handleAppendEntriesResponse(msg),
            }
        }

        fn handleRequestVote(self: *Self, msg: RequestVote) void {
            const vote_granted = blk: {
                if (msg.term < self.current_term) {
                    break :blk false;
                }

                if (self.voted_for == null or self.voted_for == msg.candidate_id) {
                    // TODO: also check if log is up-to-data
                    break :blk true;
                }

                break :blk false;
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

        fn handleRequestVoteResponse(self: *Self, msg: RequestVoteResponse) void {
            // TODO: probably should verify who is the response from to ignore duplicates
            if (self.state != .candidate) return;
            if (msg.vote_granted) self.votes += 1;

            std.log.info("Received RequestVoteResponse, vote_granted: {any}, total votes: {d}\n", .{ msg.vote_granted, self.votes });

            if (self.votes >= self.server_no / 2 + 1) self.convertToLeader();
        }

        fn handleAppendEntries(self: *Self, msg: AppendEntries) void {
            std.log.info("Received AppendEntries from server {d}\n", .{msg.leader_id});
            self.state = .follower;
            self.timeout = newElectionTimeout(getTime());
        }

        fn handleAppendEntriesResponse(self: *Self, msg: AppendEntriesResponse) void {
            _ = self;
            _ = msg;
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

fn setupTestRaft(comptime len: usize, rpcs: *[len]RPC) Raft([len]RPC) {
    const callbacks = Callbacks([len]RPC){ .user_data = rpcs, .makeRPC = struct {
        pub fn makeRPC(ud: *[len]RPC, id: u32, rpc: RPC) !void {
            ud.*[id] = rpc;
        }
    }.makeRPC };

    // our raft always gets the id 0
    return Raft([len]RPC).init(0, len, callbacks);
}

test "successful election" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs);
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
    raft.handleRPC(RPC{ .request_vote_response = rvr1 });
    // no majority => still a candidate
    try std.testing.expect(raft.state == .candidate);

    const rvr2 = RequestVoteResponse{ .term = initial_term + 1, .vote_granted = false };
    raft.handleRPC(RPC{ .request_vote_response = rvr2 });
    // vote not granted => still a candidate
    try std.testing.expect(raft.state == .candidate);

    // TODO: not implemented but should be eventually tested
    // const rvr3 = RequestVoteResponse{ .term = initial_term, .vote_granted = true };
    // raft.handleRPC(RPC{ .request_vote_response = rvr3 });
    // // stale message => still a candidate
    // try std.testing.expect(raft.state == .candidate);

    raft.handleRPC(RPC{ .request_vote_response = rvr1 });
    try std.testing.expect(raft.state == .leader);

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // the leader itself won't ever stop being the leader without outside action
    try std.testing.expect(raft.state == .leader);
}

test "failed election" {
    var rpcs: [5]RPC = undefined;
    var raft = setupTestRaft(rpcs.len, &rpcs);
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 1);

    const rvr1 = RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(RPC{ .request_vote_response = rvr1 });
    try std.testing.expect(raft.state == .candidate);

    // candidate did not receive majority of the votes
    // => new election should start after the timeout
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 2);
}
