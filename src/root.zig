const std = @import("std");

pub fn Raft(UserData: type, Entry: type) type {
    return struct {
        const State = enum { follower, candidate, leader };
        const Self = @This();

        const default_timeout = 1000;
        const heartbeat_timeout = 500;

        pub const LogEntry = struct { term: u32, entry: Entry };

        pub const Callbacks = struct {
            user_data: *UserData,
            makeRPC: *const fn (user_data: *UserData, id: u32, rpc: RPC) anyerror!void,
        };

        pub const AppendEntries = struct {
            term: u32,
            leader_id: u32,
            prev_log_index: u32,
            prev_log_term: u32,
            entries: []const LogEntry,
            leader_commit: u32,
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

        callbacks: Callbacks,
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

        pub fn init(id: u32, server_no: u32, callbacks: Callbacks, allocator: std.mem.Allocator) !Self {
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

            // TODO: apply commited entries here?

            // return min(remaining time, hearteat_timeout), even when
            // we aren't the leader (we might get elected quickly, and thus cannot
            // wait for the whole election_timeout to start sending hearbeats)
            return @min(self.timeout - time, heartbeat_timeout);
        }

        pub fn appendEntry(self: *Self, entry: Entry) !void {
            if (self.state != .leader) return error.NotALeader;

            try self.log.append(.{ .term = self.current_term, .entry = entry });

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                const append_entries = AppendEntries{
                    .term = self.current_term,
                    .leader_id = self.id,
                    // TODO: add new fields
                    .prev_log_index = 0,
                    .prev_log_term = 0,
                    .entries = &.{},
                    .leader_commit = self.commit_index,
                };

                const rpc = .{ .append_entries = append_entries };

                self.callbacks.makeRPC(self.callbacks.user_data, @intCast(idx), rpc) catch |err| {
                    std.log.warn("Sending AppendEntires to server {d} failed: {any}\n", .{ idx, err });
                };
            }

            self.timeout = newHeartbeatTimeout(getTime());
        }

        fn sendHeartbeat(self: *Self, time: u64) void {
            std.log.info("Sending heartbeat\n", .{});

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                // TODO: add new fields
                const append_entries = AppendEntries{
                    .term = self.current_term,
                    .leader_id = self.id,
                    // TODO: add new fields
                    .prev_log_index = 0,
                    .prev_log_term = 0,
                    .entries = &.{},
                    .leader_commit = self.commit_index,
                };
                const rpc = .{ .append_entries = append_entries };

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
                .append_entries => |msg| self.handleAppendEntries(msg),
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

            if (msg.term < self.current_term) return;
            if (self.state != .candidate) return;

            self.received_votes[id] = msg.vote_granted;

            var total_votes: u32 = 0;
            for (self.received_votes) |vote| {
                if (vote) total_votes += 1;
            }
            std.log.info("Received RequestVoteResponse, vote_granted: {any}, total votes: {d}\n", .{ msg.vote_granted, total_votes });

            if (total_votes >= self.server_no / 2 + 1) self.convertToLeader();
        }

        fn handleAppendEntries(self: *Self, msg: AppendEntries) void {
            self.handleNewerTerm(msg.term);

            const success = blk: {
                if (msg.term < self.current_term) {
                    break :blk false;
                }

                if (self.state == .candidate) self.convertToFollower();

                // TODO: set current leader to msg.leader_id
                // TODO: we can assert that prev_log_index is not < commit index

                // if prev_log_index == 0, this is the very first entry
                if (msg.prev_log_index != 0) {
                    // make sure the index and term of previous entry match
                    if (self.log.items.len < msg.prev_log_index) {
                        break :blk false;
                    }

                    // log indices start at 1!
                    const entry = self.log.items[msg.prev_log_index - 1];

                    if (entry.term != msg.prev_log_term) {
                        // TODO: term > our term? convert to follower
                        // also, msg.term should be greater in this case
                        // TODO: remove following entries
                        break :blk false;
                    }
                }

                // in case there are entries not matching after the prev_log_index
                // TODO: should we remove the entries right when we check the terms?
                // TODO: should be iterate over entries in the msg and append only after we find the non-matching one
                // I believe that all of the following entries should be non-matching in such case
                self.log.shrinkRetainingCapacity(msg.prev_log_index);
                self.log.appendSlice(msg.entries) catch |err| {
                    std.log.warn("Appending new entry to the log failed: {any}\n", .{err});
                    break :blk false;
                };

                if (msg.leader_commit > self.commit_index) {
                    self.commit_index = @min(msg.leader_commit, self.log.items.len);
                }

                break :blk true;
            };

            const response = AppendEntriesResponse{
                .term = self.current_term,
                .success = success,
            };

            std.log.info("Received AppendEntries from server {d} with term {d}, success: {any}\n", .{ msg.leader_id, msg.term, success });

            const rpc = .{ .append_entries_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, msg.leader_id, rpc) catch |err| {
                std.log.warn("Sending AppendEntriesResponse to server {d}, failed: {any}\n", .{ msg.leader_id, err });
            };

            self.timeout = newElectionTimeout(getTime());
        }

        fn handleAppendEntriesResponse(self: *Self, msg: AppendEntriesResponse, _: u32) void {
            self.handleNewerTerm(msg.term);

            if (msg.term < self.current_term) return;
            if (self.state != .leader) return;

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

const len = 5;
const TestRaft = Raft(anyopaque, u32);

fn setupTestRaft(rpcs: *[len]TestRaft.RPC, elect_leader: bool) TestRaft {
    const callbacks = TestRaft.Callbacks{
        .user_data = rpcs,
        .makeRPC = struct {
            pub fn makeRPC(ud: *anyopaque, id: u32, rpc: TestRaft.RPC) !void {
                // TODO: I don't know what I did here
                const ud_rpcs = @as(*[len]TestRaft.RPC, @alignCast(@ptrCast(ud)));
                ud_rpcs.*[id] = rpc;
            }
        }.makeRPC,
    };

    // our Raft always gets the id 0
    var raft = TestRaft.init(0, len, callbacks, std.testing.allocator) catch unreachable;

    if (!elect_leader) return raft;

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    for (1..len) |id| {
        const rvr = TestRaft.RequestVoteResponse{ .term = raft.current_term, .vote_granted = true };
        raft.handleRPC(@intCast(id), .{ .request_vote_response = rvr });
    }

    return raft;
}

test "successful election" {
    var rpcs: [len]TestRaft.RPC = undefined;
    var raft = setupTestRaft(&rpcs, false);
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
    const rvr1 = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(1, TestRaft.RPC{ .request_vote_response = rvr1 });
    // no majority => still a candidate
    try std.testing.expect(raft.state == .candidate);

    raft.handleRPC(1, TestRaft.RPC{ .request_vote_response = rvr1 });
    // received response from the same server twice => still a candidate
    try std.testing.expect(raft.state == .candidate);

    const rvr2 = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = false };
    raft.handleRPC(2, TestRaft.RPC{ .request_vote_response = rvr2 });
    // vote not granted => still a candidate
    try std.testing.expect(raft.state == .candidate);

    raft.handleRPC(4, TestRaft.RPC{ .request_vote_response = rvr1 });
    try std.testing.expect(raft.state == .leader);

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // the leader itself won't ever stop being the leader without outside action
    try std.testing.expect(raft.state == .leader);
}

test "failed election (timeout)" {
    var rpcs: [len]TestRaft.RPC = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 1);

    const rvr1 = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(1, TestRaft.RPC{ .request_vote_response = rvr1 });
    try std.testing.expect(raft.state == .candidate);

    // candidate did not receive majority of the votes
    // => new election should start after the timeout
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 2);
}

test "failed election (other server became the leader)" {
    var rpcs: [len]TestRaft.RPC = undefined;
    var raft = setupTestRaft(&rpcs, false);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .follower);

    // skip timeout for testing purposes
    raft.timeout = getTime() - 1;
    _ = raft.tick();

    // first election should start
    try std.testing.expect(raft.state == .candidate);
    try std.testing.expect(raft.current_term == initial_term + 1);

    const rvr = TestRaft.RequestVoteResponse{ .term = initial_term + 1, .vote_granted = true };
    raft.handleRPC(2, TestRaft.RPC{ .request_vote_response = rvr });
    try std.testing.expect(raft.state == .candidate);

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
    try std.testing.expect(raft.state == .follower);
    try std.testing.expect(raft.current_term == initial_term + 1);
}

test "follower grants a vote" {
    var rpcs: [len]TestRaft.RPC = undefined;
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
            try std.testing.expect(!rvr.vote_granted);
            try std.testing.expect(rvr.term == initial_term);
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
    var rpcs: [len]TestRaft.RPC = undefined;
    var raft = setupTestRaft(&rpcs, true);
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
    var rpcs: [len]TestRaft.RPC = undefined;
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

    try std.testing.expect(raft.state == .follower);
    _ = raft.tick();

    // append entries from leader => we should still be a follower after the tick
    try std.testing.expect(raft.state == .follower);
}

fn test_converge_logs(leader_log: []const TestRaft.LogEntry, leader_term: u32, follower_log: []const TestRaft.LogEntry, first_common: u32) !void {
    // first_common is a log index (so starts at 1), not array index!
    var rpcs: [len]TestRaft.RPC = undefined;
    var raft = setupTestRaft(&rpcs, true);
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
            .leader_commit = 0,
        };
        raft.handleRPC(leader_id, .{ .append_entries = ae });

        switch (rpcs[leader_id]) {
            .append_entries_response => |aer| {
                const valid = if (idx == first_common + 1) aer.success else !aer.success;
                try std.testing.expect(valid);
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }

    // check if the follower log is equal to leaders log
    try std.testing.expect(leader_log.len == raft.log.items.len);
    for (leader_log, raft.log.items) |l_entry, f_entry| {
        try std.testing.expect(l_entry.term == f_entry.term);
        try std.testing.expect(l_entry.entry == f_entry.entry);
    }

    try std.testing.expect(raft.current_term == leader_term);
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

test "converts to follower after receiving RPC with greater term" {
    var rpcs: [len]TestRaft.RPC = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();
    const initial_term = raft.current_term;

    try std.testing.expect(raft.state == .leader);

    const rv = TestRaft.RequestVote{
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
