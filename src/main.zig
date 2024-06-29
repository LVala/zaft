const std = @import("std");
const assert = std.debug.assert;

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
            assert(id < server_no);

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

            std.log.info("Entry {any} appended, sending AppendEntries\n", .{entry});

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;
                self.sendAppendEntries(@intCast(idx));
            }

            self.timeout = newHeartbeatTimeout(getTime());
        }

        fn sendHeartbeat(self: *Self, time: u64) void {
            std.log.info("Sending heartbeat\n", .{});

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;
                self.sendAppendEntries(@intCast(idx));
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
                self.sendRequestVote(@intCast(idx));
            }

            self.timeout = newElectionTimeout(time);
        }

        fn convertToLeader(self: *Self) void {
            assert(self.state != .leader);

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

        fn sendRequestVote(self: *Self, to: u32) void {
            assert(to != self.id);
            assert(to < self.server_no);
            assert(self.state == .candidate);

            const request_vote = RequestVote{
                .term = self.current_term,
                .candidate_id = self.id,
                .last_log_index = @intCast(self.log.items.len),
                .last_log_term = if (self.log.getLastOrNull()) |last| last.term else 0,
            };
            const rpc = .{ .request_vote = request_vote };

            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                std.log.warn("Sending RequestVote to server {d} failed: {any}\n", .{ to, err });
            };
        }

        fn sendAppendEntries(self: *Self, to: u32) void {
            assert(to != self.id);
            assert(to < self.server_no);
            assert(self.state == .leader);

            const prev_log_idx = self.next_index[to] - 1;
            const prev_log_term = if (prev_log_idx > 0) self.log.items[prev_log_idx - 1].term else 0;

            const append_entries = AppendEntries{
                .term = self.current_term,
                .leader_id = self.id,
                .prev_log_index = prev_log_idx,
                .prev_log_term = prev_log_term,
                .entries = self.log.items[prev_log_idx..],
                .leader_commit = self.commit_index,
            };
            const rpc = .{ .append_entries = append_entries };

            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                std.log.warn("Sending AppendEntries to server {d} failed: {any}\n", .{ to, err });
            };
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

                assert(self.state == .follower);

                self.voted_for = msg.candidate_id;
                break :blk true;
            };

            std.log.info("Received RequestVote from server {d} with term {d}, vote_granted: {any}\n", .{ msg.candidate_id, msg.term, vote_granted });

            self.sendRequestVoteResponse(msg.candidate_id, vote_granted);

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

                // if prev_log_index == 0, this is the very first entry
                if (msg.prev_log_index != 0) {
                    // make sure the index and term of previous entry match
                    if (self.log.items.len < msg.prev_log_index) {
                        break :blk false;
                    }

                    // log indices start at 1!
                    const entry = self.log.items[msg.prev_log_index - 1];

                    if (entry.term != msg.prev_log_term) {
                        // make sure commited entries do not conflict
                        // otherwise, something went very wrong
                        assert(msg.prev_log_term > self.commit_index);
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

            std.log.info("Received AppendEntries from server {d} with term {d}, success: {any}\n", .{ msg.leader_id, msg.term, success });

            self.sendAppendEntriesResponse(msg.leader_id, success);

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

        fn sendRequestVoteResponse(self: *Self, to: u32, vote_granted: bool) void {
            const response = RequestVoteResponse{
                .term = self.current_term,
                .vote_granted = vote_granted,
            };

            const rpc = .{ .request_vote_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                std.log.warn("Sending RequestVoteResponse to server {d}, failed: {any}\n", .{ to, err });
            };
        }

        fn sendAppendEntriesResponse(self: *Self, to: u32, success: bool) void {
            const response = AppendEntriesResponse{
                .term = self.current_term,
                .success = success,
            };

            const rpc = .{ .append_entries_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                std.log.warn("Sending AppendEntriesResponse to server {d}, failed: {any}\n", .{ to, err });
            };
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
