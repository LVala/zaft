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
            applyEntry: *const fn (user_data: *UserData, entry: Entry) anyerror!void,
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
            // next_prev_index does not come from the Raft paper
            // it is what the follower expects to receive in next prev_log_idx
            // if !success, it is the last conflicting idx (so prev_log_idx) - 1
            // if success, it is the last log idx
            // allows to detect stale messages, decouples the response from AE itself
            // and makes the AE response idempotent
            next_prev_index: u32,
            // non-Raft field, just for the sake of convinience
            responder_id: u32,
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
            // non-Raft field, just for the sake of convinience
            responder_id: u32,
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

        current_leader: ?u32 = null,

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

        pub fn getCurrentLeader(self: *const Self) ?u32 {
            return self.current_leader;
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

        pub fn appendEntry(self: *Self, entry: Entry) !void {
            if (self.state != .leader) return error.NotALeader;

            try self.log.append(.{ .term = self.current_term, .entry = entry });

            self.next_index[self.id] = @as(u32, @intCast(self.log.items.len)) + 1;
            self.match_index[self.id] = @intCast(self.log.items.len);

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
            self.current_leader = self.id;
            for (self.next_index, self.match_index) |*ni, *mi| {
                ni.* = @as(u32, @intCast(self.log.items.len)) + 1;
                mi.* = 0;
            }

            // TODO: commit the no-op entry

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

        pub fn handleRPC(self: *Self, rpc: RPC) void {
            switch (rpc) {
                .request_vote => |msg| self.handleRequestVote(msg),
                .request_vote_response => |msg| self.handleRequestVoteResponse(msg),
                .append_entries => |msg| self.handleAppendEntries(msg),
                .append_entries_response => |msg| self.handleAppendEntriesResponse(msg),
            }
        }

        fn handleRequestVote(self: *Self, msg: RequestVote) void {
            assert(msg.candidate_id < self.server_no);
            assert(msg.candidate_id != self.id);

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

        fn handleRequestVoteResponse(self: *Self, msg: RequestVoteResponse) void {
            assert(msg.responder_id < self.server_no);
            assert(msg.responder_id != self.id);

            self.handleNewerTerm(msg.term);

            if (msg.term < self.current_term) return;
            if (self.state != .candidate) return;

            self.received_votes[msg.responder_id] = msg.vote_granted;

            var total_votes: u32 = 0;
            for (self.received_votes) |vote| {
                if (vote) total_votes += 1;
            }
            std.log.info("Received RequestVoteResponse from {d}, vote_granted: {any}, total votes: {d}\n", .{ msg.responder_id, msg.vote_granted, total_votes });

            if (total_votes >= self.server_no / 2 + 1) self.convertToLeader();
        }

        fn handleAppendEntries(self: *Self, msg: AppendEntries) void {
            assert(msg.leader_id < self.server_no);
            assert(msg.leader_id != self.id);

            self.handleNewerTerm(msg.term);

            const success = blk: {
                if (msg.term < self.current_term) {
                    break :blk false;
                }

                if (self.state == .candidate) self.convertToFollower();

                self.current_leader = msg.leader_id;

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

                        // remove conflicting entries
                        self.log.shrinkRetainingCapacity(msg.prev_log_index - 1);
                        break :blk false;
                    }
                }

                // this can be a stale AppendEntries, so we cannot just append mindlessly
                // otherwise, if we just removed entries after prev_log_index
                // we could remove entries added by AE out of order
                for (msg.entries, (msg.prev_log_index + 1)..) |entry, idx| {
                    if (idx <= self.log.items.len) {
                        // previous unsuccessful AEs should have removed conflicting elements
                        // in theory, AE could skip some conflicting indicies and jump to matching prev_log_index
                        // right away, but I'm not sure how and the assert won't hurt
                        // instead of the assert, we could just remove all of the following entries
                        assert(self.log.items[idx - 1].term == entry.term);
                    } else {
                        self.log.append(entry) catch |err| {
                            std.log.warn("Appending new entry to the log failed: {any}\n", .{err});
                            break :blk false;
                        };
                    }
                }

                // because we allow messages in invalid order, sometimes scenario like this will happen
                // numbers are terms, current term = 8 (example from Figure 7 in the Raft paper)
                // leader log: 1 1 1 4 4 5 5 6 6 6
                // follow log: 1 1 1 4 4 5 5 6 6 6 6 7 ...
                // in the first empty AE message prev_log_x values will match, but the last > 2 entries are invalid
                // because the for loop above doesn't remove entries, we will keep them
                // to avoid that, let's check the entry after the newly added entries (assuming it exists)
                // if its term is < msg.term, these are stale entries, and we can remove them
                const after_entries = msg.prev_log_index + msg.entries.len;
                if (after_entries < self.log.items.len) {
                    if (self.log.items[after_entries].term < msg.term) {
                        self.log.shrinkRetainingCapacity(after_entries);
                    }
                }
                // instead of doing that, we could remove entries in the for loop instead of the assert
                // but for that to work, we would need to wait for AE with index 11
                // now we remove the invalid entries right with the very first empty AE

                if (msg.leader_commit > self.commit_index) {
                    self.commit_index = @min(msg.leader_commit, self.log.items.len);
                    self.applyEntries();
                }

                break :blk true;
            };

            std.log.info("Received AppendEntries from server {d} with term {d}, success: {any}\n", .{ msg.leader_id, msg.term, success });

            self.sendAppendEntriesResponse(msg.leader_id, success);

            self.timeout = newElectionTimeout(getTime());
        }

        fn handleAppendEntriesResponse(self: *Self, msg: AppendEntriesResponse) void {
            assert(msg.responder_id < self.server_no);
            assert(msg.responder_id != self.id);

            self.handleNewerTerm(msg.term);

            if (msg.term < self.current_term) return;
            if (self.state != .leader) return;

            // stale message
            if (msg.next_prev_index < self.match_index[msg.responder_id]) return;

            std.log.info("Received AppendEntriesResponse from {d}\n", .{msg.responder_id});

            if (!msg.success) {
                // only when we succeed the next_prev_index can be equal
                // to the last index, otherwise is must be smaller
                assert(msg.next_prev_index < self.log.items.len);
                // assuming correct operation, next_index should always decrease by 1
                assert(self.next_index[msg.responder_id] - 2 == msg.next_prev_index);

                self.next_index[msg.responder_id] = msg.next_prev_index + 1;
                assert(self.next_index[msg.responder_id] > 0);

                self.sendAppendEntries(msg.responder_id);
                return;
            }

            assert(msg.next_prev_index <= self.log.items.len);

            self.next_index[msg.responder_id] = msg.next_prev_index + 1;
            self.match_index[msg.responder_id] = msg.next_prev_index;

            for ((self.commit_index + 1)..(msg.next_prev_index + 1)) |idx| {
                if (self.log.items[idx - 1].term == self.current_term) {
                    var votes: u32 = 0;
                    for (self.match_index) |repl_idx| {
                        if (repl_idx >= idx) votes += 1;
                    }

                    if (votes >= self.server_no / 2 + 1) {
                        self.commit_index = @intCast(idx);
                    }
                }
            }

            self.applyEntries();

            if (self.next_index[msg.responder_id] <= self.log.items.len) self.sendAppendEntries(msg.responder_id);
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
                .responder_id = self.id,
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
                // assuming we delete conflicting messages right away
                // this always will be correct value matching the definition at the begining of
                // the Raft struct
                .next_prev_index = @intCast(self.log.items.len),
                .responder_id = self.id,
            };

            const rpc = .{ .append_entries_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                std.log.warn("Sending AppendEntriesResponse to server {d}, failed: {any}\n", .{ to, err });
            };
        }

        fn applyEntries(self: *Self) void {
            while (self.commit_index > self.last_applied) {
                self.last_applied += 1;
                const entry = self.log.items[self.last_applied - 1].entry;
                self.callbacks.applyEntry(self.callbacks.user_data, entry) catch |err| {
                    std.debug.panic("Applying new entry {any} failed: {any}\n", .{ entry, err });
                };
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
