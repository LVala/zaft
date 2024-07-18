const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.raft);

pub fn Raft(UserData: type, Entry: type) type {
    return struct {
        const State = enum { follower, candidate, leader };
        const Self = @This();

        pub const LogEntry = struct { term: u32, entry: Entry };

        pub const Config = struct {
            id: u32,
            server_no: u32,
            election_timeout: u64 = 1000,
            heartbeat_timeout: u64 = 500,
        };

        pub const InitialState = struct {
            current_term: ?u32 = null,
            voted_for: ?u32 = null,
            log: []LogEntry = &.{},
        };

        pub const Callbacks = struct {
            user_data: *UserData,
            makeRPC: *const fn (user_data: *UserData, id: u32, rpc: RPC) anyerror!void,
            applyEntry: *const fn (user_data: *UserData, entry: Entry) anyerror!void,
            logAppend: *const fn (user_data: *UserData, entry: LogEntry) anyerror!void,
            logPop: *const fn (user_data: *UserData) anyerror!LogEntry,
            persistCurrentTerm: *const fn (user_data: *UserData, current_term: u32) anyerror!void,
            persistVotedFor: *const fn (user_data: *UserData, voted_for: ?u32) anyerror!void,
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

        config: Config,
        callbacks: Callbacks,
        allocator: std.mem.Allocator,

        state: State,
        timeout: u64,

        current_leader: ?u32 = null,

        current_term: u32,
        voted_for: ?u32,
        log: std.ArrayList(LogEntry),

        commit_index: u32 = 0,
        last_applied: u32 = 0,

        // leader-specific
        next_index: []u32,
        match_index: []u32,

        // candidate-specific
        received_votes: []bool,

        pub fn init(config: Config, initial_state: InitialState, callbacks: Callbacks, allocator: std.mem.Allocator) !Self {
            assert(config.id < config.server_no);

            log.info("Initializing Raft, id: {}, number of servers: {}", .{ config.id, config.server_no });

            var self = Self{
                .config = config,
                .callbacks = callbacks,
                .allocator = allocator,
                .state = .follower,
                .current_term = initial_state.current_term orelse 0,
                .voted_for = initial_state.voted_for,
                .timeout = 0,
                .log = std.ArrayList(LogEntry).init(allocator),
                .next_index = try allocator.alloc(u32, config.server_no),
                .match_index = try allocator.alloc(u32, config.server_no),
                .received_votes = try allocator.alloc(bool, config.server_no),
            };

            for (initial_state.log) |entry| {
                try self.log.append(entry);
            }

            self.next_index[config.id] = @as(u32, @intCast(self.log.items.len)) + 1;
            self.match_index[config.id] = @intCast(self.log.items.len);

            self.setElectionTimeout(getTime());

            return self;
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

        pub fn checkIfApplied(self: *const Self, idx: u32) bool {
            return idx <= self.last_applied;
        }

        pub fn tick(self: *Self) u64 {
            const time = getTime();
            if (self.timeout > time) {
                // too early!
                const remaining = self.timeout - time;
                return if (self.state == .follower) remaining else @min(remaining, self.config.heartbeat_timeout);
            }

            switch (self.state) {
                .leader => self.sendHeartbeat(time),
                else => self.convertToCandidate(time),
            }

            // return min(remaining time, hearteat_timeout), even when
            // we aren't the leader (we might get elected quickly, and thus cannot
            // wait for the whole election_timeout to start sending hearbeats)
            return @min(self.timeout - time, self.config.heartbeat_timeout);
        }

        pub fn appendEntry(self: *Self, entry: Entry) !u32 {
            if (self.state != .leader) return error.NotALeader;

            const log_entry = LogEntry{ .term = self.current_term, .entry = entry };
            self.appendLogEntry(log_entry);

            // just for the sake of being consistent with next_index and match_index for other servers
            self.next_index[self.config.id] = @as(u32, @intCast(self.log.items.len)) + 1;
            self.match_index[self.config.id] = @intCast(self.log.items.len);

            log.info("Appended entry, term: {}, index: {}, entry: {}", .{ log_entry.term, self.log.items.len, entry });

            for (0..self.config.server_no) |idx| {
                if (idx == self.config.id) continue;
                self.sendAppendEntries(@intCast(idx));
            }

            self.setHeartbeatTimeout(getTime());

            return @intCast(self.log.items.len);
        }

        fn sendHeartbeat(self: *Self, time: u64) void {
            for (0..self.config.server_no) |idx| {
                if (idx == self.config.id) continue;
                self.sendAppendEntries(@intCast(idx));
            }

            self.setHeartbeatTimeout(time);
        }

        fn convertToCandidate(self: *Self, time: u64) void {
            self.state = .candidate;

            self.setCurrentTerm(self.current_term + 1);
            self.setVotedFor(self.config.id);
            for (self.received_votes) |*vote| {
                vote.* = false;
            }
            self.received_votes[self.config.id] = true;

            log.info("Converted to candidate, term: {}", .{self.current_term});

            for (0..self.config.server_no) |idx| {
                if (idx == self.config.id) continue;
                self.sendRequestVote(@intCast(idx));
            }

            self.setElectionTimeout(time);
        }

        fn convertToLeader(self: *Self) void {
            assert(self.state != .leader);

            log.info("Converted to leader, term: {}", .{self.current_term});

            self.state = .leader;
            self.current_leader = self.config.id;
            for (self.next_index, self.match_index) |*ni, *mi| {
                ni.* = @as(u32, @intCast(self.log.items.len)) + 1;
                mi.* = 0;
            }

            // TODO: commit the no-op entry

            self.sendHeartbeat(getTime());
        }

        fn convertToFollower(self: *Self) void {
            log.info("Converted to follower, term: {}", .{self.current_term});

            self.state = .follower;
            self.setVotedFor(null);
            self.setElectionTimeout(getTime());
        }

        fn sendRequestVote(self: *Self, to: u32) void {
            assert(to != self.config.id);
            assert(to < self.config.server_no);
            assert(self.state == .candidate);

            const request_vote = RequestVote{
                .term = self.current_term,
                .candidate_id = self.config.id,
                .last_log_index = @intCast(self.log.items.len),
                .last_log_term = if (self.log.getLastOrNull()) |last| last.term else 0,
            };
            const rpc = .{ .request_vote = request_vote };

            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                log.warn("Sending RequestVote to server {} failed: {}", .{ to, err });
                return;
            };

            log.debug("Sent RequestVote to {}, term: {}, ll_index: {}, ll_term: {}", .{
                to,
                request_vote.term,
                request_vote.last_log_index,
                request_vote.last_log_term,
            });
        }

        fn sendAppendEntries(self: *Self, to: u32) void {
            assert(to != self.config.id);
            assert(to < self.config.server_no);
            assert(self.state == .leader);

            const prev_log_idx = self.next_index[to] - 1;
            const prev_log_term = if (prev_log_idx > 0) self.log.items[prev_log_idx - 1].term else 0;

            const append_entries = AppendEntries{
                .term = self.current_term,
                .leader_id = self.config.id,
                .prev_log_index = prev_log_idx,
                .prev_log_term = prev_log_term,
                .entries = self.log.items[prev_log_idx..],
                .leader_commit = self.commit_index,
            };
            const rpc = .{ .append_entries = append_entries };

            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                log.warn("Sending AppendEntries to server {} failed: {}", .{ to, err });
                return;
            };

            log.debug("Sent AppendEntries to {}, term: {}, pl_index: {}, pl_term: {}, l_commit: {}, entries number: {any}", .{
                to,
                append_entries.term,
                append_entries.prev_log_index,
                append_entries.prev_log_term,
                append_entries.leader_commit,
                append_entries.entries.len,
            });
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
            assert(msg.candidate_id < self.config.server_no);
            assert(msg.candidate_id != self.config.id);

            self.handleNewerTerm(msg.term);

            log.debug("Received RequestVote from {}, term: {}, ll_index: {}, ll_term: {}", .{
                msg.candidate_id,
                msg.term,
                msg.last_log_index,
                msg.last_log_term,
            });

            const vote_granted = blk: {
                if (msg.term < self.current_term) {
                    log.debug("Vote not granted, stale message (current term = {})", .{self.current_term});
                    break :blk false;
                }

                // ensure candidate's log is up-to-date
                const last_term = if (self.log.getLastOrNull()) |last| last.term else 0;
                if (msg.last_log_term < last_term) {
                    log.debug("Vote not granted, log not up-to-date (last log term = {})", .{last_term});
                    break :blk false;
                }

                if (msg.last_log_term == last_term and msg.last_log_index < self.log.items.len) {
                    log.debug("Vote not granted, log not up-to-date (last log index = {})", .{self.log.items.len});
                    break :blk false;
                }

                // ensure we did not vote previously
                // this should be false if we are not a follower
                // as the leader and a candidate would vote for themselves already
                if (self.voted_for != null and self.voted_for != msg.candidate_id) {
                    log.debug("Vote not granted, already voted for {any}", .{self.voted_for});
                    break :blk false;
                }

                assert(self.state == .follower);

                self.setVotedFor(msg.candidate_id);

                log.debug("Vote granted, term: {}", .{self.current_term});
                break :blk true;
            };

            self.sendRequestVoteResponse(msg.candidate_id, vote_granted);

            self.setElectionTimeout(getTime());
        }

        fn handleRequestVoteResponse(self: *Self, msg: RequestVoteResponse) void {
            assert(msg.responder_id < self.config.server_no);
            assert(msg.responder_id != self.config.id);

            self.handleNewerTerm(msg.term);

            log.debug("Received RequestVoteResponse from {}, term: {}, vote_granted: {}", .{
                msg.responder_id,
                msg.term,
                msg.vote_granted,
            });

            if (msg.term < self.current_term) {
                log.debug("Ignoring RequestVoteResponse: stale message (current term = {})", .{self.current_term});
                return;
            }
            if (self.state != .candidate) {
                log.debug("Ignoring RequestVoteResponse: not a candidate", .{});
                return;
            }

            self.received_votes[msg.responder_id] = msg.vote_granted;

            var total_votes: u32 = 0;
            for (self.received_votes) |vote| {
                if (vote) total_votes += 1;
            }

            log.debug("Vote received: {}, votes: {}/{}", .{
                msg.vote_granted,
                total_votes,
                self.config.server_no,
            });

            if (total_votes >= self.config.server_no / 2 + 1) self.convertToLeader();
        }

        fn handleAppendEntries(self: *Self, msg: AppendEntries) void {
            assert(msg.leader_id < self.config.server_no);
            assert(msg.leader_id != self.config.id);

            self.handleNewerTerm(msg.term);

            log.debug("Received AppendEntries from {}, term: {}, pl_index: {}, pl_term: {}, l_commit: {}, entries number: {}", .{
                msg.leader_id,
                msg.term,
                msg.prev_log_index,
                msg.prev_log_term,
                msg.leader_commit,
                msg.entries.len,
            });

            const success = blk: {
                if (msg.term < self.current_term) {
                    log.debug("Ignoring AppendEntries: stale message (current term = {})", .{self.current_term});
                    break :blk false;
                }

                if (self.state == .candidate) {
                    log.debug("Received AppendEntries while being candidate", .{});
                    self.convertToFollower();
                }

                self.current_leader = msg.leader_id;

                // if prev_log_index == 0, this is the very first entry
                if (msg.prev_log_index != 0) {
                    // make sure the index and term of previous entry match
                    if (self.log.items.len < msg.prev_log_index) {
                        log.debug("Entries not appended: local log too short (last index = {})", .{self.log.items.len});
                        break :blk false;
                    }

                    // log indices start at 1!
                    const entry = self.log.items[msg.prev_log_index - 1];

                    if (entry.term != msg.prev_log_term) {
                        // make sure commited entries do not conflict
                        // otherwise, something went very wrong
                        assert(msg.prev_log_term > self.commit_index);

                        // remove conflicting entries
                        self.shrinkLog(msg.prev_log_index - 1);
                        log.debug("Entries not appended: conflicting entry at prev_index (local entry term  = {}), removing subsequent entries", .{entry.term});
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
                        self.appendLogEntry(entry);

                        log.info("Appended entry, term: {}, index: {}, entry: {}", .{ entry.term, self.log.items.len, entry.entry });
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
                        self.shrinkLog(after_entries);
                    }
                }
                // instead of doing that, we could remove entries in the for loop instead of the assert
                // but for that to work, we would need to wait for AE with index 11
                // now we remove the invalid entries right with the very first empty AE

                if (msg.leader_commit > self.commit_index) {
                    const new_commit_index = @min(msg.leader_commit, self.log.items.len);
                    log.debug("Commit index changed from {} to {}", .{ self.commit_index, new_commit_index });
                    self.commit_index = new_commit_index;
                    self.applyEntries();
                }

                break :blk true;
            };

            self.sendAppendEntriesResponse(msg.leader_id, success);

            self.setElectionTimeout(getTime());
        }

        fn handleAppendEntriesResponse(self: *Self, msg: AppendEntriesResponse) void {
            assert(msg.responder_id < self.config.server_no);
            assert(msg.responder_id != self.config.id);

            self.handleNewerTerm(msg.term);

            log.debug("Received AppendEntriesResponse from {}, term: {}, success: {}, next prev index: {}", .{
                msg.responder_id,
                msg.term,
                msg.success,
                msg.next_prev_index,
            });

            if (msg.term < self.current_term) {
                log.debug("Ignoring AppendEntriesResponse: stale message (current_term = {})", .{self.current_term});
                return;
            }
            if (self.state != .leader) {
                log.debug("Ignoring AppendEntriesResponse: not a leader", .{});
                return;
            }

            // FIXME: this makes it imposible to replicate the log on followers that rebooted but did not persist the log
            // as they will respond will next_prev_index == 0 (bc their log will be empty after reboot)
            // but our match_index will still indicate that the entries were replicated
            // but if we remove this condition, we won't be able to handle AER out of order
            // one idea is to resend AE with lowered next_index, but never decrement the match_index for that follower
            if (msg.next_prev_index < self.match_index[msg.responder_id]) {
                log.debug("Ignoring AppendEntriesResponse: stale message (match index = {})", .{self.match_index[msg.responder_id]});
                return;
            }

            if (!msg.success) {
                // only when we succeed the next_prev_index can be equal
                // to the last index, otherwise is must be smaller
                assert(msg.next_prev_index < self.log.items.len);
                // assuming correct operation, next_index should always decrease by 1
                assert(self.next_index[msg.responder_id] - 2 == msg.next_prev_index);

                self.next_index[msg.responder_id] = msg.next_prev_index + 1;
                log.debug("AppendEntries for {} failed, next index: {}", .{ msg.responder_id, self.next_index[msg.responder_id] });
                assert(self.next_index[msg.responder_id] > 0);

                self.sendAppendEntries(msg.responder_id);
                return;
            }

            assert(msg.next_prev_index <= self.log.items.len);

            self.next_index[msg.responder_id] = msg.next_prev_index + 1;
            self.match_index[msg.responder_id] = msg.next_prev_index;
            log.debug("AppendEntries for {} succeeded, next index: {}, match index: {}", .{
                msg.responder_id,
                self.next_index[msg.responder_id],
                self.match_index[msg.responder_id],
            });

            for ((self.commit_index + 1)..(msg.next_prev_index + 1)) |idx| {
                if (self.log.items[idx - 1].term == self.current_term) {
                    var votes: u32 = 0;
                    for (self.match_index) |repl_idx| {
                        if (repl_idx >= idx) votes += 1;
                    }

                    if (votes >= self.config.server_no / 2 + 1) {
                        log.debug("Commit index changed from {} to {}", .{ self.commit_index, idx });
                        self.commit_index = @intCast(idx);
                    }
                }
            }

            self.applyEntries();

            if (self.next_index[msg.responder_id] <= self.log.items.len) self.sendAppendEntries(msg.responder_id);
        }

        fn handleNewerTerm(self: *Self, msg_term: u32) void {
            if (msg_term > self.current_term) {
                log.debug("Received message with term {}, which is greater than current term {}", .{ msg_term, self.current_term });

                self.setCurrentTerm(msg_term);
                self.convertToFollower();
            }
        }

        fn sendRequestVoteResponse(self: *Self, to: u32, vote_granted: bool) void {
            const response = RequestVoteResponse{
                .term = self.current_term,
                .vote_granted = vote_granted,
                .responder_id = self.config.id,
            };

            const rpc = .{ .request_vote_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                log.warn("Sending RequestVoteResponse to server {}, failed: {}", .{ to, err });
                return;
            };

            log.debug("Sent RequestVoteResponse to {}, term: {}, vote_granted: {}", .{
                to,
                response.term,
                response.vote_granted,
            });
        }

        fn sendAppendEntriesResponse(self: *Self, to: u32, success: bool) void {
            const response = AppendEntriesResponse{
                .term = self.current_term,
                .success = success,
                // assuming we delete conflicting messages right away
                // this always will be correct value matching the definition at the begining of
                // the Raft struct
                .next_prev_index = @intCast(self.log.items.len),
                .responder_id = self.config.id,
            };

            const rpc = .{ .append_entries_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch |err| {
                log.warn("Sending AppendEntriesResponse to server {d}, failed: {any}", .{ to, err });
                return;
            };

            log.debug("Sent AppendEntriesResponse to {}, term: {}, success: {}, next prev index: {}", .{
                to,
                response.term,
                response.success,
                response.next_prev_index,
            });
        }

        fn setCurrentTerm(self: *Self, new_current_term: u32) void {
            self.current_term = new_current_term;
            self.callbacks.persistCurrentTerm(self.callbacks.user_data, new_current_term) catch |err| {
                std.debug.panic("Persisting current_term {} failed: {}", .{ new_current_term, err });
            };
        }

        fn setVotedFor(self: *Self, new_voted_for: ?u32) void {
            self.voted_for = new_voted_for;
            self.callbacks.persistVotedFor(self.callbacks.user_data, new_voted_for) catch |err| {
                std.debug.panic("Persisting voted_for {any} failed: {}", .{ new_voted_for, err });
            };
        }

        fn appendLogEntry(self: *Self, entry: LogEntry) void {
            self.log.append(entry) catch |err| {
                std.debug.panic("Appending entry to the in-memory log failed: {}", .{err});
            };

            self.callbacks.logAppend(self.callbacks.user_data, entry) catch |err| {
                std.debug.panic("Appending antry {} failed: {}", .{ entry, err });
            };
        }

        fn shrinkLog(self: *Self, new_len: usize) void {
            assert(new_len <= self.log.items.len);

            var len = self.log.items.len;
            while (len > new_len) : (len -= 1) {
                const entry = self.callbacks.logPop(self.callbacks.user_data) catch |err| {
                    std.debug.panic("Popping entry failed: {}", .{err});
                };

                assert(entry.term == self.log.items[len - 1].term);
            }

            self.log.shrinkRetainingCapacity(new_len);
        }

        fn applyEntries(self: *Self) void {
            while (self.commit_index > self.last_applied) {
                self.last_applied += 1;
                const entry = self.log.items[self.last_applied - 1].entry;
                self.callbacks.applyEntry(self.callbacks.user_data, entry) catch |err| {
                    std.debug.panic("Applying new entry {} failed: {}", .{ entry, err });
                };

                log.info("Applied entry with index {}", .{self.last_applied});
            }
        }

        fn setElectionTimeout(self: *Self, time: u64) void {
            const start = self.config.election_timeout;
            const stop = 2 * self.config.election_timeout;
            const interval = std.crypto.random.intRangeAtMost(u64, start, stop);
            self.timeout = time + interval;
        }

        fn setHeartbeatTimeout(self: *Self, time: u64) void {
            self.timeout = time + self.config.heartbeat_timeout;
        }
    };
}

fn getTime() u64 {
    const time_ms = std.time.milliTimestamp();
    return @intCast(time_ms);
}
