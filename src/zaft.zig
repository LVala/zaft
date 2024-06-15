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

        callbacks: Callbacks(UserData),
        id: u32,
        server_no: u32,

        current_term: u32 = 0,
        voted_for: ?u32 = null,

        state: State,
        timeout: u64,
        votes: u32 = 0,

        pub fn init(id: u32, server_no: u32, callbacks: Callbacks(UserData)) Self {
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
                return self.timeout - time;
            }

            self.timeout = switch (self.state) {
                .leader => blk: {
                    self.sendHeartbeat();
                    break :blk newHeartbeatTimeout(time);
                },
                else => blk: {
                    self.convertToCandidate();
                    // TODO: when entering the leader state
                    // the timeout between first and second heartbeat
                    // because of the last returned value when still
                    // in the candidate state
                    break :blk newElectionTimeout(time);
                },
            };

            return self.timeout - time;
        }

        fn sendHeartbeat(self: *Self) void {
            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                const request_vote = AppendEntries{
                    .term = self.current_term,
                    .leader_id = self.id,
                };
                const rpc = .{ .append_entries = request_vote };

                self.callbacks.makeRPC(self.callbacks.user_data, @intCast(idx), rpc) catch |err| {
                    std.debug.print("makeRPC failed: {}\n", .{err});
                };
            }
        }

        fn convertToCandidate(self: *Self) void {
            std.debug.print("Converted to candidate\n", .{});
            self.current_term += 1;
            self.state = .candidate;
            self.voted_for = self.id;
            self.votes = 1;

            for (0..self.server_no) |idx| {
                if (idx == self.id) continue;

                const request_vote = RequestVote{
                    .term = self.current_term,
                    .candidate_id = self.id,
                };
                const rpc = .{ .request_vote = request_vote };

                self.callbacks.makeRPC(self.callbacks.user_data, @intCast(idx), rpc) catch |err| {
                    std.debug.print("makeRPC failed: {}\n", .{err});
                };
            }
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
            std.debug.print("Received request vote from {d}\n", .{msg.candidate_id});
            const vote_granted = blk: {
                if (msg.term < self.current_term) {
                    break :blk false;
                }

                // TODO: also check if log is up-to-data
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

            std.debug.print("Granting vote to {d}: {any}\n", .{ msg.candidate_id, vote_granted });

            const rpc = .{ .request_vote_response = response };
            self.callbacks.makeRPC(self.callbacks.user_data, msg.candidate_id, rpc) catch |err| {
                std.debug.print("makeRPC failed: {}\n", .{err});
            };

            self.timeout = newElectionTimeout(getTime());
        }

        fn handleRequestVoteResponse(self: *Self, msg: RequestVoteResponse) void {
            std.debug.print("Received {any} request vote response\n", .{msg.vote_granted});

            // TODO: probably should verify who is the response from to ignore duplicates
            if (self.state != .candidate) return;
            if (msg.vote_granted) self.votes += 1;

            std.debug.print("Votes {d}, servers {d}\n", .{ self.votes, self.server_no });

            if (self.votes >= self.server_no / 2 + 1) {
                // we received the majority of the votes
                self.state = .leader;
                self.sendHeartbeat();
                self.timeout = newHeartbeatTimeout(getTime());
                std.debug.print("WE ARE LEADER\n", .{});
            }
        }

        fn handleAppendEntries(self: *Self, msg: AppendEntries) void {
            std.debug.print("Received AppendEntries\n", .{});
            self.state = .follower;
            self.timeout = newElectionTimeout(getTime());
            _ = msg;
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
            // TODO
            return time + 500;
        }
    };
}

fn getTime() u64 {
    const time_ms = std.time.milliTimestamp();
    return @intCast(time_ms);
}
