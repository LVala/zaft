const std = @import("std");

const RequestVote = struct {
    term: u32,
    candidate_id: u32,
};

const RequestVoteResult = struct {
    term: u32,
    vote_granted: bool,
};

const AppendEntries = struct {
    term: u32,
    leader_id: u32,
};

const AppendEntriesResult = struct {
    term: u32,
    success: bool,
};

const RPC = union(enum) {
    append_entries: AppendEntries,
    request_vote: RequestVote,
};

const RPCResult = union(enum) {
    append_entries: AppendEntriesResult,
    request_vote: RequestVoteResult,
};

pub fn Config(UserData: type) type {
    return struct {
        user_data: *UserData,
        makeRPC: *const fn (user_data: *UserData, node_id: u32, rpc: RPC) anyerror!void,
    };
}

const Raft = struct {
    current_term: u32 = 0,
    voted_for: ?u32 = null,
    id: u32,

    const Self = @This();

    pub fn init() Self {
        return Self{ .id = 5 };
    }

    pub fn tick() void {
        std.debug.print("Ticked!\n", .{});
    }
};
