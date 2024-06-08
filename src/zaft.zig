const std = @import("std");

const RequestVote = struct {
    term: u32,
    candidate_id: u32,
    lastLogIndex: u32,
    lastLogTerm: u32,
};

pub fn Config(UserData: type) type {
    return struct {
        user_data: *UserData,
        sendRequestVote: *const fn (user_data: *UserData, node_id: u32, msg: RequestVote) anyerror!void,
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
