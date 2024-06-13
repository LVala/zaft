const std = @import("std");

pub const RequestVote = struct {
    term: u32,
    candidate_id: u32,
};

pub const AppendEntries = struct {
    term: u32,
    leader_id: u32,
};

pub const RPC = union(enum) {
    append_entries: AppendEntries,
    request_vote: RequestVote,
};

pub fn Callbacks(UserData: type) type {
    return struct {
        user_data: *UserData,
        makeRPC: *const fn (user_data: *UserData, id: u32, rpc: RPC) anyerror!void,
    };
}

pub fn Raft(UserData: type) type {
    return struct {
        callbacks: Callbacks(UserData),
        current_term: u32 = 0,
        voted_for: ?u32 = null,
        id: u32,

        const Self = @This();

        pub fn init(id: u32, callbacks: Callbacks(UserData)) Self {
            return Self{
                .callbacks = callbacks,
                .id = id,
            };
        }

        pub fn tick(self: *Self) void {
            std.debug.print("Ticked!\n", .{});

            const rpc = RPC{ .request_vote = RequestVote{ .term = 500, .candidate_id = 100 } };
            const to: u32 = if (self.id == 1) 0 else 1;
            self.callbacks.makeRPC(self.callbacks.user_data, to, rpc) catch unreachable;
        }

        pub fn handleRPC(self: *Self, rpc: RPC) void {
            _ = self;
            std.debug.print("Handled {any}!\n", .{rpc});
        }
    };
}
