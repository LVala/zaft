const std = @import("std");
const zaft = @import("zaft");

const testing = std.testing;

const len = 5;

pub const TestRaft = zaft.Raft(anyopaque, u32);
pub const TestUserData = struct {
    rpcs: [len]TestRaft.RPC = undefined,
    persisted_voted_for: ?u32 = null,
    persisted_current_term: u32 = 0,
    allocator: std.mem.Allocator,
    persisted_log: std.ArrayList(TestRaft.LogEntry),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .persisted_log = std.ArrayList(TestRaft.LogEntry).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.persisted_log.deinit();
    }
};

pub fn getTime() u64 {
    const time_ms = std.time.milliTimestamp();
    return @intCast(time_ms);
}

pub fn appendTestEntry(raft: *TestRaft, entry: TestRaft.LogEntry) !void {
    try raft.log.append(entry);
    const ud = @as(*TestUserData, @alignCast(@ptrCast(raft.callbacks.user_data)));
    try ud.persisted_log.append(entry);
}

pub fn testPersistedMatch(raft: *TestRaft) !void {
    const ud = @as(*TestUserData, @alignCast(@ptrCast(raft.callbacks.user_data)));
    try testing.expect(ud.persisted_current_term == raft.current_term);
    try testing.expect(ud.persisted_voted_for == raft.voted_for);

    try testing.expect(ud.persisted_log.items.len == raft.log.items.len);

    for (ud.persisted_log.items, raft.log.items) |raft_entry, persisted_entry| {
        try testing.expect(raft_entry.term == persisted_entry.term);
        try testing.expect(raft_entry.entry == persisted_entry.entry);
    }
}

pub fn setupTestRaft(user_data: *TestUserData, elect_leader: bool) TestRaft {
    // our Raft always gets the id 0
    const config = TestRaft.Config{ .id = 0, .server_no = len };
    const initial_state = TestRaft.InitialState{};

    const callbacks = TestRaft.Callbacks{
        .user_data = user_data,
        .makeRPC = makeRPC,
        .applyEntry = applyEntry,
        .logAppend = logAppend,
        .logPop = logPop,
        .persistVotedFor = persistVotedFor,
        .persistCurrentTerm = persistCurrentTerm,
    };

    var raft = TestRaft.init(config, initial_state, callbacks, std.testing.allocator) catch unreachable;

    if (!elect_leader) return raft;

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    for (1..len) |id| {
        const rvr = TestRaft.RequestVoteResponse{
            .term = raft.current_term,
            .vote_granted = true,
            .responder_id = @intCast(id),
        };
        raft.handleRPC(.{ .request_vote_response = rvr });
    }

    return raft;
}

fn makeRPC(ud: *anyopaque, id: u32, rpc: TestRaft.RPC) !void {
    // TODO: I don't know what I did here
    const casted_ud = @as(*TestUserData, @alignCast(@ptrCast(ud)));
    casted_ud.rpcs[id] = rpc;
}

fn applyEntry(ud: *anyopaque, entry: u32) !void {
    _ = ud;
    _ = entry;
}

fn logAppend(ud: *anyopaque, log_entry: TestRaft.LogEntry) !void {
    const casted_ud = @as(*TestUserData, @alignCast(@ptrCast(ud)));
    try casted_ud.persisted_log.append(log_entry);
}

fn logPop(ud: *anyopaque) !TestRaft.LogEntry {
    const casted_ud = @as(*TestUserData, @alignCast(@ptrCast(ud)));
    return casted_ud.persisted_log.pop();
}

fn persistVotedFor(ud: *anyopaque, voted_for: ?u32) !void {
    const casted_ud = @as(*TestUserData, @alignCast(@ptrCast(ud)));
    casted_ud.persisted_voted_for = voted_for;
}

fn persistCurrentTerm(ud: *anyopaque, current_term: u32) !void {
    const casted_ud = @as(*TestUserData, @alignCast(@ptrCast(ud)));
    casted_ud.persisted_current_term = current_term;
}
