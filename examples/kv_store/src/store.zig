const std = @import("std");
const xev = @import("xev");
const zaft = @import("zaft");
const network = @import("network.zig");

const Add = struct {
    key: []const u8,
    value: []const u8,
};

const Get = struct {
    key: []const u8,
};

const Remove = struct {
    key: []const u8,
};

const Entry = union(enum) {
    add: Add,
    get: Get,
    remove: Remove,
};

const UserData = struct {
    senders: []Sender,
};

const Raft = zaft.Raft(UserData, Entry);
const Receiver = network.Receiver(Raft);
const Sender = network.Sender(Raft);

pub fn run(addresses: []const []const u8, self_id: usize, allocator: std.mem.Allocator) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const senders = try allocator.alloc(Sender, addresses.len);
    defer allocator.free(senders);

    for (addresses, 0..) |address, id| {
        if (id == self_id) {
            senders[id] = undefined;
        } else {
            const sender = try Sender.init(address, &loop, allocator);
            senders[id] = sender;
        }
    }

    var user_data = UserData{ .senders = senders };
    const callbacks = Raft.Callbacks{
        .user_data = &user_data,
        .makeRPC = makeRPC,
    };
    var raft = try Raft.init(@intCast(self_id), @intCast(addresses.len), callbacks, allocator);

    var receiver = try Receiver.init(addresses[self_id], &loop, &raft, allocator);
    try receiver.listen();

    const timer = try xev.Timer.init();
    defer timer.deinit();

    var completion: xev.Completion = undefined;
    timer.run(&loop, &completion, 0, Raft, &raft, timerCallback);

    try loop.run(.until_done);
}

fn makeRPC(user_data: *UserData, id: u32, rpc: Raft.RPC) !void {
    try user_data.senders[id].send(rpc);
}

fn timerCallback(
    raft: ?*Raft,
    loop: *xev.Loop,
    completion: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = result catch unreachable;

    const next_ms = raft.?.tick();

    const timer = try xev.Timer.init();
    defer timer.deinit();

    timer.run(loop, completion, next_ms, Raft, raft.?, timerCallback);
    return .disarm;
}
