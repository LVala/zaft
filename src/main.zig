const std = @import("std");
const xev = @import("xev");
const zaft = @import("zaft.zig");
const RPC = @import("rpc.zig").RPC;

const addresses = [_][]const u8{ "127.0.0.1:10000", "127.0.0.1:10001", "127.0.0.1:10002" };

const UserData = struct {
    rpcs: []RPC,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const self_id = try std.fmt.parseInt(u32, args.next().?, 10);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const rpcs = try allocator.alloc(RPC, addresses.len);
    for (addresses, 0..) |address, id| {
        if (id == self_id) {
            rpcs[id] = undefined;
        } else {
            const rpc = try RPC.init(address, &loop, allocator);
            rpcs[id] = rpc;
        }
    }

    const self_address = try std.net.Address.parseIp4("127.0.0.1", 10000);
    const server = try xev.TCP.init(self_address);
    try server.bind(self_address);
    try server.listen(1);

    var read_buffer: [1024]u8 = undefined;
    var siema: []u8 = &read_buffer;
    var c_accept: xev.Completion = undefined;
    server.accept(&loop, &c_accept, []u8, &siema, acceptCallback);

    // const w = try xev.Timer.init();
    // defer w.deinit();
    //
    // var completion: xev.Completion = undefined;
    // w.run(&loop, &completion, 1000, RPC, &rpc, &timerCallback);

    try loop.run(.until_done);
}

fn acceptCallback(_: ?*[]u8, _: *xev.Loop, _: *xev.Completion, result: xev.TCP.AcceptError!xev.TCP) xev.CallbackAction {
    _ = result catch |err| {
        std.debug.print("Accepting connection failed: {any}", err);
        return .rearm;
    };

    // server.read();
    //
    return .disarm;
}

fn timerCallback(
    rpc: ?*RPC,
    loop: *xev.Loop,
    completion: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = result catch unreachable;
    // QUESTION: is this the right thing to do?
    // .rearm runs the callback instantly, withouth the delay
    const w = try xev.Timer.init();
    defer w.deinit();

    w.run(loop, completion, 1000, RPC, rpc, &timerCallback);

    rpc.?.call("siemanko\n") catch unreachable;

    return .disarm;
}
