const std = @import("std");
const xev = @import("xev");
const zaft = @import("zaft.zig");
const network = @import("network.zig");

const addresses = [_][]const u8{ "127.0.0.1:10000", "127.0.0.1:10001", "127.0.0.1:10002" };

const UserData = struct {
    senders: []network.Sender,
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

    const senders = try allocator.alloc(network.Sender, addresses.len);
    defer allocator.free(senders);

    for (addresses, 0..) |address, id| {
        if (id == self_id) {
            senders[id] = undefined;
        } else {
            const sender = try network.Sender.init(address, &loop, allocator);
            senders[id] = sender;
        }
    }

    var user_data = UserData{ .senders = senders };
    const callbacks = zaft.Callbacks(UserData){
        .user_data = &user_data,
        .makeRPC = makeRPC,
    };
    const raft = zaft.Raft(UserData).init(self_id, callbacks);

    var receiver = try network.Receiver.init(addresses[self_id], &loop, &raft, allocator);
    try receiver.listen();

    // const timer = try xev.Timer.init();
    // defer timer.deinit();
    //
    // var completion: xev.Completion = undefined;
    // timer.run(&loop, &completion, 5000, Siema, &siema, timerCallback);

    try loop.run(.until_done);
}

fn makeRPC(user_data: *UserData, id: u32, rpc: zaft.RPC) !void {
    _ = user_data;
    _ = rpc;
    std.debug.print("MAKING RPC to {d}...\n", .{id});
}

// fn timerCallback(
//     siema: ?*Siema,
//     loop: *xev.Loop,
//     c: *xev.Completion,
//     result: xev.Timer.RunError!void,
// ) xev.CallbackAction {
//     for (siema.?.senders, 0..) |*sender, id| {
//         if (id == siema.?.self_id) continue;
//
//         sender.send("{\"hello\":5,\"hi\":\"yooooo\"}") catch |err| {
//             std.debug.print("ERROR sending: {any}\n", .{err});
//         };
//     }
//
//     _ = loop;
//     _ = c;
//     _ = result catch unreachable;
//     std.debug.print("hello\n", .{});
//     return .disarm;
// }
