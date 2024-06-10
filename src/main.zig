const std = @import("std");
const xev = @import("xev");
const zaft = @import("zaft.zig");
const network = @import("network.zig");

const addresses = [_][]const u8{ "127.0.0.1:10000", "127.0.0.1:10001", "127.0.0.1:10002" };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const self_id = try std.fmt.parseInt(u32, args.next().?, 10);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var receiver = try network.Receiver.init(addresses[self_id], &loop, allocator);
    try receiver.listen();

    const senders = try allocator.alloc(network.Sender, addresses.len);
    for (addresses, 0..) |address, id| {
        if (id == self_id) {
            senders[id] = undefined;
        } else {
            const sender = try network.Sender.init(address, &loop, allocator);
            senders[id] = sender;
        }
    }

    try loop.run(.until_done);
}
