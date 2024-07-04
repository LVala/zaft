const std = @import("std");
const store = @import("store.zig");

const addresses = [_][]const u8{ "127.0.0.1:10000", "127.0.0.1:10001" };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const self_id = try std.fmt.parseInt(usize, args.next().?, 10);

    try store.run(&addresses, self_id, allocator);
}
