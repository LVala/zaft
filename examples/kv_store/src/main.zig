const std = @import("std");
const zaft = @import("zaft");
const server = @import("server.zig");
const Ticker = @import("ticker.zig").Ticker;

const ClientServer = server.ClientServer;
const RaftServer = server.RaftServer;

pub const Entry = struct {};
const UserData = struct {
    allocator: std.mem.Allocator,
    addresses: []const []const u8,
    cond: *std.Thread.Condition,
};

pub const Raft = zaft.Raft(UserData, Entry);

const raft_addresses = [_][]const u8{ "127.0.0.1:10000", "127.0.0.1:10001", "127.0.0.1:10002" };
const client_addresses = [_][]const u8{ "127.0.0.1:20000", "127.0.0.1:20001", "127.0.0.1:20002" };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const self_id = try std.fmt.parseInt(usize, args.next().?, 10);

    var mutex = std.Thread.Mutex{};
    var cond = std.Thread.Condition{};

    const addresses = try allocator.alloc([]u8, raft_addresses.len);
    defer allocator.free(addresses);

    for (addresses, raft_addresses) |*address, raw_address| {
        address.* = try std.fmt.allocPrint(allocator, "http://{s}/", .{raw_address});
    }

    var user_data = UserData{
        .allocator = allocator,
        .addresses = addresses,
        .cond = &cond,
    };
    const callbacks = Raft.Callbacks{
        .user_data = &user_data,
        .makeRPC = makeRPC,
        .applyEntry = applyEntry,
    };
    var raft = try Raft.init(@intCast(self_id), @intCast(raft_addresses.len), callbacks, allocator);

    var ticker = Ticker.init(&raft, &mutex);
    try ticker.run_in_thread();

    const raft_address = try parseAddress(raft_addresses[self_id]);
    var raft_server = try RaftServer.init(raft_address, &raft, &mutex, &cond, allocator);
    try raft_server.run_in_thread();

    const client_address = try parseAddress(client_addresses[self_id]);
    var client_server = try ClientServer.init(client_address, &raft, &mutex, &cond, allocator);
    try client_server.run();
}

fn makeRPC(user_data: *UserData, id: u32, rpc: Raft.RPC) !void {
    const address = user_data.addresses[id];
    const uri = try std.Uri.parse(address);

    var client = std.http.Client{ .allocator = user_data.allocator };
    defer client.deinit();

    var buffer: [1024]u8 = undefined;
    var request = try client.open(.POST, uri, .{ .server_header_buffer = &buffer });
    defer request.deinit();

    const json = try std.json.stringifyAlloc(user_data.allocator, rpc, .{});
    defer user_data.allocator.free(json);

    request.transfer_encoding = .{ .content_length = json.len };

    try request.send();
    try request.writeAll(json);
    try request.finish();
    // we don't wait for the response, this function has to be non-blocking
}

fn applyEntry(user_data: *UserData, entry: Entry) !void {
    _ = entry;

    user_data.cond.signal();
}

fn parseAddress(address: []const u8) !std.net.Address {
    var it = std.mem.splitSequence(u8, address, ":");

    const ip = it.first();
    const port = try std.fmt.parseInt(u16, it.rest(), 10);

    return std.net.Address.parseIp4(ip, port);
}
