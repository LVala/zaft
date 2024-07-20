const std = @import("std");
const zaft = @import("zaft");
const server = @import("server.zig");
const Ticker = @import("ticker.zig").Ticker;
const Storage = @import("storage.zig").Storage;

const ClientServer = server.ClientServer;
const RaftServer = server.RaftServer;

pub const Add = struct {
    key: []const u8,
    value: []const u8,
};

pub const Remove = struct {
    key: []const u8,
};

pub const Entry = union(enum) {
    add: Add,
    remove: Remove,
};

const UserData = struct {
    allocator: std.mem.Allocator,
    addresses: []const []const u8,
    store: *std.StringHashMap([]const u8),
    cond: *std.Thread.Condition,
    storage: *Storage,
};

pub const Raft = zaft.Raft(UserData, Entry);

// TODO: make these configurable
const raft_addresses = [_][]const u8{ "127.0.0.1:10000", "127.0.0.1:10001", "127.0.0.1:10002" };
const client_addresses = [_][]const u8{ "127.0.0.1:20000", "127.0.0.1:20001", "127.0.0.1:20002" };
const cache_name = "zaft-cache";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const self_id = try std.fmt.parseInt(usize, args.next().?, 10);

    const raft_http_addresses = try allocator.alloc([]u8, raft_addresses.len);
    defer allocator.free(raft_http_addresses);

    const client_http_addresses = try allocator.alloc([]u8, client_addresses.len);
    defer allocator.free(client_http_addresses);

    for (raft_http_addresses, raft_addresses) |*address, raw_address| {
        address.* = try std.fmt.allocPrint(allocator, "http://{s}/", .{raw_address});
    }

    for (client_http_addresses, client_addresses) |*address, raw_address| {
        address.* = try std.fmt.allocPrint(allocator, "http://{s}/", .{raw_address});
    }

    var mutex = std.Thread.Mutex{};
    var cond = std.Thread.Condition{};

    var store = std.StringHashMap([]const u8).init(allocator);
    defer store.deinit();

    var buffer: [128]u8 = undefined;
    const cache_dir_name = try std.fmt.bufPrint(&buffer, "{s}-{}", .{ cache_name, self_id });
    var storage = try Storage.init(cache_dir_name);
    defer storage.deinit();

    const readLog = try storage.readLog(allocator);
    defer allocator.free(readLog);

    const config = Raft.Config{ .id = @intCast(self_id), .server_no = @intCast(raft_addresses.len) };
    const initial_state = Raft.InitialState{
        .current_term = try storage.readCurrentTerm(),
        .voted_for = try storage.readVotedFor(),
        .log = readLog,
    };
    var user_data = UserData{
        .allocator = allocator,
        .addresses = raft_http_addresses,
        .store = &store,
        .cond = &cond,
        .storage = &storage,
    };
    const callbacks = Raft.Callbacks{
        .user_data = &user_data,
        .makeRPC = makeRPC,
        .applyEntry = applyEntry,
        .logAppend = logAppend,
        .logPop = logPop,
        .persistCurrentTerm = persistCurrentTerm,
        .persistVotedFor = persistVotedFor,
    };
    var raft = try Raft.init(config, initial_state, callbacks, allocator);

    var ticker = Ticker{ .raft = &raft, .mutex = &mutex };
    const ticker_thread = try std.Thread.spawn(.{}, Ticker.run, .{&ticker});
    ticker_thread.detach();

    const raft_address = try parseAddress(raft_addresses[self_id]);
    var raft_server = RaftServer{
        .raft = &raft,
        .mutex = &mutex,
        .address = raft_address,
        .allocator = allocator,
    };
    const raft_thread = try std.Thread.spawn(.{}, RaftServer.run, .{&raft_server});
    raft_thread.detach();

    const client_address = try parseAddress(client_addresses[self_id]);
    var client_server = ClientServer{
        .raft = &raft,
        .cond = &cond,
        .mutex = &mutex,
        .store = &store,
        .address = client_address,
        .addresses = client_http_addresses,
        .allocator = allocator,
    };
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

    request.transfer_encoding = .chunked;

    try request.send();
    try request.writeAll(json);
    try request.finish();
    // we don't wait for the response, this function has to be non-blocking
}

fn applyEntry(user_data: *UserData, entry: Entry) !void {
    switch (entry) {
        .remove => |ent| {
            // we do not free the memory for the key/value
            // as it still is used by the entry in the log
            _ = user_data.store.remove(ent.key);
        },
        .add => |ent| {
            try user_data.store.put(ent.key, ent.value);
        },
    }

    user_data.cond.signal();
}

fn logAppend(ud: *UserData, log_entry: Raft.LogEntry) !void {
    try ud.storage.appendLog(log_entry, ud.allocator);
}

fn logPop(ud: *UserData) !Raft.LogEntry {
    return ud.storage.popLog(ud.allocator);
}

fn persistVotedFor(ud: *UserData, voted_for: ?u32) !void {
    try ud.storage.writeVotedFor(voted_for);
}

fn persistCurrentTerm(ud: *UserData, current_term: u32) !void {
    try ud.storage.writeCurrentTerm(current_term);
}

fn parseAddress(address: []const u8) !std.net.Address {
    var it = std.mem.splitSequence(u8, address, ":");

    const ip = it.first();
    const port = try std.fmt.parseInt(u16, it.rest(), 10);

    return std.net.Address.parseIp4(ip, port);
}
