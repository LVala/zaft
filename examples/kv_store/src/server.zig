const std = @import("std");
const http = std.http;
const common = @import("main.zig");

pub const RaftServer = struct {
    address: std.net.Address,
    raft: *common.Raft,
    mutex: *std.Thread.Mutex,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn run(self: *Self) !void {
        var buffer: [1024]u8 = undefined;
        var net_server = try self.address.listen(.{});

        std.log.info("Listening for Raft RPCs on {any}\n", .{self.address});

        while (true) {
            const conn = try net_server.accept();
            defer conn.stream.close();

            var http_server = http.Server.init(conn, &buffer);
            var request = try http_server.receiveHead();
            self.handleRequest(&request) catch |err| {
                std.log.warn("Unagle to handle request {}, reason: {}\n", .{ request.head, err });
            };
        }
    }

    fn handleRequest(self: *Self, request: *http.Server.Request) !void {
        var buffer: [1024]u8 = undefined;
        const body = try read_body(request, &buffer);

        const json = std.json.parseFromSlice(common.Raft.RPC, self.allocator, body, .{ .allocate = .alloc_always }) catch |err| {
            std.debug.print("Failed to parse received JSON message from {s}: {any}\n", .{ body, err });
            try request.respond("", .{ .status = .not_acceptable });
            return;
        };
        // defer json.deinit();
        // TODO: we need to clean up the Entry memory somewhere

        try request.respond("", .{});

        self.mutex.lock();
        defer self.mutex.unlock();

        self.raft.handleRPC(json.value);
    }
};

pub const ClientServer = struct {
    address: std.net.Address,
    addresses: []const []const u8,
    raft: *common.Raft,
    mutex: *std.Thread.Mutex,
    cond: *std.Thread.Condition,
    allocator: std.mem.Allocator,
    store: *std.StringHashMap([]const u8),

    const Self = @This();

    pub fn run(self: *Self) !void {
        var buffer: [1024]u8 = undefined;
        var net_server = try self.address.listen(.{ .reuse_address = true });

        std.log.info("Listening for Client requests on {any}\n", .{self.address});

        while (true) {
            const conn = try net_server.accept();
            defer conn.stream.close();

            var http_server = http.Server.init(conn, &buffer);
            var request = try http_server.receiveHead();
            self.handleRequest(&request) catch |err| {
                std.log.warn("Unagle to handle request {}, reason: {}\n", .{ request.head, err });
            };
        }
    }

    fn handleRequest(self: *Self, request: *http.Server.Request) !void {
        var buffer: [1024]u8 = undefined;
        const body = try read_body(request, &buffer);

        self.mutex.lock();
        defer self.mutex.unlock();

        try self.checkValid(request);

        // TODO: we need to clean up the Entry memory somewhere
        const entry = switch (request.head.method) {
            .POST => blk: {
                const value = try self.allocator.alloc(u8, body.len);
                const key = try self.allocator.alloc(u8, request.head.target.len);
                @memcpy(value, body);
                @memcpy(key, request.head.target);

                break :blk common.Entry{ .add = common.Add{ .key = key, .value = value } };
            },
            .DELETE => blk: {
                const key = try self.allocator.alloc(u8, request.head.target.len);
                @memcpy(key, request.head.target);

                break :blk common.Entry{ .remove = common.Remove{ .key = key } };
            },
            .GET => {
                if (self.store.get(request.head.target)) |value| {
                    try request.respond(value, .{ .status = .ok });
                } else {
                    try request.respond("", .{ .status = .not_found });
                }
                return;
            },
            else => {
                try request.respond("", .{ .status = .bad_request });
                return;
            },
        };

        // TODO: this probably should eventually timeout and send response
        const idx = try self.raft.appendEntry(entry);
        while (!self.raft.checkIfApplied(idx)) {
            self.cond.wait(self.mutex);
        }

        try request.respond("", .{ .status = .ok });
    }

    fn checkValid(self: *Self, request: *http.Server.Request) !void {
        const cur_leader = self.raft.getCurrentLeader();
        const not_leader = cur_leader != self.raft.id;
        const for_leader = request.head.method == .POST or request.head.method == .DELETE;

        if (for_leader and not_leader) {
            if (cur_leader) |idx| {
                try request.respond("", .{
                    .status = .permanent_redirect,
                    .extra_headers = &.{
                        http.Header{ .name = "location", .value = self.addresses[idx] },
                    },
                });
            } else {
                try request.respond("", .{ .status = .too_early });
            }

            return error.NotALeader;
        }
    }
};

fn read_body(request: *http.Server.Request, buffer: []u8) ![]u8 {
    const reader = try request.reader();
    const len = try reader.read(buffer);

    var temp_buf: [10]u8 = undefined;
    const rest_len = try reader.read(&temp_buf);
    if (rest_len != 0) {
        try request.respond("", .{ .status = .payload_too_large });
        return error.PayloadTooLarge;
    }

    return buffer[0..len];
}
