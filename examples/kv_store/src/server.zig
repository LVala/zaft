const std = @import("std");
const http = std.http;
const main = @import("main.zig");

const Raft = main.Raft;
const Entry = main.Entry;

pub const ClientServer = Server(.client);
pub const RaftServer = Server(.raft);

const ServerType = enum { raft, client };

fn Server(server_type: ServerType) type {
    return struct {
        address: std.net.Address,
        raft: *Raft,
        mutex: *std.Thread.Mutex,
        cond: *std.Thread.Condition,
        allocator: std.mem.Allocator,
        read_buffer: [1024]u8 = undefined,

        const Self = @This();
        const handleRequest = switch (server_type) {
            .raft => Self.handleRaftRequest,
            .client => Self.handleClientRequest,
        };

        pub fn init(address: std.net.Address, raft: *Raft, mutex: *std.Thread.Mutex, cond: *std.Thread.Condition, allocator: std.mem.Allocator) !Self {
            return Self{
                .address = address,
                .raft = raft,
                .mutex = mutex,
                .cond = cond,
                .allocator = allocator,
            };
        }

        pub fn run_in_thread(self: *Self) !void {
            const thread = try std.Thread.spawn(.{}, do_run_thread, .{self});
            thread.detach();
        }

        fn do_run_thread(self: *Self) void {
            self.run() catch |err| {
                std.log.err("Server failed with reason: {any}\n", .{err});
            };
        }

        pub fn run(self: *Self) !void {
            var net_server = try self.address.listen(.{});

            std.log.info("Listening on {any}\n", .{self.address});

            while (true) {
                const conn = try net_server.accept();
                defer conn.stream.close();

                var http_server = http.Server.init(conn, &self.read_buffer);
                var request = try http_server.receiveHead();
                try self.handleRequest(&request);
            }
        }

        fn handleRaftRequest(self: *Self, request: *http.Server.Request) !void {
            const reader = try request.reader();
            const len = try reader.read(&self.read_buffer);
            const body = self.read_buffer[0..len];

            const json = std.json.parseFromSlice(Raft.RPC, self.allocator, body, .{ .allocate = .alloc_always }) catch |err| {
                std.debug.print("Failed to parse received JSON message: {any}\n", .{err});
                try request.respond("", .{ .status = .bad_request });
                return;
            };
            defer json.deinit();

            try request.respond("", .{});

            self.mutex.lock();
            defer self.mutex.unlock();

            self.raft.handleRPC(json.value);
        }

        fn handleClientRequest(self: *Self, request: *http.Server.Request) !void {
            // TODO: redirect to the leader
            // TODO: this probably should eventually timeout and send response
            self.mutex.lock();
            const idx = try self.raft.appendEntry(Entry{});
            while (!self.raft.checkIfApplied(idx)) {
                self.cond.wait(self.mutex);
            }
            self.mutex.unlock();

            try request.respond("Dupa", .{});
        }
    };
}
