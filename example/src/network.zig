const std = @import("std");
const xev = @import("xev");
const zaft = @import("zaft.zig");

pub fn Receiver(UserData: type) type {
    return struct {
        address: std.net.Address,
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        server: xev.TCP,
        completion: xev.Completion = undefined,
        raft: *zaft.Raft(UserData),

        const Self = @This();
        const buffer_size = 1024;

        pub fn init(raw_address: []const u8, loop: *xev.Loop, raft: *zaft.Raft(UserData), allocator: std.mem.Allocator) !Self {
            const address = try parseAddress(raw_address);
            const server = try xev.TCP.init(address);

            return Self{
                .address = address,
                .allocator = allocator,
                .loop = loop,
                .server = server,
                .raft = raft,
            };
        }

        pub fn listen(self: *Self) !void {
            try self.server.bind(self.address);
            try self.server.listen(1);
            self.server.accept(self.loop, &self.completion, Self, self, Self.acceptCallback);

            std.debug.print("Listening on {}\n", .{self.address});
        }

        fn acceptCallback(self: ?*Self, loop: *xev.Loop, _: *xev.Completion, result: xev.TCP.AcceptError!xev.TCP) xev.CallbackAction {
            const server = result catch |err| {
                std.debug.print("Accepting connection on {any} failed: {any}\n", .{ self.?.address, err });
                return .rearm;
            };

            // TODO: handle errors nicely
            const completion = self.?.allocator.create(xev.Completion) catch unreachable;
            const buffer = self.?.allocator.alloc(u8, buffer_size) catch unreachable;

            server.read(loop, completion, .{ .slice = buffer }, Self, self, readCallback);

            return .rearm;
        }

        fn readCallback(self: ?*Self, _: *xev.Loop, completion: *xev.Completion, _: xev.TCP, buffer: xev.ReadBuffer, result: xev.ReadError!usize) xev.CallbackAction {
            const len = result catch |err| {
                std.debug.print("Reading on {any} failed: {any}\n", .{ self.?.address, err });
                self.?.allocator.free(buffer.slice);
                self.?.allocator.destroy(completion);
                return .disarm;
            };

            const json = std.json.parseFromSlice(zaft.RPC, self.?.allocator, buffer.slice[0..len], .{ .allocate = .alloc_always }) catch |err| {
                std.debug.print("Failed to parse received JSON message: {any}", .{err});
                return .rearm;
            };
            defer json.deinit();

            self.?.raft.handleRPC(json.value);

            return .rearm;
        }
    };
}

pub const Sender = struct {
    address: std.net.Address,
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    client: xev.TCP,
    completion: xev.Completion = undefined,
    state: State = .disconnected,
    send_buffer: []const u8 = undefined,

    const State = enum {
        connected,
        connecting,
        disconnected,
    };

    const Self = @This();

    pub fn init(raw_address: []const u8, loop: *xev.Loop, allocator: std.mem.Allocator) !Self {
        const address = try parseAddress(raw_address);
        const client = try xev.TCP.init(address);

        return Self{
            .address = address,
            .allocator = allocator,
            .loop = loop,
            .client = client,
        };
    }

    pub fn send(self: *Self, rpc: zaft.RPC) !void {
        self.send_buffer = try std.json.stringifyAlloc(self.allocator, rpc, .{});

        switch (self.state) {
            .connected => {
                self.write();
            },
            .disconnected => {
                self.connect();
            },
            .connecting => {
                // TODO: maybe buffer calls when socket is in the connecting state?
            },
        }
    }

    fn connect(self: *Self) void {
        self.client.connect(self.loop, &self.completion, self.address, Self, self, Self.connectCallback);
    }

    fn write(self: *Self) void {
        self.client.write(self.loop, &self.completion, .{ .slice = self.send_buffer }, Self, self, Self.writeCallback);
    }

    fn connectCallback(self: ?*Self, _: *xev.Loop, _: *xev.Completion, client: xev.TCP, result: xev.TCP.ConnectError!void) xev.CallbackAction {
        _ = result catch |err| {
            std.debug.print("Connection to {any} failed: {any}\n", .{ self.?.address, err });
            // TODO: take care of this unreachable and handle closing the socket
            self.?.client = xev.TCP.init(self.?.address) catch unreachable;
            return .disarm;
        };

        self.?.state = .connected;
        self.?.client = client;

        std.debug.print("Succesfully connected to {any}\n", .{self.?.address});

        self.?.write();

        return .disarm;
    }

    fn writeCallback(self: ?*Self, _: *xev.Loop, _: *xev.Completion, client: xev.TCP, _: xev.WriteBuffer, result: xev.TCP.WriteError!usize) xev.CallbackAction {
        defer self.?.allocator.free(self.?.send_buffer);

        self.?.client = client;

        if (result) |bytes| {
            if (bytes > self.?.send_buffer.len) {
                std.debug.print("Received message is too long ({d} > {d})\n", .{ bytes, self.?.send_buffer.len });
            }
        } else |err| {
            std.debug.print("Sending to {any} failed: {any}\n", .{ self.?.address, err });
            // TODO: take care of this unreachable and handle closing the socket
            self.?.client = xev.TCP.init(self.?.address) catch unreachable;
            self.?.state = .disconnected;
        }

        return .disarm;
    }
};

fn parseAddress(address: []const u8) !std.net.Address {
    var it = std.mem.splitSequence(u8, address, ":");

    const ip = it.first();
    const port = try std.fmt.parseInt(u16, it.rest(), 10);

    return std.net.Address.parseIp4(ip, port);
}
