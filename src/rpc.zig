const std = @import("std");
const xev = @import("xev");

// TODO: RPC is a bit of a overstatement
pub const RPC = struct {
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
        var it = std.mem.splitSequence(u8, raw_address, ":");

        const ip = it.first();
        const port = try std.fmt.parseInt(u16, it.rest(), 10);
        const address = try std.net.Address.parseIp4(ip, port);

        const client = try xev.TCP.init(address);

        return Self{
            .address = address,
            .allocator = allocator,
            .loop = loop,
            .client = client,
        };
    }

    pub fn call(self: *Self, msg: []const u8) !void {
        // TODO: we could avoid allocations entirely, but this is simpler
        const pt = try self.allocator.alloc(u8, msg.len);
        @memcpy(pt, msg);
        self.send_buffer = pt;

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
            // TODO: take care of this unreachable
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
            std.debug.print("Succesfully send {d} bytes to {any}\n", .{ bytes, self.?.address });
        } else |err| {
            std.debug.print("Sending to {any} failed: {any}\n", .{ self.?.address, err });
            // TODO: take care of this unreachable
            self.?.client = xev.TCP.init(self.?.address) catch unreachable;
            self.?.state = .disconnected;
        }

        return .disarm;
    }

    // TODO: tests
};
