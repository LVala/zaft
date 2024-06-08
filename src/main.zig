const std = @import("std");
const xev = @import("xev");
const zaft = @import("zaft.zig");

const ips = [_][]const u8{ "127.0.0.1", "127.0.0.1", "127.0.0.1" };
const ports = [_]u16{ 10001, 10002, 10003 };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var args = try std.process.argsWithAllocator(alloc);
    defer args.deinit();

    _ = args.next();
    const self_id = try std.fmt.parseInt(u32, args.next().?, 10);

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var c_accept: xev.Completion = undefined;

    var socket: xev.TCP = undefined;
    server.accept(&loop, &c_accept, xev.TCP, &socket, acceptCallback);

    const w = try xev.Timer.init();
    defer w.deinit();

    var c: xev.Completion = undefined;
    w.run(&loop, &c, 5000, void, null, &timerCallback);

    try loop.run(.until_done);

    // std.debug.print("KONIEC\n", .{});
    //
    // const send_buf = "12345\n";
    // var c_write: xev.Completion = undefined;
    // socket.write(&loop, &c_write, .{ .slice = send_buf }, void, null, writeCallback);
    //
    // try loop.run(.until_done);
    //
    // std.debug.print("REAL KONIEC\n", .{});
}

fn acceptCallback(ud: ?*xev.TCP, _: *xev.Loop, _: *xev.Completion, result: xev.TCP.AcceptError!xev.TCP) xev.CallbackAction {
    _ = result catch unreachable;
    if (result) |socket| {
        ud.?.* = socket;
        // var send_buf = [_]u8{ 1, 1, 2, 3, 5, 8, 14 };
        // const send_buf = "12345\n";
        // var c_write: xev.Completion = undefined;
        // socket.write(loop, &c_write, .{ .slice = send_buf }, void, null, writeCallback);
    } else |err| {
        std.debug.print("Error accepting a TCP connection: {any}\n", .{err});
    }

    std.debug.print("CON ACCEPTED\n", .{});

    return .disarm;
}

fn writeCallback(_: ?*void, _: *xev.Loop, _: *xev.Completion, _: xev.TCP, _: xev.WriteBuffer, result: xev.TCP.WriteError!usize) xev.CallbackAction {
    std.debug.print("DATA SEND {any}\n", .{result});

    return .rearm;
}

// fn timerCallback(
//     userdata: ?*void,
//     loop: *xev.Loop,
//     c: *xev.Completion,
//     result: xev.Timer.RunError!void,
// ) xev.CallbackAction {
//     _ = userdata;
//     _ = loop;
//     _ = c;
//     _ = result catch unreachable;
//
//     std.debug.print("Siema z petli\n", .{});
//
//     return .disarm;
// }
