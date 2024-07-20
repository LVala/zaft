const std = @import("std");
const common = @import("main.zig");

const ct_name = "current_term";
const vf_name = "voted_for";
const log_name = "log";

pub const Storage = struct {
    current_term: std.fs.File,
    voted_for: std.fs.File,
    log: std.fs.Dir,
    log_index: u32,

    const Self = @This();

    pub fn init(cache_name: []const u8) !Self {
        const cwd = std.fs.cwd();

        cwd.makeDir(cache_name) catch |err| switch (err) {
            std.fs.Dir.MakeError.PathAlreadyExists => {},
            else => return err,
        };
        var cache_dir = try cwd.openDir(cache_name, .{ .iterate = true });
        defer cache_dir.close();

        // TODO: write default values if files were just created
        const ct_file = try cache_dir.createFile(ct_name, .{ .read = true, .truncate = false });
        const vf_file = try cache_dir.createFile(vf_name, .{ .read = true, .truncate = false });

        cache_dir.makeDir(log_name) catch |err| switch (err) {
            std.fs.Dir.MakeError.PathAlreadyExists => {},
            else => return err,
        };
        const log_dir = try cache_dir.openDir(log_name, .{ .iterate = true });

        var iter = log_dir.iterate();
        var file_count: u32 = 0;
        while (try iter.next()) |entry| {
            if (entry.kind == .file) file_count += 1;
        }

        return Self{
            .current_term = ct_file,
            .voted_for = vf_file,
            .log = log_dir,
            .log_index = file_count,
        };
    }

    pub fn deinit(self: *Self) void {
        self.current_term.close();
        self.voted_for.close();
        self.log.close();
    }

    pub fn readCurrentTerm(self: *const Self) !u32 {
        var buffer: [128]u8 = undefined;
        const len = try self.current_term.readAll(&buffer);

        if (len == 0) return 0;
        const voted_for = try std.fmt.parseInt(u32, buffer[0..len], 10);
        return voted_for;
    }

    pub fn writeCurrentTerm(self: *Self, current_term: u32) !void {
        var buffer: [128]u8 = undefined;
        const size = std.fmt.formatIntBuf(&buffer, current_term, 10, .lower, .{});

        try self.current_term.seekTo(0);
        try self.current_term.writeAll(buffer[0..size]);
        try std.posix.ftruncate(self.current_term.handle, size);
    }

    pub fn readVotedFor(self: *const Self) !?u32 {
        var buffer: [128]u8 = undefined;
        const len = try self.voted_for.readAll(&buffer);

        if (len == 0) return null;
        const voted_for = try std.fmt.parseInt(u32, buffer[0..len], 10);
        return voted_for;
    }

    pub fn writeVotedFor(self: *Self, voted_for: ?u32) !void {
        var buffer: [128]u8 = undefined;
        const size = if (voted_for) |idx| std.fmt.formatIntBuf(&buffer, idx, 10, .lower, .{}) else 0;

        try self.voted_for.seekTo(0);
        try self.voted_for.writeAll(buffer[0..size]);
        try std.posix.ftruncate(self.voted_for.handle, size);
    }

    pub fn readLog(self: *const Self, allocator: std.mem.Allocator) ![]common.Raft.LogEntry {
        const entry_log = try allocator.alloc(common.Raft.LogEntry, self.log_index);
        var name: [128]u8 = undefined;
        var buffer: [1024]u8 = undefined;

        var idx: u32 = 1;
        while (idx <= self.log_index) : (idx += 1) {
            const size = std.fmt.formatIntBuf(&name, idx, 10, .lower, .{});
            const file = try self.log.openFile(name[0..size], .{});
            defer file.close();

            const len = try file.readAll(&buffer);

            const json = try std.json.parseFromSlice(common.Raft.LogEntry, allocator, buffer[0..len], .{ .allocate = .alloc_always });
            // defer json.deinit();
            // TODO: we need to clean up the Entry memory somewhere

            entry_log[idx - 1] = json.value;
        }

        return entry_log;
    }

    pub fn appendLog(self: *Self, entry: common.Raft.LogEntry, allocator: std.mem.Allocator) !void {
        var name: [128]u8 = undefined;
        const idx = self.log_index + 1;

        const json = try std.json.stringifyAlloc(allocator, entry, .{});
        defer allocator.free(json);

        const size = std.fmt.formatIntBuf(&name, idx, 10, .lower, .{});
        const file = try self.log.createFile(name[0..size], .{});
        defer file.close();

        try file.writeAll(json);
        self.log_index = idx;
    }

    pub fn popLog(self: *Self, allocator: std.mem.Allocator) !common.Raft.LogEntry {
        var name: [128]u8 = undefined;
        var buffer: [1024]u8 = undefined;

        const size = std.fmt.formatIntBuf(&name, self.log_index, 10, .lower, .{});
        const file = try self.log.openFile(name[0..size], .{});
        defer file.close();

        const len = try file.readAll(&buffer);

        const json = try std.json.parseFromSlice(common.Raft.LogEntry, allocator, buffer[0..len], .{ .allocate = .alloc_always });
        // defer json.deinit();
        // TODO: we need to clean up the Entry memory somewhere

        try self.log.deleteFile(name[0..size]);
        self.log_index -= 1;

        return json.value;
    }
};
