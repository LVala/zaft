const std = @import("std");

const Raft = @import("main.zig").Raft;

pub const Ticker = struct {
    raft: *Raft,
    mutex: *std.Thread.Mutex,

    const Self = @This();

    pub fn init(raft: *Raft, mutex: *std.Thread.Mutex) Self {
        return Self{ .raft = raft, .mutex = mutex };
    }

    pub fn run_in_thread(self: *Self) !void {
        const thread = try std.Thread.spawn(.{}, run, .{self});
        thread.detach();
    }

    pub fn run(self: *Self) void {
        while (true) {
            self.mutex.lock();
            const wait_ms = self.raft.tick();
            self.mutex.unlock();

            std.time.sleep(wait_ms * 1000000);
        }
    }
};
