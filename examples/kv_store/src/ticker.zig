const std = @import("std");

const Raft = @import("main.zig").Raft;

pub const Ticker = struct {
    raft: *Raft,
    mutex: *std.Thread.Mutex,

    const Self = @This();

    pub fn run(self: *Self) void {
        while (true) {
            self.mutex.lock();
            const wait_ms = self.raft.tick();
            self.mutex.unlock();

            std.time.sleep(wait_ms * 1000000);
        }
    }
};
