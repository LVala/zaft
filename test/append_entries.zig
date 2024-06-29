const std = @import("std");
const utils = @import("utils.zig");

const testing = std.testing;

const TestRPCs = utils.TestRPCs;
const TestRaft = utils.TestRaft;
const setupTestRaft = utils.setupTestRaft;

fn test_converge_logs(leader_log: []const TestRaft.LogEntry, leader_term: u32, follower_log: []const TestRaft.LogEntry, first_common: u32) !void {
    // first_common is a log index (so starts at 1), not array index!
    var rpcs: TestRPCs = undefined;
    var raft = setupTestRaft(&rpcs, true);
    defer raft.deinit();

    const leader_id = 1;

    for (follower_log) |entry| try raft.log.append(entry);
    raft.current_term = follower_log[follower_log.len - 1].term;

    // we start with empty AppendEntries
    var idx: usize = leader_log.len + 1;
    while (idx > first_common) : (idx -= 1) {
        const ae = TestRaft.AppendEntries{
            .leader_id = leader_id,
            .term = leader_term,
            .prev_log_index = @intCast(idx - 1),
            .prev_log_term = leader_log[idx - 2].term,
            .entries = leader_log[(idx - 1)..],
            .leader_commit = 0,
        };
        raft.handleRPC(leader_id, .{ .append_entries = ae });

        switch (rpcs[leader_id]) {
            .append_entries_response => |aer| {
                const valid = if (idx == first_common + 1) aer.success else !aer.success;
                try testing.expect(valid);
            },
            else => {
                return error.TestUnexpectedResult;
            },
        }
    }

    // check if the follower log is equal to leaders log
    try testing.expect(leader_log.len == raft.log.items.len);
    for (leader_log, raft.log.items) |l_entry, f_entry| {
        try testing.expect(l_entry.term == f_entry.term);
        try testing.expect(l_entry.entry == f_entry.entry);
    }

    try testing.expect(raft.current_term == leader_term);
}

test "follower adds new entries from AppendEntries" {
    // from Figure 7 in the Raft paper
    const leader_log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
    };

    const follower_log1 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log1, 9);

    const follower_log2 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log2, 4);
}

test "follower replaces conflicting entires based on AppendEntries" {
    // from Figure 7 in the Raft paper
    const leader_log = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
    };

    // entries with value 99 should be removed
    const follower_log1 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
        .{ .term = 6, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log1, 10);

    const follower_log2 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 5, .entry = 6 },
        .{ .term = 5, .entry = 7 },
        .{ .term = 6, .entry = 8 },
        .{ .term = 6, .entry = 9 },
        .{ .term = 6, .entry = 10 },
        .{ .term = 7, .entry = 99 },
        .{ .term = 7, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log2, 10);

    const follower_log3 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 4, .entry = 4 },
        .{ .term = 4, .entry = 5 },
        .{ .term = 4, .entry = 99 },
        .{ .term = 4, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log3, 5);

    const follower_log4 = [_]TestRaft.LogEntry{
        .{ .term = 1, .entry = 1 },
        .{ .term = 1, .entry = 2 },
        .{ .term = 1, .entry = 3 },
        .{ .term = 2, .entry = 99 },
        .{ .term = 2, .entry = 99 },
        .{ .term = 2, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
        .{ .term = 3, .entry = 99 },
    };
    try test_converge_logs(&leader_log, 8, &follower_log4, 3);
}
