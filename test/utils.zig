const std = @import("std");
const zaft = @import("zaft");

const len = 5;
pub const TestRaft = zaft.Raft(anyopaque, u32);
pub const TestRPCs = [len]TestRaft.RPC;

pub fn getTime() u64 {
    const time_ms = std.time.milliTimestamp();
    return @intCast(time_ms);
}

pub fn setupTestRaft(rpcs: *TestRPCs, elect_leader: bool) TestRaft {
    const callbacks = TestRaft.Callbacks{
        .user_data = rpcs,
        .makeRPC = struct {
            pub fn makeRPC(ud: *anyopaque, id: u32, rpc: TestRaft.RPC) !void {
                // TODO: I don't know what I did here
                const ud_rpcs = @as(*TestRPCs, @alignCast(@ptrCast(ud)));
                ud_rpcs.*[id] = rpc;
            }
        }.makeRPC,
    };

    // our Raft always gets the id 0
    var raft = TestRaft.init(0, len, callbacks, std.testing.allocator) catch unreachable;

    if (!elect_leader) return raft;

    raft.timeout = getTime() - 1;
    _ = raft.tick();

    for (1..len) |id| {
        const rvr = TestRaft.RequestVoteResponse{ .term = raft.current_term, .vote_granted = true };
        raft.handleRPC(@intCast(id), .{ .request_vote_response = rvr });
    }

    return raft;
}
