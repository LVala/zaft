<div align="center">

# `zaft`

<a href="https://ziglang.org/download/"><img src="https://img.shields.io/badge/0.13.0-orange?style=flat&logo=zig&label=Zig&color=%23eba742"></a>
<a href="https://github.com/LVala/zaft/releases"><img src="https://img.shields.io/github/v/release/LVala/zaft?link=https%3A%2F%2Fgithub.com%2FLVala%2Fzaft%2Freleases"></a>
<a href="https://github.com/LVala/zaft/actions"><img src="https://img.shields.io/github/actions/workflow/status/LVala/zaft/ci.yml?link=https%3A%2F%2Fgithub.com%2FLVala%2Fzaft%2Factions"></a>

### The Raft Consensus Algorithm in Zig

This repository houses `zaft` - [Raft Consensus Algorithm](https://raft.github.io/) library implemented in Zig. It provides the building blocks
for creating distributed systems requiring consensus among replicated state machines, like databases.

</div>

## Installation

This package can be installed using the Zig package manager. In `build.zig.zon` add `zaft` to the dependency list:

```zig
// in build.zig.zon
.{
    .name = "my-project",
    .version = "0.0.0",
    .dependencies = .{
        .zaft = .{
            .url = "https://github.com/LVala/zaft/archive/<git-ref-here>.tar.gz",
            .hash = "12208070233b17de6be05e32af096a6760682b48598323234824def41789e993432c"
        },
    },
}
```

The output of `zig build` will provide you with a valid hash, use it to replace the one above.

Add the `zaft` module in `build.zig`:

```zig
// in build.zig
const zaft = b.dependency("zaft", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("zaft", zaft.module("zaft"));
```

Now you should be able to import `zaft` in your `exe`s root source file:

```zig
const zaft = @import("zaft");
```

## Usage

This section will show you how to integrate `zaft` with your program step-by-step. If you prefer to take a look at a fully working example,
check out the [kv_store](./examples/kv_store) - in-memory, replicated key-value store based on `zaft`.

> [!IMPORTANT]
> This tutorial assumes some familiarity with the Raft Consensus Algorithm. If not, I highly advise you to at least skim through
> the [Raft paper](https://raft.github.io/raft.pdf). Don't worry, it's a short and very well-written paper!

Firstly, initialize the `Raft` struct:

```zig
// we'll get to UserData and Entry in a second
const Raft = @import("zaft").Raft(UserData, Entry);

const raft = Raft.init(config, initial_state, callbacks, allocator);
defer raft.deinit();
```

`Raft.init` takes four arguments:

* `config` - configuration of this particular Raft node:

```zig
const config = Raft.Config{
    .id = 3,  // id of this Raft node
    .server_no = 5,  // total number of Raft nodes, there should be nodes with ids from 0 up to server_no-1
    // there's other options with some sane defaults, check out this struct's definition to learn
    // what else can be configured
};
```

* `callbacks` - `Raft` will call this function to perform various actions:

```zig
// makeRPC is used to send Raft messages to other nodes
// this function should be non-blocking (not wait for the response)
fn makeRPC(ud: *UserData, id: u32, rpc: Raft.RPC) !void {
    const address = ud.node_addresse[id];
    // it's your responsibility to serialize the message, consider using e.g. std.json
    const msg: []u8 = serialize(rpc);
    try ud.client.send(address, msg);
}

// Entry can be whatever you want
// in this callback the entry should be applied to the state machine
// applying an entry must be deterministic! This means that, after applying the
// same entries in the same order, the state machine must end up in the same state every time
fn applyEntry(ud: *UserData, entry: Entry) !void {
    // let's assume that is some kind of key-value store
    switch(entry) {
        .add => |add| try ud.store.add(add.key, add.value),
        .remove => |remove| try ud.store.remove(remove.key),
    }
}

// these two functions have to append/pop entry to/from the log
fn logAppend(ud: *UserData, log_entry: Raft.LogEntry) !void {
    try ud.database.appendEntry(log_entry);
}

fn logPop(ud: *UserData) !Raft.LogEntry {
    const log_entry = try ud.database.popEntry();
    return log_entry;
}

// these functions are responsible for persisting values, that can be overridden on every save 
fn persistCurrentTerm(ud: *UserData, current_term: u32) !void {
    try ud.database.persistCurrentTerm(current_term);
}

fn persistVotedFor(ud: *UserData, voted_for: ?u32) !void {
    try ud.database.persistVotedFor(voted_for);
}
```

> [!WARNING]
> Notice that all of the callbacks can return an error (mostly for the sake of convenience).
>
> Error returned from `makeRPC` will be ignored, the RPC will be simply retried after
> an appropriate timeout. Errors returned from other functions, as of now, will result in a panic.

```zig
// pointer to user_data will be passed as a first argument to all of the callbacks
// you can place whatever you want in the UserData
const user_data = UserData {
    // these are some imaginary utilities
    // necessary to make Raft work
    database: Database,
    http_client: HttpClient,
    node_addresses: []std.net.Address,
    store: Store,
};

const callbacks = Raft.Callbacks {
    .user_data = &user_data,
    .makeRPC = makeRPC,
    .applyEntry = applyEntry,
    .logAppend = logAppend,
    .logPop = logPop,
    .persistCurrentTerm = persisCurrentTerm,
    .persistVotedFor = persistVotedFor,
};
```

* `initial_state` - the persisted state of this Raft node. On each reboot, you need to read the persisted Raft state
(`current_term`, `voted_for`, and `log`, previously saved by the callbacks) and use it as the `InitialState`:

```zig
const initial_state = Raft.InitialState {
    //let's assume we saved the state to a persistent database of some kind
    .voted_for = user_data.database.readVotedFor(),
    .current_term = user_data.database.readCurrentTerm(),
    .log = user_data.database.readLog(),
};
```

Lastly, an `std.mem.Allocator` will be used to provide memory for the Raft log.

---

The `Raft` struct needs to be periodically ticked to trigger timeouts and other necessary actions. You can use a separate thread to do that, or
built your program based on an event loop like [libexev](https://github.com/mitchellh/libxev) with its `xev.Timer`.

```zig
const tick_after = raft.tick();
// tick_after is the number of milliseconds after which raft should be ticked again
```

For instance, [kv_store](./examples/kv_store/src/ticker.zig) uses a separate thread exclusively to tick the `Raft` struct.

> [!WARNING]
> The `Raft` struct is *not* thread-safe. Use appropriate synchronization means to make sure it is not accessed simultaneously by many threads
> (e.g. a simple `std.Thread.Mutex` will do).

Next, messages from other Raft nodes need to be fed to the local `Raft` struct by calling:

```zig
// you will need to receive and deserialize the messages from other peers
const msg: Raft.RPC = try recv_msg();
raft.handleRPC(msg);
```

Lastly, entries can be appended to `Raft`s log by calling:

```zig
const entry = Entry { ... };
const idx = try raft.appendEntry(entry);
```

It will return an index of the new entry. According to the Raft algorithm, your program should block on client requests
until the entry has been applied. You can use `std.Thread.Condition` and call its `notify` function in the `applyEntry` callback to notify
the program that the entry was applied. You can check whether the entry was applied by using `raft.checkIfApplied(idx)`.
Take a look at how [kv_store](./examples/kv_store/src/main.zig) does this.

`appendEntry` function will return an error if the node is not a leader. In such a case, you should redirect the client request to the leader node.
You can check which node is the leader by using `raft.getCurrentLeader()`. You can also check if the node is a leader proactively by calling
`raft.checkifLeader()`.

## Next steps

The library is fine to be used already, but there's plenty of room for improvements and new features, like:

* Creating a simulator to test and find problems in the implementation.
* Add auto-generated API documentation based on `zig build -femit-docs`.
* Implementing _Cluster membership changes_.
* Implementing _Log compaction_.
