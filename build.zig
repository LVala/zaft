const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zaft = b.addModule("zaft", .{
        .root_source_file = b.path("src/main.zig"),
    });

    const test_filter = b.option([]const u8, "test-filter", "Skip tests that do not match any filter");
    const test_exe = b.addTest(.{
        .name = "zaft-test",
        .root_source_file = b.path("test/main.zig"),
        .target = target,
        .optimize = optimize,
        .filters = if (test_filter) |f| &.{f} else &.{},
    });

    test_exe.root_module.addImport("zaft", zaft);

    const test_run = b.addRunArtifact(test_exe);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&test_run.step);
}
