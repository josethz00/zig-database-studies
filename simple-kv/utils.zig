const std = @import("std");

pub fn get_now_unix_timestamp() !i64 {
    return std.time.timestamp();
}

pub fn writeToWal(wal_path: []const u8, cmd: []const u8, key: []const u8, value: ?[]const u8) !void {
    const fd = try std.posix.open(wal_path, .{
        .CREAT = true,
        .APPEND = true,
        .ACCMODE = .WRONLY, // write-only mode
    }, 0o666);
    defer std.posix.close(fd);

    var buffer: [1024]u8 = undefined;
    var n: []u8 = undefined;

    if (value) |v| {
        n = try std.fmt.bufPrint(&buffer, "{s} {s} {s}\n", .{ cmd, key, v });
    } else {
        n = try std.fmt.bufPrint(&buffer, "{s} {s}\n", .{ cmd, key });
    }

    _ = try std.posix.write(fd, buffer[0..n.len]);
    _ = try std.posix.fsync(fd);
}
