const std = @import("std");
const utils = @import("utils.zig");
const memtable = @import("./memtable.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var memtable_kv = try memtable.MemTable.new(allocator);
    defer memtable_kv.deinit(allocator);

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var stdin_buffer: [1024]u8 = undefined;
    var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
    const stdin = &stdin_reader.interface;

    // Replay WAL
    if (std.fs.cwd().openFile("simple-kv.wal", .{ .mode = .read_only })) |wal_file| {
        defer wal_file.close();

        var line_buffer: [1024]u8 = undefined;
        var reader = wal_file.deprecatedReader();
        while (try reader.readUntilDelimiterOrEof(&line_buffer, '\n')) |line| {
            const trimmed = std.mem.trim(u8, line, " \r\n\t");
            if (trimmed.len == 0) continue;

            var it = std.mem.tokenizeAny(u8, trimmed, " ");
            const cmd = it.next() orelse continue;

            if (std.mem.eql(u8, cmd, "PUT")) {
                const key = it.next() orelse continue;
                const value = it.next() orelse continue;
                try memtable_kv.put(allocator, key, value, try utils.get_now_unix_timestamp());
            } else if (std.mem.eql(u8, cmd, "DELETE")) {
                const key = it.next() orelse continue;
                try memtable_kv.delete(allocator, key, try utils.get_now_unix_timestamp());
            }
        }
    } else |_| {
        // If WAL doesn't exist, just continue with empty memtable
        // The WAL file will be then created
    }

    try stdout.print("Welcome to SimpleKV\n", .{});
    try stdout.print("WAL replay complete\n\n", .{});

    // REPL
    while (true) {
        try stdout.print("simplekv > ", .{});
        try stdout.flush();

        const line = try stdin.takeDelimiterExclusive('\n');
        if (line.len == 0) break;

        const trimmed = std.mem.trim(u8, line, " \r\n\t");
        const trimmed_len = trimmed.len;

        const lc: []u8 = try allocator.alloc(u8, trimmed_len);
        defer allocator.free(lc);
        _ = std.ascii.lowerString(lc, trimmed);

        if (std.mem.eql(u8, lc, "exit")) {
            try stdout.print("Bye!\n", .{});
            try stdout.flush();
            break;
        } else if (std.mem.startsWith(u8, lc, "put")) {
            var it = std.mem.tokenizeAny(u8, trimmed, " ");
            _ = it.next(); // skip command

            const key = it.next() orelse {
                try stdout.print("Malformed PUT: missing key\n", .{});
                continue;
            };
            const value = it.next() orelse {
                try stdout.print("Malformed PUT: missing value\n", .{});
                continue;
            };

            if (it.next() != null) {
                try stdout.print("Malformed PUT: too many arguments\n", .{});
                continue;
            }

            try utils.writeToWal("simple-kv.wal", "PUT", key, value);
            try memtable_kv.put(allocator, key, value, try utils.get_now_unix_timestamp());
            try stdout.print("PUT k:{s} v:{s}\n", .{ key, value });
        } else if (std.mem.startsWith(u8, lc, "delete")) {
            var it = std.mem.tokenizeAny(u8, trimmed, " ");
            _ = it.next();

            const key = it.next() orelse {
                try stdout.print("Malformed DELETE: missing key\n", .{});
                continue;
            };

            if (it.next() != null) {
                try stdout.print("Malformed DELETE: too many arguments\n", .{});
                continue;
            }

            try utils.writeToWal("simple-kv.wal", "DELETE", key, null);
            try memtable_kv.delete(allocator, key, try utils.get_now_unix_timestamp());
            try stdout.print("DELETE {s}\n", .{key});
        } else if (std.mem.startsWith(u8, lc, "get")) {
            var it = std.mem.tokenizeAny(u8, trimmed, " ");
            _ = it.next();

            const key = it.next() orelse {
                try stdout.print("Malformed GET: missing key\n", .{});
                continue;
            };

            if (it.next() != null) {
                try stdout.print("Malformed GET: too many arguments\n", .{});
                continue;
            }

            const retrieved_entry = memtable_kv.get(key) orelse {
                try stdout.print("GET - key not found\n", .{});
                continue;
            };
            try stdout.print("GET {s} -> {s}\n", .{ retrieved_entry.key, retrieved_entry.value.? });
        } else if (std.mem.eql(u8, lc, "scan")) {
            var all_entries = try memtable_kv.scan(allocator);
            for (all_entries.items) |entry| {
                try stdout.print("{s} -> {s}\n", .{ entry.key, entry.value.? });
            }
            all_entries.deinit(allocator);
        } else {
            try stdout.print("Unknown command: {s}\n", .{trimmed});
        }

        try stdout.flush();
    }
}
