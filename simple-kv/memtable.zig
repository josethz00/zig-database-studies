const std = @import("std");

pub const MemTableEntry = struct {
    key: []u8,
    value: ?[]u8,
    timestamp: i64,
    deleted: bool,
};

pub const MemTable = struct {
    entries: std.ArrayList(MemTableEntry),
    size: usize,
    lock: std.Thread.Mutex,

    pub fn new(allocator: std.mem.Allocator) !@This() {
        return @This(){ .entries = try std.ArrayList(MemTableEntry).initCapacity(allocator, 100), .size = 0, .lock = std.Thread.Mutex{} };
    }

    pub fn deinit(self: *MemTable, allocator: std.mem.Allocator) void {
        self.entries.deinit(allocator);
    }

    pub fn put(self: *@This(), allocator: std.mem.Allocator, key: []const u8, value: []const u8, timestamp: i64) !void {
        _ = self.lock.lock();
        defer self.lock.unlock();

        const recovered_index = self.get_index(key);

        const cloned_key = try allocator.dupe(u8, key);
        const cloned_value = try allocator.dupe(u8, value);

        const new_entry = MemTableEntry{
            .key = cloned_key,
            .value = cloned_value,
            .timestamp = timestamp,
            .deleted = false,
        };

        if (recovered_index) |index| {
            // Key exists, update the entry
            const old_entry = self.entries.items[index];

            // Adjust size based on the difference between old and new entries. This handles possible tombstones
            self.size -= old_entry.key.len + (if (old_entry.value) |v| v.len else 0) + 8 + 1;

            allocator.free(old_entry.key);
            if (old_entry.value) |v| {
                allocator.free(v);
            }

            self.entries.items[index] = new_entry;
        } else {
            // Key does not exist, insert a new entry.
            const insert_index = self.find_insert_index(key);
            try self.entries.insert(allocator, insert_index, new_entry);
        }

        // Recalculate the total size of the MemTable
        // The calculation accounts for the sizes of all entries.
        // Each entry's size is the sum of:
        // - The key's byte length (`key.len`).
        // - The value's byte length (`value.len`).
        // - The fixed size of the timestamp (8 bytes, for i64).
        // - The fixed size of the `deleted` boolean (1 byte).
        self.size += new_entry.key.len + (if (new_entry.value) |v| v.len else 0) + 8 + 1;
    }

    pub fn delete(self: *@This(), allocator: std.mem.Allocator, key: []const u8, timestamp: i64) !void {
        _ = self.lock.lock();
        defer self.lock.unlock();

        const recovered_index = self.get_index(key);
        const cloned_key = try allocator.dupe(u8, key);

        const tombstone_entry = MemTableEntry{
            .key = cloned_key,
            .value = null, // The tombstone has no value.
            .timestamp = timestamp,
            .deleted = true,
        };

        if (recovered_index) |index| {
            // Key exists. Update the existing entry to a tombstone.
            const old_entry = self.entries.items[index];

            // Adjust size by first subtracting the old entry's size...
            self.size -= old_entry.key.len + (if (old_entry.value) |v| v.len else 0) + 8 + 1;

            // free the old key and value's memory to avoid leaks.
            allocator.free(old_entry.key);
            if (old_entry.value) |v| {
                allocator.free(v);
            }

            self.entries.items[index] = tombstone_entry;
        } else {
            // Key does not exist. Insert a new tombstone.
            const insert_index = self.find_insert_index(key);
            try self.entries.insert(allocator, insert_index, tombstone_entry);
        }

        // add the new tombstone's size.
        self.size += tombstone_entry.key.len + 0 + 8 + 1;
    }

    pub fn get(self: *@This(), key: []const u8) ?MemTableEntry {
        const index = self.get_index(key) orelse return null;
        return self.entries.items[index];
    }

    pub fn scan(self: *@This(), allocator: std.mem.Allocator) anyerror!std.ArrayList(MemTableEntry) {
        var all_entries = try std.ArrayList(MemTableEntry).initCapacity(allocator, self.entries.capacity);

        for (self.entries.items) |item| {
            if (item.deleted) {
                continue;
            }
            try all_entries.append(allocator, item);
        }
        return all_entries;
    }

    fn get_index(self: *MemTable, key: []const u8) ?usize {
        var lowest_index: usize = 0;
        var highest_index: usize = self.entries.items.len;

        // BINARY SEARCH
        while (lowest_index < highest_index) {
            const mid = lowest_index + (highest_index - lowest_index) / 2;
            const item = self.entries.items[mid];
            const cmp = std.mem.eql(u8, item.key, key);

            if (cmp) {
                return mid; // found
            } else if (std.mem.lessThan(u8, item.key, key)) {
                lowest_index = mid + 1;
            } else {
                highest_index = mid;
            }
        }

        return null; // not found
    }

    fn find_insert_index(self: *MemTable, key: []const u8) usize {
        var low: usize = 0;
        var high: usize = self.entries.items.len;

        while (low < high) {
            const mid = low + (high - low) / 2;
            if (std.mem.lessThan(u8, self.entries.items[mid].key, key)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        return low;
    }
};
