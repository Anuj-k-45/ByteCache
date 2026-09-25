package storage;

import java.util.ArrayList;
import java.util.List;

public class RedisStream {

    private final List<StreamEntry> entries = new ArrayList<>();

    public void addEntry(
            StreamEntry entry) {

        StreamId newId = entry.getId();

        // First entry
        if (entries.isEmpty()) {

            StreamId minimumId = new StreamId(0, 0);

            if (newId.compareTo(minimumId) <= 0) {

                throw new IllegalArgumentException(
                        "The ID specified in XADD must be greater than 0-0");
            }

            entries.add(entry);

            return;
        }

        StreamEntry lastEntry = entries.get(
                entries.size() - 1);

        StreamId lastId = lastEntry.getId();

        if (newId.compareTo(lastId) <= 0) {

            throw new IllegalArgumentException(
                    "The ID specified in XADD is equal or smaller than the target stream top item");
        }

        entries.add(entry);
    }
}