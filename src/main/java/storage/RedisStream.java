package storage;

import java.util.ArrayList;
import java.util.List;

public class RedisStream {

    private final List<StreamEntry> entries = new ArrayList<>();

    public StreamId generateNextId() {

        long millisecondsTime = System.currentTimeMillis();

        long sequenceNumber = getNextSequenceNumber(
                millisecondsTime);

        return new StreamId(
                millisecondsTime,
                sequenceNumber);
    }

    public StreamId addEntry(
            StreamEntry entry) {

        StreamId newId = entry.getId();

        if (entries.isEmpty()) {

            StreamId minimumId = new StreamId(0, 0);

            if (newId.compareTo(minimumId) <= 0) {

                throw new IllegalArgumentException(
                        "The ID specified in XADD must be greater than 0-0");
            }

            entries.add(entry);

            return newId;
        }

        StreamId lastId = entries.get(
                entries.size() - 1)
                .getId();

        if (newId.compareTo(lastId) <= 0) {

            throw new IllegalArgumentException(
                    "The ID specified in XADD is equal or smaller than the target stream top item");
        }

        entries.add(entry);

        return newId;
    }

    public long getNextSequenceNumber(
            long millisecondsTime) {

        if (millisecondsTime == 0) {

            long lastSequence = getLastSequenceNumber(
                    millisecondsTime);

            if (lastSequence == -1) {
                return 1;
            }

            return lastSequence + 1;
        }

        long lastSequence = getLastSequenceNumber(
                millisecondsTime);

        if (lastSequence == -1) {
            return 0;
        }

        return lastSequence + 1;
    }

    private long getLastSequenceNumber(
            long millisecondsTime) {

        for (int i = entries.size() - 1; i >= 0; i--) {

            StreamId id = entries.get(i).getId();

            if (id.getMillisecondsTime() == millisecondsTime) {

                return id.getSequenceNumber();
            }

            if (id.getMillisecondsTime() < millisecondsTime) {

                break;
            }
        }

        return -1;
    }

    public List<StreamEntry> getRange(
            StreamId startId,
            StreamId endId) {

        List<StreamEntry> result = new ArrayList<>();

        for (StreamEntry entry : entries) {

            StreamId entryId = entry.getId();

            if (entryId.compareTo(startId) >= 0
                    && entryId.compareTo(endId) <= 0) {

                result.add(entry);
            }
        }

        return result;
    }

    public List<StreamEntry> getEntriesAfter(
            StreamId startId) {

        List<StreamEntry> result = new ArrayList<>();

        for (StreamEntry entry : entries) {

            StreamId entryId = entry.getId();

            if (entryId.compareTo(startId) > 0) {
                result.add(entry);
            }
        }

        return result;
    }

    public StreamId getLastId() {

        if (entries.isEmpty()) {
            return new StreamId(0, 0);
        }

        return entries.get(entries.size() - 1).getId();
    }
}