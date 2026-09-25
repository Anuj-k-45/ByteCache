package storage;

public class StreamId implements Comparable<StreamId> {

    private final long millisecondsTime;
    private final long sequenceNumber;

    public StreamId(
            long millisecondsTime,
            long sequenceNumber) {

        this.millisecondsTime = millisecondsTime;
        this.sequenceNumber = sequenceNumber;
    }

    public static StreamId parse(String id) {

        String[] parts = id.split("-");

        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid stream ID");
        }

        long millisecondsTime = Long.parseLong(parts[0]);

        long sequenceNumber = Long.parseLong(parts[1]);

        return new StreamId(
                millisecondsTime,
                sequenceNumber);
    }

    public long getMillisecondsTime() {
        return millisecondsTime;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public int compareTo(StreamId other) {

        if (millisecondsTime != other.millisecondsTime) {

            return Long.compare(
                    millisecondsTime,
                    other.millisecondsTime);
        }

        return Long.compare(
                sequenceNumber,
                other.sequenceNumber);
    }

    @Override
    public String toString() {

        return millisecondsTime
                + "-"
                + sequenceNumber;
    }
}