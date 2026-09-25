package storage;

import java.util.Map;

public class StreamEntry {

    private final StreamId id;
    private final Map<String, String> fields;

    public StreamEntry(
            StreamId id,
            Map<String, String> fields) {

        this.id = id;
        this.fields = fields;
    }

    public StreamId getId() {
        return id;
    }

    public Map<String, String> getFields() {
        return fields;
    }
}