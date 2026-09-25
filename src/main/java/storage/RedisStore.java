package storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisStore {

    private final Map<String, Object> data = new ConcurrentHashMap<>();

    public void set(
            String key,
            String value) {

        data.put(
                key,
                new StoredValue(value, null));
    }

    public void set(
            String key,
            String value,
            long expiryMilliseconds) {

        long expiresAt = System.currentTimeMillis()
                + expiryMilliseconds;

        data.put(
                key,
                new StoredValue(
                        value,
                        expiresAt));
    }

    public String get(String key) {

        Object storedObject = data.get(key);

        if (!(storedObject instanceof StoredValue)) {
            return null;
        }

        StoredValue storedValue = (StoredValue) storedObject;

        if (storedValue.isExpired()) {

            data.remove(key);

            return null;
        }

        return storedValue.getValue();
    }

    public void addStreamEntry(
            String key,
            StreamEntry entry) {

        Object storedObject = data.get(key);

        RedisStream stream;

        if (storedObject == null) {

            stream = new RedisStream();

            data.put(
                    key,
                    stream);

        } else if (storedObject instanceof RedisStream) {

            stream = (RedisStream) storedObject;

        } else {

            throw new IllegalStateException(
                    "Key already contains a different type");
        }

        stream.addEntry(entry);
    }

    public String getType(String key) {

        Object storedObject = data.get(key);

        if (storedObject == null) {
            return "none";
        }

        if (storedObject instanceof RedisStream) {
            return "stream";
        }

        if (storedObject instanceof StoredValue) {
            return "string";
        }

        return "none";
    }
}