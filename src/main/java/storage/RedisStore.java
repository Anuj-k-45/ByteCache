package storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisStore {

    private final Map<String, StoredValue> data = new ConcurrentHashMap<>();

    public void set(String key, String value) {
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
                new StoredValue(value, expiresAt));
    }

    public String get(String key) {

        StoredValue storedValue = data.get(key);

        if (storedValue == null) {
            return null;
        }

        if (storedValue.isExpired()) {
            data.remove(key);
            return null;
        }

        return storedValue.getValue();
    }
}