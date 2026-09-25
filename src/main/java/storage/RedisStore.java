package storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisStore {

    private final Map<String, String> data = new ConcurrentHashMap<>();

    public void set(String key, String value) {

        data.put(key, value);
    }

    public String get(String key) {

        return data.get(key);
    }

    public boolean contains(String key) {

        return data.containsKey(key);
    }
}