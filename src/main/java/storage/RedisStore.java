package storage;

import java.util.ArrayList;
import java.util.List;
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

    public synchronized void addStreamEntry(
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

        notifyAll();
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

        if (storedObject instanceof RedisList) {
            return "list";
        }

        return "none";
    }

    public StreamId generateStreamId(
            String key,
            long millisecondsTime) {

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

            throw new IllegalArgumentException(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        long sequenceNumber = stream.getNextSequenceNumber(
                millisecondsTime);

        return new StreamId(
                millisecondsTime,
                sequenceNumber);
    }

    public StreamId generateNextStreamId(
            String key) {

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

            throw new IllegalArgumentException(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        return stream.generateNextId();
    }

    public List<StreamEntry> getStreamRange(
            String key,
            StreamId startId,
            StreamId endId) {

        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisStream)) {
            return new ArrayList<>();
        }

        RedisStream stream = (RedisStream) storedObject;

        return stream.getRange(
                startId,
                endId);
    }

    public List<StreamEntry> getStreamEntriesAfter(
            String key,
            StreamId startId) {

        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisStream)) {
            return new ArrayList<>();
        }

        RedisStream stream = (RedisStream) storedObject;

        return stream.getEntriesAfter(
                startId);
    }

    public synchronized void waitForStreamUpdate(
            long timeoutMilliseconds)
            throws InterruptedException {

        wait(timeoutMilliseconds);
    }

    public synchronized void notifyStreamUpdate() {
        notifyAll();
    }

    public StreamId getStreamLastId(String key) {

        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisStream)) {
            return new StreamId(0, 0);
        }

        RedisStream stream = (RedisStream) storedObject;

        return stream.getLastId();
    }

    public synchronized int rpush(String key, String value) {

        Object storedObject = data.get(key);

        RedisList list;

        if (storedObject == null) {
            list = new RedisList();
            data.put(key, list);
        } else if (storedObject instanceof RedisList) {
            list = (RedisList) storedObject;
        } else {
            throw new IllegalArgumentException(
                    "Key already contains a different type");
        }

        list.add(value);

        notifyAll();

        return list.size();
    }

    public List<String> lrange(
            String key,
            int start,
            int stop) {

        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisList)) {
            return new ArrayList<>();
        }

        RedisList list = (RedisList) storedObject;

        return list.getRange(start, stop);
    }

    public synchronized int lpush(String key, String value) {

        Object storedObject = data.get(key);

        RedisList list;

        if (storedObject == null) {
            list = new RedisList();
            data.put(key, list);
        } else if (storedObject instanceof RedisList) {
            list = (RedisList) storedObject;
        } else {
            throw new IllegalArgumentException(
                    "Key already contains a different type");
        }

        list.addFirst(value);

        notifyAll();

        return list.size();
    }

    public int llen(String key) {
        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisList)) {
            return 0;
        }

        RedisList list = (RedisList) storedObject;

        return list.size();
    }

    public synchronized String lpop(String key) {
        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisList)) {
            return null;
        }

        RedisList list = (RedisList) storedObject;

        return list.removeFirst();
    }

    public synchronized List<String> lpop(String key, int count) {
        Object storedObject = data.get(key);

        if (!(storedObject instanceof RedisList)) {
            return new ArrayList<>();
        }

        RedisList list = (RedisList) storedObject;

        return list.removeFirst(count);
    }

    public synchronized void waitForListUpdate()
            throws InterruptedException {

        wait();
    }

    public synchronized String[] blockingLpop(
            String key,
            long timeoutMilliseconds)
            throws InterruptedException {

        long deadline = System.currentTimeMillis() + timeoutMilliseconds;

        while (true) {

            Object storedObject = data.get(key);

            if (storedObject instanceof RedisList) {

                RedisList list = (RedisList) storedObject;

                String value = list.removeFirst();

                if (value != null) {
                    return new String[] { key, value };
                }
            }

            if (timeoutMilliseconds == 0) {
                wait();
            } else {

                long remaining = deadline - System.currentTimeMillis();

                if (remaining <= 0) {
                    return null;
                }

                wait(remaining);
            }
        }
    }

}