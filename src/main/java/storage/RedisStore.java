package storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import connection.ClientContext;

public class RedisStore {
    private final boolean replica;

    private final String dir;
    private final String dbfilename;

    private final String replicationId;
    private long replicationOffset;

    private final List<OutputStream> replicaConnections;
    private final Map<OutputStream, Long> replicaAcknowledgedOffsets;

    private final Map<String, Object> data = new ConcurrentHashMap<>();

    private final Map<String, Set<ClientContext>> watchers = new ConcurrentHashMap<>();

    public RedisStore(
            boolean replica,
            String dir,
            String dbfilename) {

        this.replica = replica;

        this.dir = dir;
        this.dbfilename = dbfilename;

        this.replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

        this.replicationOffset = 0;

        this.replicaConnections = new CopyOnWriteArrayList<>();

        this.replicaAcknowledgedOffsets = new ConcurrentHashMap<>();
    }

    public boolean isReplica() {
        return replica;
    }

    public String getDir() {
        return dir;
    }

    public List<String> keys() {

        List<String> keys = new ArrayList<>();

        for (String key : data.keySet()) {

            /*
             * Calling get() also removes an expired
             * StoredValue.
             */
            Object value = data.get(key);

            if (value instanceof StoredValue) {

                if (((StoredValue) value).isExpired()) {

                    data.remove(key);

                    continue;
                }
            }

            keys.add(key);
        }

        return keys;
    }

    public String getDbfilename() {
        return dbfilename;
    }

    public int getReplicaCount() {
        return replicaConnections.size();
    }

    public String getReplicationId() {
        return replicationId;
    }

    public long getReplicationOffset() {
        return replicationOffset;
    }

    public synchronized void addReplica(
            OutputStream outputStream) {

        replicaConnections.add(outputStream);

        replicaAcknowledgedOffsets.put(
                outputStream,
                0L);

        System.out.println(
                "Replica registered. Total replicas: "
                        + replicaConnections.size());
    }

    public void propagateCommand(
            List<String> command)
            throws IOException {

        byte[] data = encodeRespCommand(command);

        /*
         * The master's replication offset represents
         * the number of bytes produced in the
         * replication stream.
         *
         * This write command is part of that stream,
         * so advance the offset exactly once.
         */
        synchronized (this) {

            replicationOffset += data.length;
        }

        for (OutputStream replica : replicaConnections) {

            try {

                synchronized (replica) {

                    replica.write(data);
                    replica.flush();
                }

            } catch (IOException e) {

                replicaConnections.remove(replica);
                replicaAcknowledgedOffsets.remove(replica);

                System.out.println(
                        "Replica disconnected. Remaining replicas: "
                                + replicaConnections.size());
            }
        }
    }

    public synchronized void recordReplicaAck(
            OutputStream replica,
            long acknowledgedOffset) {

        if (!replicaAcknowledgedOffsets.containsKey(replica)) {
            return;
        }

        replicaAcknowledgedOffsets.put(
                replica,
                acknowledgedOffset);

        System.out.println(
                "Replica ACK received: "
                        + acknowledgedOffset);

        notifyAll();
    }

    private int countAcknowledgedReplicas(
            long targetOffset) {

        int count = 0;

        for (OutputStream replica : replicaConnections) {

            Long acknowledgedOffset = replicaAcknowledgedOffsets.get(replica);

            if (acknowledgedOffset != null
                    && acknowledgedOffset >= targetOffset) {

                count++;
            }
        }
        return count;
    }

    public int waitForReplicas(
            int requiredReplicas,
            long timeoutMilliseconds)
            throws IOException, InterruptedException {

        /*
         * WAIT 0 can return immediately.
         */
        if (requiredReplicas <= 0) {
            return getReplicaCount();
        }

        long targetOffset;

        List<OutputStream> replicas;

        /*
         * Take a snapshot of the current state.
         *
         * We do not hold the RedisStore monitor while
         * writing GETACK commands because ACK processing
         * also needs this monitor.
         */
        synchronized (this) {

            targetOffset = replicationOffset;

            replicas = new ArrayList<>(
                    replicaConnections);
        }

        /*
         * If there have been no writes, all currently
         * connected replicas are already at offset 0.
         */
        if (targetOffset == 0) {
            return replicas.size();
        }

        /*
         * Check whether enough replicas have already
         * acknowledged the target offset.
         */
        synchronized (this) {

            int acknowledged = countAcknowledgedReplicas(
                    targetOffset);

            if (acknowledged >= requiredReplicas) {
                return acknowledged;
            }
        }

        /*
         * Ask every replica for its current offset.
         */
        for (OutputStream replica : replicas) {

            try {

                String getAck = "*3\r\n"
                        + "$8\r\n"
                        + "REPLCONF\r\n"
                        + "$6\r\n"
                        + "GETACK\r\n"
                        + "$1\r\n"
                        + "*\r\n";

                synchronized (replica) {

                    replica.write(
                            getAck.getBytes(
                                    StandardCharsets.UTF_8));

                    replica.flush();
                }

            } catch (IOException e) {

                replicaConnections.remove(replica);
                replicaAcknowledgedOffsets.remove(replica);
            }
        }

        /*
         * timeout = 0 means wait indefinitely.
         */
        long deadline = System.currentTimeMillis()
                + timeoutMilliseconds;

        synchronized (this) {

            while (true) {

                int acknowledged = countAcknowledgedReplicas(
                        targetOffset);

                /*
                 * Required number reached.
                 */
                if (acknowledged >= requiredReplicas) {
                    return acknowledged;
                }

                /*
                 * Infinite wait.
                 */
                if (timeoutMilliseconds == 0) {

                    wait();

                    continue;
                }

                /*
                 * Calculate remaining time.
                 */
                long remaining = deadline
                        - System.currentTimeMillis();

                /*
                 * Timeout expired.
                 */
                if (remaining <= 0) {

                    return countAcknowledgedReplicas(
                            targetOffset);
                }

                /*
                 * Wait until either:
                 *
                 * 1. An ACK arrives and calls notifyAll()
                 * 2. The timeout expires
                 */
                wait(remaining);
            }
        }
    }

    private byte[] encodeRespCommand(
            List<String> command) {

        StringBuilder response = new StringBuilder();

        response.append("*")
                .append(command.size())
                .append("\r\n");

        for (String argument : command) {

            byte[] bytes = argument.getBytes(
                    StandardCharsets.UTF_8);

            response.append("$")
                    .append(bytes.length)
                    .append("\r\n");

            response.append(argument)
                    .append("\r\n");
        }

        return response.toString()
                .getBytes(StandardCharsets.UTF_8);
    }

    public void set(
            String key,
            String value) {

        data.put(
                key,
                new StoredValue(value, null));

        markKeyModified(key);
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

        markKeyModified(key);
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

    public synchronized void watchKey(
            String key,
            ClientContext context) {

        watchers
                .computeIfAbsent(
                        key,
                        k -> new HashSet<>())
                .add(context);

        context.watchKey(key);
    }

    private void markKeyModified(String key) {

        Set<ClientContext> clients = watchers.get(key);

        if (clients == null) {
            return;
        }

        for (ClientContext context : clients) {
            context.markWatchDirty();
        }
    }

    public synchronized void clearWatchState(
            ClientContext context) {

        for (String key : context.getWatchedKeys()) {

            Set<ClientContext> clients = watchers.get(key);

            if (clients == null) {
                continue;
            }

            clients.remove(context);

            if (clients.isEmpty()) {
                watchers.remove(key);
            }
        }

        context.clearWatchState();
    }

}