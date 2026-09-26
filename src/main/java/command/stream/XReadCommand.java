package command.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import command.Command;
import storage.RedisStore;
import storage.StreamEntry;
import storage.StreamId;

public class XReadCommand implements Command {

    @Override
    public void execute(
            List<String> command,
            OutputStream outputStream,
            RedisStore store) throws IOException {

        int index = 1;

        boolean blocking = false;
        long blockTimeout = 0;

        // Check for BLOCK option
        if (command.get(index).equalsIgnoreCase("BLOCK")) {

            blocking = true;

            blockTimeout = Long.parseLong(
                    command.get(index + 1));

            index += 2;
        }

        // STREAMS must come next
        if (!command.get(index).equalsIgnoreCase("STREAMS")) {
            return;
        }

        index++;

        int remainingArguments = command.size() - index;

        // Half are keys, half are IDs
        if (remainingArguments % 2 != 0) {
            return;
        }

        int streamCount = remainingArguments / 2;

        List<XReadResult> results = new ArrayList<>();

        // Store the resolved starting IDs.
        // This is important for "$".
        List<StreamId> startIds = new ArrayList<>();

        // ---------------------------------
        // Read initial stream state
        // ---------------------------------

        for (int i = 0; i < streamCount; i++) {

            String key = command.get(index + i);

            String startIdString = command.get(
                    index + streamCount + i);

            StreamId startId;

            try {

                if (startIdString.equals("$")) {

                    // "$" means:
                    // use the current last ID of the stream.
                    startId = store.getStreamLastId(key);

                } else {

                    startId = StreamId.parse(startIdString);
                }

            } catch (IllegalArgumentException e) {

                sendError(
                        outputStream,
                        "Invalid stream ID");

                return;
            }

            // Remember this ID.
            // If we block and wake up later,
            // we must continue using this same ID.
            startIds.add(startId);

            List<StreamEntry> entries = store.getStreamEntriesAfter(
                    key,
                    startId);

            results.add(
                    new XReadResult(
                            key,
                            entries));
        }

        // ---------------------------------
        // Check whether we already have data
        // ---------------------------------

        if (hasEntries(results)) {

            sendXReadResponse(
                    outputStream,
                    results);

            return;
        }

        // ---------------------------------
        // Normal XREAD
        // ---------------------------------

        if (!blocking) {

            sendXReadResponse(
                    outputStream,
                    results);

            return;
        }

        // ---------------------------------
        // BLOCKING PART
        // ---------------------------------

        long deadline;

        if (blockTimeout == 0) {

            // BLOCK 0 means wait indefinitely.
            deadline = Long.MAX_VALUE;

        } else {

            deadline = System.currentTimeMillis()
                    + blockTimeout;
        }

        while (true) {

            long remaining;

            if (blockTimeout == 0) {

                // wait(0) means wait indefinitely.
                remaining = 0;

            } else {

                remaining = deadline
                        - System.currentTimeMillis();

                if (remaining <= 0) {

                    // Timeout reached.
                    send(
                            outputStream,
                            "*-1\r\n");

                    return;
                }
            }

            try {

                store.waitForStreamUpdate(
                        remaining);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                return;
            }

            // ---------------------------------
            // Something changed.
            // Check the streams again.
            // ---------------------------------

            results.clear();

            for (int i = 0; i < streamCount; i++) {

                String key = command.get(index + i);

                // IMPORTANT:
                // Use the same starting ID that
                // we captured before blocking.
                StreamId startId = startIds.get(i);

                List<StreamEntry> entries = store.getStreamEntriesAfter(
                        key,
                        startId);

                results.add(
                        new XReadResult(
                                key,
                                entries));
            }

            // If new entries exist, return immediately.
            if (hasEntries(results)) {

                sendXReadResponse(
                        outputStream,
                        results);

                return;
            }

            // ---------------------------------
            // Check timeout again
            // ---------------------------------

            if (blockTimeout != 0
                    && System.currentTimeMillis() >= deadline) {

                send(
                        outputStream,
                        "*-1\r\n");

                return;
            }
        }
    }

    private void send(
            OutputStream outputStream,
            String response) throws IOException {

        outputStream.write(
                response.getBytes(
                        StandardCharsets.UTF_8));

        outputStream.flush();
    }

    private void sendError(
            OutputStream outputStream,
            String message) throws IOException {

        send(
                outputStream,
                "-ERR " + message + "\r\n");
    }

    private void sendXReadResponse(
            OutputStream outputStream,
            List<XReadResult> results)
            throws IOException {

        StringBuilder response = new StringBuilder();

        int streamsWithEntries = 0;

        for (XReadResult result : results) {

            if (!result.entries.isEmpty()) {
                streamsWithEntries++;
            }
        }

        response.append("*")
                .append(streamsWithEntries)
                .append("\r\n");

        for (XReadResult result : results) {

            if (result.entries.isEmpty()) {
                continue;
            }

            response.append("*2\r\n");

            appendBulkString(
                    response,
                    result.key);

            response.append("*")
                    .append(result.entries.size())
                    .append("\r\n");

            for (StreamEntry entry : result.entries) {

                response.append("*2\r\n");

                appendBulkString(
                        response,
                        entry.getId().toString());

                Map<String, String> fields = entry.getFields();

                response.append("*")
                        .append(fields.size() * 2)
                        .append("\r\n");

                for (Map.Entry<String, String> field : fields.entrySet()) {

                    appendBulkString(
                            response,
                            field.getKey());

                    appendBulkString(
                            response,
                            field.getValue());
                }
            }
        }

        send(
                outputStream,
                response.toString());
    }

    private void appendBulkString(
            StringBuilder response,
            String value) {

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        response.append("$")
                .append(bytes.length)
                .append("\r\n");

        response.append(value)
                .append("\r\n");
    }

    private boolean hasEntries(
            List<XReadResult> results) {

        for (XReadResult result : results) {

            if (!result.entries.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    private static class XReadResult {

        private final String key;
        private final List<StreamEntry> entries;

        private XReadResult(
                String key,
                List<StreamEntry> entries) {

            this.key = key;
            this.entries = entries;
        }
    }
}