package connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import protocol.RespParser;
import storage.RedisStore;
import storage.StreamEntry;
import storage.StreamId;

public class ClientConnection implements Runnable {

        private final Socket socket;
        private final RespParser parser;
        private final RedisStore store;

        public ClientConnection(
                        Socket socket,
                        RedisStore store) {

                this.socket = socket;
                this.store = store;
                this.parser = new RespParser();
        }

        @Override
        public void run() {

                try (
                                InputStream inputStream = socket.getInputStream();
                                OutputStream outputStream = socket.getOutputStream()) {

                        byte[] buffer = new byte[1024];

                        int bytesRead;

                        while ((bytesRead = inputStream.read(buffer)) != -1) {

                                parser.feed(
                                                buffer,
                                                bytesRead);

                                List<List<String>> commands = parser.getCompleteCommands();

                                for (List<String> command : commands) {

                                        handleCommand(
                                                        command,
                                                        outputStream);
                                }
                        }

                } catch (IOException e) {

                        System.out.println(
                                        "Client error: "
                                                        + e.getMessage());

                } finally {

                        try {
                                socket.close();
                        } catch (IOException e) {
                                // Connection is already closed.
                        }

                        System.out.println("Client disconnected!");
                }
        }

        private void handleCommand(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.isEmpty()) {
                        return;
                }

                String commandName = command.get(0);

                if (commandName.equalsIgnoreCase("PING")) {

                        handlePing(outputStream);

                } else if (commandName.equalsIgnoreCase("ECHO")) {

                        handleEcho(
                                        command,
                                        outputStream);

                } else if (commandName.equalsIgnoreCase("SET")) {

                        handleSet(
                                        command,
                                        outputStream);

                } else if (commandName.equalsIgnoreCase("GET")) {

                        handleGet(
                                        command,
                                        outputStream);

                } else if (commandName.equalsIgnoreCase("TYPE")) {

                        handleType(
                                        command,
                                        outputStream);
                } else if (commandName.equalsIgnoreCase("XADD")) {

                        handleXAdd(
                                        command,
                                        outputStream);
                } else if (commandName.equalsIgnoreCase("XRANGE")) {

                        handleXRange(command, outputStream);

                } else if (commandName.equalsIgnoreCase("XREAD")) {

                        handleXRead(command, outputStream);

                }
        }

        private void handlePing(
                        OutputStream outputStream) throws IOException {

                send(
                                outputStream,
                                "+PONG\r\n");
        }

        private void handleEcho(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.size() != 2) {
                        return;
                }

                String argument = command.get(1);

                sendBulkString(
                                outputStream,
                                argument);
        }

        private void handleSet(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.size() < 3) {
                        return;
                }

                String key = command.get(1);
                String value = command.get(2);

                // Normal SET
                if (command.size() == 3) {

                        store.set(
                                        key,
                                        value);

                        send(
                                        outputStream,
                                        "+OK\r\n");

                        return;
                }

                // SET with PX option
                if (command.size() == 5
                                && command.get(3).equalsIgnoreCase("PX")) {

                        long expiryMilliseconds = Long.parseLong(command.get(4));

                        store.set(
                                        key,
                                        value,
                                        expiryMilliseconds);

                        send(
                                        outputStream,
                                        "+OK\r\n");
                }
        }

        private void handleGet(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.size() != 2) {
                        return;
                }

                String key = command.get(1);

                String value = store.get(key);

                if (value == null) {

                        send(
                                        outputStream,
                                        "$-1\r\n");

                        return;
                }

                sendBulkString(
                                outputStream,
                                value);
        }

        private void handleType(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.size() != 2) {
                        return;
                }

                String key = command.get(1);

                String type = store.getType(key);

                send(
                                outputStream,
                                "+" + type + "\r\n");
        }

        private void handleXAdd(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.size() < 5) {
                        return;
                }

                String streamKey = command.get(1);

                String entryIdString = command.get(2);

                StreamId entryId;

                // Auto-generate complete ID
                if (entryIdString.equals("*")) {

                        try {

                                entryId = store.generateNextStreamId(
                                                streamKey);

                        } catch (IllegalArgumentException e) {

                                sendError(
                                                outputStream,
                                                e.getMessage());

                                return;
                        }

                        // Auto-generate sequence number
                } else if (entryIdString.endsWith("-*")) {

                        String millisecondsPart = entryIdString.substring(
                                        0,
                                        entryIdString.length() - 2);

                        long millisecondsTime;

                        try {

                                millisecondsTime = Long.parseLong(
                                                millisecondsPart);

                        } catch (NumberFormatException e) {

                                sendError(
                                                outputStream,
                                                "Invalid stream ID");

                                return;
                        }

                        try {

                                entryId = store.generateStreamId(
                                                streamKey,
                                                millisecondsTime);

                        } catch (IllegalArgumentException e) {

                                sendError(
                                                outputStream,
                                                e.getMessage());

                                return;
                        }

                        // Explicit ID
                } else {

                        try {

                                entryId = StreamId.parse(
                                                entryIdString);

                        } catch (IllegalArgumentException e) {

                                sendError(
                                                outputStream,
                                                "Invalid stream ID");

                                return;
                        }
                }

                // After the ID, fields and values
                // must come in pairs.
                if ((command.size() - 3) % 2 != 0) {
                        return;
                }

                Map<String, String> fields = new LinkedHashMap<>();

                for (int i = 3; i < command.size(); i += 2) {

                        String field = command.get(i);

                        String value = command.get(i + 1);

                        fields.put(
                                        field,
                                        value);
                }

                StreamEntry entry = new StreamEntry(
                                entryId,
                                fields);

                try {

                        store.addStreamEntry(
                                        streamKey,
                                        entry);

                } catch (IllegalArgumentException e) {

                        sendError(
                                        outputStream,
                                        e.getMessage());

                        return;
                }

                // XADD returns the generated/explicit
                // entry ID as a RESP bulk string.
                sendBulkString(
                                outputStream,
                                entryId.toString());
        }

        private void handleXRange(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

                if (command.size() != 4) {
                        return;
                }

                String key = command.get(1);
                String startIdString = command.get(2);
                String endIdString = command.get(3);

                StreamId startId;
                StreamId endId;

                try {
                        startId = parseRangeStartId(
                                        startIdString);

                        endId = parseRangeEndId(
                                        endIdString);

                } catch (IllegalArgumentException e) {

                        sendError(
                                        outputStream,
                                        "Invalid stream ID");

                        return;
                }

                List<StreamEntry> entries = store.getStreamRange(
                                key,
                                startId,
                                endId);

                sendStreamEntries(
                                outputStream,
                                entries);
        }

        private void send(
                        OutputStream outputStream,
                        String response) throws IOException {

                outputStream.write(
                                response.getBytes(
                                                StandardCharsets.UTF_8));

                outputStream.flush();
        }

        private void sendBulkString(
                        OutputStream outputStream,
                        String value) throws IOException {

                byte[] valueBytes = value.getBytes(
                                StandardCharsets.UTF_8);

                String header = "$"
                                + valueBytes.length
                                + "\r\n";

                outputStream.write(
                                header.getBytes(
                                                StandardCharsets.UTF_8));

                outputStream.write(
                                valueBytes);

                outputStream.write(
                                "\r\n".getBytes(
                                                StandardCharsets.UTF_8));

                outputStream.flush();
        }

        private void handleXRead(
                        List<String> command,
                        OutputStream outputStream) throws IOException {

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
        
        private void sendError(
                        OutputStream outputStream,
                        String message) throws IOException {

                send(
                                outputStream,
                                "-ERR " + message + "\r\n");
        }

        private StreamId parseRangeStartId(
                        String id) {

                if (id.equals("-")) {
                        return new StreamId(
                                        0,
                                        0);
                }

                if (id.contains("-")) {
                        return StreamId.parse(id);
                }

                long millisecondsTime = Long.parseLong(id);

                return new StreamId(
                                millisecondsTime,
                                0);
        }

        private StreamId parseRangeEndId(
                        String id) {

                if (id.equals("+")) {
                        return new StreamId(
                                        Long.MAX_VALUE,
                                        Long.MAX_VALUE);
                }

                if (id.contains("-")) {
                        return StreamId.parse(id);
                }

                long millisecondsTime = Long.parseLong(id);

                return new StreamId(
                                millisecondsTime,
                                Long.MAX_VALUE);
        }

        private void sendStreamEntries(
                        OutputStream outputStream,
                        List<StreamEntry> entries) throws IOException {

                send(
                                outputStream,
                                "*" + entries.size() + "\r\n");

                for (StreamEntry entry : entries) {

                        send(
                                        outputStream,
                                        "*2\r\n");

                        sendBulkString(
                                        outputStream,
                                        entry.getId().toString());

                        Map<String, String> fields = entry.getFields();

                        send(
                                        outputStream,
                                        "*" + (fields.size() * 2) + "\r\n");

                        for (Map.Entry<String, String> field : fields.entrySet()) {

                                sendBulkString(
                                                outputStream,
                                                field.getKey());

                                sendBulkString(
                                                outputStream,
                                                field.getValue());
                        }
                }
        }

        private void sendXReadResponse(
                        OutputStream outputStream,
                        List<XReadResult> results) throws IOException {

                send(
                                outputStream,
                                "*" + results.size() + "\r\n");

                for (XReadResult result : results) {

                        send(
                                        outputStream,
                                        "*2\r\n");

                        sendBulkString(
                                        outputStream,
                                        result.key);

                        send(
                                        outputStream,
                                        "*" + result.entries.size() + "\r\n");

                        for (StreamEntry entry : result.entries) {

                                send(
                                                outputStream,
                                                "*2\r\n");

                                sendBulkString(
                                                outputStream,
                                                entry.getId().toString());

                                Map<String, String> fields = entry.getFields();

                                send(
                                                outputStream,
                                                "*" + (fields.size() * 2) + "\r\n");

                                for (Map.Entry<String, String> field : fields.entrySet()) {

                                        sendBulkString(
                                                        outputStream,
                                                        field.getKey());

                                        sendBulkString(
                                                        outputStream,
                                                        field.getValue());
                                }
                        }
                }
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

        private boolean hasEntries(
                        List<XReadResult> results) {

                for (XReadResult result : results) {

                        if (!result.entries.isEmpty()) {
                                return true;
                        }
                }

                return false;
        }
}
