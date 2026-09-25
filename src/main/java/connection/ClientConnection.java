package connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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

                try {

                        entryId = StreamId.parse(entryIdString);

                } catch (IllegalArgumentException e) {

                        sendError(
                                        outputStream,
                                        "Invalid stream ID");

                        return;
                }

                if ((command.size() - 3) % 2 != 0) {
                        return;
                }

                Map<String, String> fields = new HashMap<>();

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

                sendBulkString(
                                outputStream,
                                entryId.toString());
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

        private void sendError(
                        OutputStream outputStream,
                        String message) throws IOException {

                send(
                                outputStream,
                                "-ERR " + message + "\r\n");
        }
}
