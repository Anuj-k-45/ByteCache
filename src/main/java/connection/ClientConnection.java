package connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import protocol.RespParser;
import storage.RedisStore;

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

            System.out.println(
                    "Client disconnected!");
        }
    }

    private void handleCommand(
            List<String> command,
            OutputStream outputStream) throws IOException {

        if (command.isEmpty()) {
            return;
        }

        String commandName = command.get(0);

        if (commandName.equalsIgnoreCase(
                "PING")) {

            handlePing(outputStream);

        } else if (commandName.equalsIgnoreCase(
                "ECHO")) {

            handleEcho(
                    command,
                    outputStream);

        } else if (commandName.equalsIgnoreCase(
                "SET")) {

            handleSet(
                    command,
                    outputStream);

        } else if (commandName.equalsIgnoreCase(
                "GET")) {

            handleGet(
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

        if (command.size() != 3) {
            return;
        }

        String key = command.get(1);

        String value = command.get(2);

        store.set(
                key,
                value);

        send(
                outputStream,
                "+OK\r\n");
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

    private void send(
            OutputStream outputStream,
            String response) throws IOException {

        outputStream.write(
                response.getBytes(
                        StandardCharsets.UTF_8));
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
    }
}