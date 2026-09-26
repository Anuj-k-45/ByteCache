package connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

import command.CommandDispatcher;
import protocol.RespParser;
import storage.RedisStore;

public class ClientConnection implements Runnable {

        private final Socket socket;
        private final RespParser parser;
        private final RedisStore store;
        private final CommandDispatcher commandDispatcher;

        public ClientConnection(
                        Socket socket,
                        RedisStore store) {

                this.socket = socket;
                this.store = store;
                this.parser = new RespParser();
                this.commandDispatcher = new CommandDispatcher();
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

                String commandName = command.get(0).toUpperCase();

                // New command architecture
                if (commandName.equals("PING")
                                || commandName.equals("ECHO")
                                || commandName.equals("SET")
                                || commandName.equals("GET")
                                || commandName.equals("TYPE")
                                || commandName.equals("XADD")
                                || commandName.equals("XRANGE")
                                || commandName.equals("XREAD")
                                || commandName.equals("INCR")
                        ) {

                        commandDispatcher.dispatch(
                                        command,
                                        outputStream,
                                        store);
                }
        }
}
