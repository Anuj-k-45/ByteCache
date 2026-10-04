package server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.CommandDispatcher;
import connection.ClientContext;
import connection.NullOutputStream;
import protocol.RespParser;
import storage.RedisStore;

public class ReplicationConnection {

        private final String masterHost;
        private final int masterPort;
        private final int replicaPort;
        private final RedisStore store;

        private final RespParser parser;
        private final CommandDispatcher commandDispatcher;
        private final ClientContext context;

        private long replicationOffset = 0;

        public ReplicationConnection(
                        String masterHost,
                        int masterPort,
                        int replicaPort,
                        RedisStore store) {

                this.masterHost = masterHost;
                this.masterPort = masterPort;
                this.replicaPort = replicaPort;
                this.store = store;
                this.parser = new RespParser();
                this.commandDispatcher = new CommandDispatcher();
                this.context = new ClientContext();
        }

        public void connectAndHandshake()
                        throws IOException {

                Socket socket = new Socket(
                                masterHost,
                                masterPort);

                System.out.println(
                                "Connected to master "
                                                + masterHost
                                                + ":"
                                                + masterPort);

                InputStream inputStream = socket.getInputStream();

                OutputStream outputStream = socket.getOutputStream();

                // -------------------------------------------------
                // STEP 1: PING
                // -------------------------------------------------

                String ping = "*1\r\n"
                                + "$4\r\n"
                                + "PING\r\n";

                outputStream.write(
                                ping.getBytes(StandardCharsets.UTF_8));

                outputStream.flush();

                System.out.println(
                                "PING sent to master");

                // Wait for +PONG
                readResponse(inputStream);

                System.out.println(
                                "PONG received from master");

                // -------------------------------------------------
                // STEP 2: REPLCONF listening-port
                // -------------------------------------------------

                String port = String.valueOf(replicaPort);

                String listeningPort = "*3\r\n"
                                + "$8\r\n"
                                + "REPLCONF\r\n"
                                + "$14\r\n"
                                + "listening-port\r\n"
                                + "$" + port.length() + "\r\n"
                                + port
                                + "\r\n";

                outputStream.write(
                                listeningPort.getBytes(
                                                StandardCharsets.UTF_8));

                outputStream.flush();

                System.out.println(
                                "REPLCONF listening-port sent");

                // Wait for +OK
                readResponse(inputStream);

                System.out.println(
                                "REPLCONF listening-port acknowledged");

                // -------------------------------------------------
                // STEP 3: REPLCONF capa psync2
                // -------------------------------------------------

                String capability = "*3\r\n"
                                + "$8\r\n"
                                + "REPLCONF\r\n"
                                + "$4\r\n"
                                + "capa\r\n"
                                + "$6\r\n"
                                + "psync2\r\n";

                outputStream.write(
                                capability.getBytes(
                                                StandardCharsets.UTF_8));

                outputStream.flush();

                System.out.println(
                                "REPLCONF capa psync2 sent");

                // Wait for +OK
                readResponse(inputStream);

                System.out.println(
                                "REPLCONF capa psync2 acknowledged");

                // -------------------------------------------------
                // STEP 4: PSYNC
                // -------------------------------------------------
                String psync = "*3\r\n"
                                + "$5\r\n"
                                + "PSYNC\r\n"
                                + "$1\r\n"
                                + "?\r\n"
                                + "$2\r\n"
                                + "-1\r\n";

                outputStream.write(
                                psync.getBytes(
                                                StandardCharsets.UTF_8));

                outputStream.flush();

                System.out.println(
                                "PSYNC ? -1 sent to master");

                // Read FULLRESYNC
                readResponse(inputStream);

                System.out.println(
                                "FULLRESYNC received from master");

                // Read RDB header + contents
                byte[] rdbFile = readRdbFile(inputStream);

                System.out.println(
                                "RDB received: "
                                                + rdbFile.length
                                                + " bytes");

                listenForReplicationCommands(
                                inputStream,
                                outputStream);
        }

        private void readResponse(
                        InputStream inputStream)
                        throws IOException {

                StringBuilder response = new StringBuilder();

                int previous = -1;
                int current;

                while ((current = inputStream.read()) != -1) {

                        response.append((char) current);

                        if (previous == '\r'
                                        && current == '\n') {

                                break;
                        }

                        previous = current;
                }

                System.out.println(
                                "Master response: "
                                                + response);
        }

        private byte[] readRdbFile(
                        InputStream inputStream)
                        throws IOException {

                // Read: $<length>\r\n
                StringBuilder header = new StringBuilder();

                int previous = -1;
                int current;

                while ((current = inputStream.read()) != -1) {

                        header.append((char) current);

                        if (previous == '\r'
                                        && current == '\n') {

                                break;
                        }

                        previous = current;
                }

                String headerString = header.toString();

                System.out.println(
                                "RDB header: "
                                                + headerString);

                // Example:
                // "$88\r\n"

                int length = Integer.parseInt(
                                headerString.substring(
                                                1,
                                                headerString.length() - 2));

                System.out.println(
                                "RDB length: "
                                                + length);

                // Read exactly <length> bytes
                byte[] rdbFile = new byte[length];

                int totalRead = 0;

                while (totalRead < length) {

                        int bytesRead = inputStream.read(
                                        rdbFile,
                                        totalRead,
                                        length - totalRead);

                        if (bytesRead == -1) {

                                throw new IOException(
                                                "Connection closed while reading RDB");
                        }

                        totalRead += bytesRead;
                }

                return rdbFile;
        }

        private void listenForReplicationCommands(
                        InputStream inputStream,
                        OutputStream outputStream)
                        throws IOException {

                byte[] buffer = new byte[1024];

                while (true) {

                        int bytesRead = inputStream.read(buffer);

                        if (bytesRead == -1) {

                                System.out.println(
                                                "Master closed replication connection");

                                break;
                        }

                        parser.feed(buffer, bytesRead);

                        var commands = parser.getCompleteCommands();

                        for (var command : commands) {

                                if (isGetAck(command)) {

                                        // ACK uses offset BEFORE this command
                                        sendAck(outputStream);

                                        // Now this GETACK becomes part of processed history
                                        replicationOffset += calculateRespSize(command);

                                } else {

                                        commandDispatcher.dispatch(
                                                        command,
                                                        new NullOutputStream(),
                                                        store,
                                                        context);

                                        replicationOffset += calculateRespSize(command);
                                }
                        }
                }
        }

        private boolean isGetAck(
                        List<String> command) {

                return command.size() >= 2
                                && command.get(0).equalsIgnoreCase("REPLCONF")
                                && command.get(1).equalsIgnoreCase("GETACK");
        }

        private void sendAck(
                        OutputStream outputStream)
                        throws IOException {

                String offset = String.valueOf(replicationOffset);

                String response = "*3\r\n"
                                + "$8\r\n"
                                + "REPLCONF\r\n"
                                + "$3\r\n"
                                + "ACK\r\n"
                                + "$" + offset.length() + "\r\n"
                                + offset
                                + "\r\n";

                outputStream.write(
                                response.getBytes(StandardCharsets.UTF_8));

                outputStream.flush();

                System.out.println(
                                "Sent REPLCONF ACK "
                                                + replicationOffset);
        }

        private int calculateRespSize(
                        List<String> command) {

                int size = 0;

                // *<number of elements>\r\n
                size += 1;
                size += String.valueOf(command.size()).length();
                size += 2;

                for (String argument : command) {

                        byte[] bytes = argument.getBytes(StandardCharsets.UTF_8);

                        // $<byte-length>\r\n
                        size += 1;
                        size += String.valueOf(bytes.length).length();
                        size += 2;

                        // actual data + \r\n
                        size += bytes.length;
                        size += 2;
                }

                return size;
        }

        public synchronized long getReplicationOffset() {
                return replicationOffset;
        }

}