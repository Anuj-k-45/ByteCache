package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class ReplConfCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context)
            throws IOException {

        /*
         * Replica -> Master:
         *
         * REPLCONF ACK <offset>
         *
         * This is an acknowledgement, not a normal
         * REPLCONF command that needs a response.
         */
        if (arguments.size() >= 3
                && arguments.get(1).equalsIgnoreCase("ACK")) {

            long acknowledgedOffset = Long.parseLong(arguments.get(2));

            store.recordReplicaAck(
                    outputStream,
                    acknowledgedOffset);

            return;
        }

        /*
         * Handshake commands such as:
         *
         * REPLCONF listening-port 6380
         * REPLCONF capa psync2
         *
         * still receive +OK.
         */
        String response = "+OK\r\n";

        outputStream.write(
                response.getBytes(
                        StandardCharsets.UTF_8));

        outputStream.flush();
    }
}