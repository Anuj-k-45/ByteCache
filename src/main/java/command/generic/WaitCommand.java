package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class WaitCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context)
            throws IOException {

        if (arguments.size() < 3) {

            String response = "-ERR wrong number of arguments for 'WAIT' command\r\n";

            outputStream.write(
                    response.getBytes(
                            StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        int requiredReplicas = Integer.parseInt(
                arguments.get(1));

        long timeoutMilliseconds = Long.parseLong(
                arguments.get(2));

        try {

            int acknowledgedReplicas = store.waitForReplicas(
                    requiredReplicas,
                    timeoutMilliseconds);

            String response = ":" + acknowledgedReplicas + "\r\n";

            outputStream.write(
                    response.getBytes(
                            StandardCharsets.UTF_8));

            outputStream.flush();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            String response = ":0\r\n";

            outputStream.write(
                    response.getBytes(
                            StandardCharsets.UTF_8));

            outputStream.flush();
        }
    }
}