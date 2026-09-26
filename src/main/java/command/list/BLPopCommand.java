package command.list;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class BLPopCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (arguments.size() != 3) {
            sendError(
                    outputStream,
                    "wrong number of arguments for 'blpop' command");
            return;
        }

        String key = arguments.get(1);

        double timeoutSeconds;

        try {
            timeoutSeconds = Double.parseDouble(arguments.get(2));
        } catch (NumberFormatException e) {
            sendError(
                    outputStream,
                    "timeout is not a float or out of range");
            return;
        }

        if (timeoutSeconds < 0) {
            sendError(
                    outputStream,
                    "timeout is not a float or out of range");
            return;
        }

        long timeoutMilliseconds = (long) (timeoutSeconds * 1000);

        try {

            String[] result = store.blockingLpop(
                    key,
                    timeoutMilliseconds);

            // Timeout
            if (result == null) {
                outputStream.write(
                        "*-1\r\n".getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
                return;
            }

            String response = "*2\r\n"
                    + "$"
                    + result[0]
                            .getBytes(StandardCharsets.UTF_8).length
                    + "\r\n"
                    + result[0]
                    + "\r\n"
                    + "$"
                    + result[1]
                            .getBytes(StandardCharsets.UTF_8).length
                    + "\r\n"
                    + result[1]
                    + "\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            sendError(
                    outputStream,
                    "blocking operation interrupted");
        }
    }

    private void sendError(
            OutputStream outputStream,
            String message) throws IOException {

        String response = "-ERR " + message + "\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}