package command.string;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class SetCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (arguments.size() < 3) {
            sendError(
                    outputStream,
                    "wrong number of arguments for 'set' command");
            return;
        }

        String key = arguments.get(1);
        String value = arguments.get(2);

        // Basic SET
        if (arguments.size() == 3) {

            store.set(key, value);

            sendSimpleString(
                    outputStream,
                    "OK");

            return;
        }

        // SET with options
        if (arguments.size() >= 5) {

            String option = arguments.get(3);

            if (option.equalsIgnoreCase("PX")) {

                long expiryMilliseconds;

                try {
                    expiryMilliseconds = Long.parseLong(arguments.get(4));

                } catch (NumberFormatException e) {

                    sendError(
                            outputStream,
                            "invalid expire time in 'set' command");

                    return;
                }

                if (expiryMilliseconds < 0) {

                    sendError(
                            outputStream,
                            "invalid expire time in 'set' command");

                    return;
                }

                store.set(
                        key,
                        value,
                        expiryMilliseconds);

                sendSimpleString(
                        outputStream,
                        "OK");

                return;
            }
        }

        sendError(
                outputStream,
                "syntax error");
    }

    private void sendSimpleString(
            OutputStream outputStream,
            String message) throws IOException {

        String response = "+" + message + "\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
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