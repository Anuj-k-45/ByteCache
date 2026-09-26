package command.numeric;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import storage.RedisStore;

public class IncrCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store) throws IOException {

        if (arguments.size() < 2) {
            sendError(
                    outputStream,
                    "wrong number of arguments for 'incr' command");
            return;
        }

        String key = arguments.get(1);

        String value = store.get(key);

        // Key does not exist.
        // For INCR, Redis creates it with value 1.
        if (value == null) {

            store.set(key, "1");

            sendInteger(
                    outputStream,
                    1);

            return;
        }

        long number;

        try {

            number = Long.parseLong(value);

        } catch (NumberFormatException e) {

            sendError(
                    outputStream,
                    "value is not an integer or out of range");

            return;
        }

        long incrementedValue = number + 1;

        store.set(
                key,
                String.valueOf(incrementedValue));

        sendInteger(
                outputStream,
                incrementedValue);
    }

    private void sendInteger(
            OutputStream outputStream,
            long value) throws IOException {

        String response = ":" + value + "\r\n";

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