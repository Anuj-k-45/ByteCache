package command.list;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class LPopCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (arguments.size() != 2 && arguments.size() != 3) {
            sendError(
                    outputStream,
                    "wrong number of arguments for 'lpop' command");
            return;
        }

        String key = arguments.get(1);

        // LPOP key
        if (arguments.size() == 2) {

            String value = store.lpop(key);

            if (value == null) {
                outputStream.write(
                        "$-1\r\n".getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
                return;
            }

            sendBulkString(outputStream, value);
            return;
        }

        // LPOP key count
        int count;

        try {
            count = Integer.parseInt(arguments.get(2));
        } catch (NumberFormatException e) {
            sendError(
                    outputStream,
                    "value is not an integer or out of range");
            return;
        }

        if (count < 0) {
            sendError(
                    outputStream,
                    "value is not an integer or out of range");
            return;
        }

        List<String> values = store.lpop(key, count);

        StringBuilder response = new StringBuilder("*" + values.size() + "\r\n");

        for (String value : values) {
            response.append("$")
                    .append(
                            value.getBytes(StandardCharsets.UTF_8).length)
                    .append("\r\n")
                    .append(value)
                    .append("\r\n");
        }

        outputStream.write(
                response.toString().getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }

    private void sendBulkString(
            OutputStream outputStream,
            String value) throws IOException {

        String response = "$" +
                value.getBytes(StandardCharsets.UTF_8).length +
                "\r\n" +
                value +
                "\r\n";

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