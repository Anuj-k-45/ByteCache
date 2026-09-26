package command.list;

import command.Command;
import connection.ClientContext;
import storage.RedisList;
import storage.RedisStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LRangeCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (arguments.size() != 4) {
            sendError(
                    outputStream,
                    "wrong number of arguments for 'lrange' command");
            return;
        }

        String key = arguments.get(1);

        int start;
        int stop;

        try {
            start = Integer.parseInt(arguments.get(2));
            stop = Integer.parseInt(arguments.get(3));
        } catch (NumberFormatException e) {
            sendError(
                    outputStream,
                    "value is not an integer or out of range");
            return;
        }

        List<String> values = store.lrange(key, start, stop);

        StringBuilder response = new StringBuilder("*" + values.size() + "\r\n");

        for (String value : values) {
            response.append("$")
                    .append(value.getBytes(StandardCharsets.UTF_8).length)
                    .append("\r\n")
                    .append(value)
                    .append("\r\n");
        }

        outputStream.write(
                response.toString().getBytes(StandardCharsets.UTF_8));

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