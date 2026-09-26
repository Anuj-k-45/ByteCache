package command.list;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class LPushCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (arguments.size() < 3) {
            sendError(
                    outputStream,
                    "wrong number of arguments for 'lpush' command");
            return;
        }

        String key = arguments.get(1);

        int size = 0;

        for (int i = 2; i < arguments.size(); i++) {
            size = store.lpush(key, arguments.get(i));
        }

        String response = ":" + size + "\r\n";

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