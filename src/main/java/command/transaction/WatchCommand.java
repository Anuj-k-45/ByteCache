package command.transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class WatchCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (context.isInTransaction()) {

            String response = "-ERR WATCH inside MULTI is not allowed\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        if (arguments.size() < 2) {

            String response = "-ERR wrong number of arguments for 'watch' command\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        /*
         * arguments[0] = WATCH
         * arguments[1...] = keys
         */
        for (int i = 1; i < arguments.size(); i++) {

            String key = arguments.get(i);

            store.watchKey(
                    key,
                    context);
        }

        String response = "+OK\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}