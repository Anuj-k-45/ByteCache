package command.transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class DiscardCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (!context.isInTransaction()) {
            String response = "-ERR DISCARD without MULTI\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();

            return;
        }

        context.endTransaction();

        String response = "+OK\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
    }
}