package command.transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class ExecCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (!context.isInTransaction()) {

            outputStream.write(
                    "-ERR EXEC without MULTI\r\n"
                            .getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        // Empty transaction for now.
        context.endTransaction();

        outputStream.write(
                "*0\r\n".getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}