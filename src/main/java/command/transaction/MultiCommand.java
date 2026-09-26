package command.transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class MultiCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        context.startTransaction();

        outputStream.write(
                "+OK\r\n".getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}