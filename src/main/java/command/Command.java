package command;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import connection.ClientContext;
import storage.RedisStore;

public interface Command {

    void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException;
}