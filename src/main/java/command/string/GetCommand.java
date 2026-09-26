package command.string;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class GetCommand implements Command {

        @Override
        public void execute(
                        List<String> arguments,
                        OutputStream outputStream,
                        RedisStore store,
                        ClientContext context) throws IOException {

                if (arguments.size() < 2) {
                        outputStream.write(
                                        "-ERR wrong number of arguments for 'get' command\r\n"
                                                        .getBytes(StandardCharsets.UTF_8));
                        outputStream.flush();
                        return;
                }

                String key = arguments.get(1);

                String value = store.get(key);

                if (value == null) {
                        outputStream.write(
                                        "$-1\r\n".getBytes(StandardCharsets.UTF_8));
                        outputStream.flush();
                        return;
                }

                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

                String response = "$" + valueBytes.length + "\r\n"
                                + value
                                + "\r\n";

                outputStream.write(
                                response.getBytes(StandardCharsets.UTF_8));

                outputStream.flush();
        }
}