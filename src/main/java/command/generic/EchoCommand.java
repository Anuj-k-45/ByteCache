package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class EchoCommand implements Command {

        @Override
        public void execute(
                        List<String> arguments,
                        OutputStream outputStream,
                        RedisStore store,
                        ClientContext context) throws IOException {

                if (arguments.size() < 2) {
                        outputStream.write(
                                        "-ERR wrong number of arguments for 'echo' command\r\n"
                                                        .getBytes(StandardCharsets.UTF_8));
                        outputStream.flush();
                        return;
                }

                String message = arguments.get(1);

                String response = "$" + message.getBytes(StandardCharsets.UTF_8).length
                                + "\r\n"
                                + message
                                + "\r\n";

                outputStream.write(
                                response.getBytes(StandardCharsets.UTF_8));

                outputStream.flush();
        }
}