package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class InfoCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        String role;

        if (store.isReplica()) {
            role = "slave";
        } else {
            role = "master";
        }

        String responseBody = "role:" + role + "\r\n";

        byte[] bodyBytes = responseBody.getBytes(StandardCharsets.UTF_8);

        String response = "$" + bodyBytes.length + "\r\n"
                + responseBody
                + "\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}