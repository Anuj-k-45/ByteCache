package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class KeysCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context)
            throws IOException {

        /*
         * For this CodeCrafters stage we only
         * support:
         *
         * KEYS *
         */

        if (arguments.size() != 2
                || !arguments.get(1).equals("*")) {

            String response = "-ERR only KEYS * is supported\r\n";

            outputStream.write(
                    response.getBytes(
                            StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        List<String> keys = store.keys();

        StringBuilder response = new StringBuilder();

        /*
         * RESP array.
         *
         * Example:
         *
         * *2\r\n
         * $3\r\nfoo\r\n
         * $3\r\nbar\r\n
         */
        response.append("*")
                .append(keys.size())
                .append("\r\n");

        for (String key : keys) {

            byte[] keyBytes = key.getBytes(
                    StandardCharsets.UTF_8);

            response.append("$")
                    .append(keyBytes.length)
                    .append("\r\n");

            response.append(key)
                    .append("\r\n");
        }

        outputStream.write(
                response.toString()
                        .getBytes(
                                StandardCharsets.UTF_8));

        outputStream.flush();
    }
}