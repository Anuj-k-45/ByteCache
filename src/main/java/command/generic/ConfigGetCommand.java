package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class ConfigGetCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context)
            throws IOException {

        /*
         * Command format:
         *
         * CONFIG GET <parameter>
         *
         * arguments:
         *
         * [0] = CONFIG
         * [1] = GET
         * [2] = parameter
         */

        if (arguments.size() < 3) {

            String response = "-ERR wrong number of arguments for 'config' command\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        String subCommand = arguments.get(1).toLowerCase();

        if (!subCommand.equals("get")) {

            String response = "-ERR unknown subcommand\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        String parameter = arguments.get(2).toLowerCase();

        String value;

        if (parameter.equals("dir")) {

            value = store.getDir();

        } else if (parameter.equals("dbfilename")) {

            value = store.getDbfilename();

        } else {

            /*
             * Unknown configuration parameter.
             */
            String response = "*0\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        /*
         * CONFIG GET returns:
         *
         * *2
         * $<length>
         * <parameter>
         * $<length>
         * <value>
         */

        byte[] parameterBytes = parameter.getBytes(StandardCharsets.UTF_8);

        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        String response = "*2\r\n"
                + "$" + parameterBytes.length + "\r\n"
                + parameter + "\r\n"
                + "$" + valueBytes.length + "\r\n"
                + value + "\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}