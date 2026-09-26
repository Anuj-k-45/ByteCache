package command;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import command.generic.EchoCommand;
import command.generic.PingCommand;
import command.generic.TypeCommand;
import command.numeric.IncrCommand;
import command.stream.XAddCommand;
import command.stream.XRangeCommand;
import command.stream.XReadCommand;
import command.string.GetCommand;
import command.string.SetCommand;
import command.transaction.ExecCommand;
import command.transaction.MultiCommand;
import connection.ClientContext;
import storage.RedisStore;

public class CommandDispatcher {

    private final Map<String, Command> commands = new HashMap<>();

    public CommandDispatcher() {

        commands.put("PING", new PingCommand());
        commands.put("ECHO", new EchoCommand());
        commands.put("SET", new SetCommand());
        commands.put("GET", new GetCommand());
        commands.put("TYPE", new TypeCommand());
        commands.put("XADD", new XAddCommand());
        commands.put("XRANGE", new XRangeCommand());
        commands.put("XREAD", new XReadCommand());
        commands.put("INCR", new IncrCommand());
        commands.put("MULTI", new MultiCommand());
        commands.put("EXEC", new ExecCommand());
    }

    public void dispatch(
            List<String> command,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (command.isEmpty()) {
            return;
        }

        String commandName = command.get(0).toUpperCase();

        Command handler = commands.get(commandName);

        if (handler == null) {
            String response = "-ERR unknown command '" + command.get(0) + "'\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();
            return;
        }

        handler.execute(
                command,
                outputStream,
                store,
                context);
    }
}