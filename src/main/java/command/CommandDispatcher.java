package command;

import java.io.IOException;
import java.io.OutputStream;
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
    }

    public void dispatch(
            List<String> command,
            OutputStream outputStream,
            RedisStore store) throws IOException {

        if (command.isEmpty()) {
            return;
        }

        String commandName = command.get(0).toUpperCase();

        Command handler = commands.get(commandName);

        if (handler == null) {
            return;
        }

        handler.execute(
                command,
                outputStream,
                store);
    }
}