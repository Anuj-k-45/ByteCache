package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import command.Command;
import protocol.RespWriter;
import storage.RedisStore;

public class PingCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store) throws IOException {

        RespWriter writer = new RespWriter(outputStream);

        writer.send("+PONG\r\n");
    }
}