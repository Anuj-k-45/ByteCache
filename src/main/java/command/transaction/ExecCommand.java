package command.transaction;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import command.Command;
import command.CommandDispatcher;
import connection.ClientContext;
import storage.RedisStore;

public class ExecCommand implements Command {

    private final CommandDispatcher dispatcher;

    public ExecCommand(
            CommandDispatcher dispatcher) {

        this.dispatcher = dispatcher;
    }

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (!context.isInTransaction()) {

            String response = "-ERR EXEC without MULTI\r\n";

            outputStream.write(
                    response.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        /*
         * First check whether any watched key
         * was modified.
         */
        if (context.isWatchDirty()) {

            store.clearWatchState(context);

            context.endTransaction();

            outputStream.write(
                    "*-1\r\n".getBytes(StandardCharsets.UTF_8));

            outputStream.flush();

            return;
        }

        /*
         * No watched key was modified.
         * Execute all queued commands.
         */
        List<List<String>> queuedCommands = context.getQueuedCommands();

        List<byte[]> responses = new ArrayList<>();

        for (List<String> queuedCommand : queuedCommands) {

            ByteArrayOutputStream commandResponse = new ByteArrayOutputStream();

            dispatcher.executeQueuedCommand(
                    queuedCommand,
                    commandResponse,
                    store,
                    context);

            responses.add(
                    commandResponse.toByteArray());
        }

        /*
         * EXEC consumes the transaction and
         * clears WATCH state.
         */
        store.clearWatchState(context);

        context.endTransaction();

        String header = "*" + responses.size() + "\r\n";

        outputStream.write(
                header.getBytes(StandardCharsets.UTF_8));

        for (byte[] response : responses) {
            outputStream.write(response);
        }

        outputStream.flush();
    }
}