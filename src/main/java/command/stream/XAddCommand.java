package command.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;
import storage.StreamEntry;
import storage.StreamId;

public class XAddCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store,
            ClientContext context) throws IOException {

        if (arguments.size() < 5) {
            return;
        }

        String streamKey = arguments.get(1);

        String entryIdString = arguments.get(2);

        StreamId entryId;

        // Auto-generate complete ID
        if (entryIdString.equals("*")) {

            try {

                entryId = store.generateNextStreamId(
                        streamKey);

            } catch (IllegalArgumentException e) {

                sendError(
                        outputStream,
                        e.getMessage());

                return;
            }

            // Auto-generate sequence number
        } else if (entryIdString.endsWith("-*")) {

            String millisecondsPart = entryIdString.substring(
                    0,
                    entryIdString.length() - 2);

            long millisecondsTime;

            try {

                millisecondsTime = Long.parseLong(millisecondsPart);

            } catch (NumberFormatException e) {

                sendError(
                        outputStream,
                        "Invalid stream ID");

                return;
            }

            try {

                entryId = store.generateStreamId(
                        streamKey,
                        millisecondsTime);

            } catch (IllegalArgumentException e) {

                sendError(
                        outputStream,
                        e.getMessage());

                return;
            }

            // Explicit ID
        } else {

            try {

                entryId = StreamId.parse(
                        entryIdString);

            } catch (IllegalArgumentException e) {

                sendError(
                        outputStream,
                        "Invalid stream ID");

                return;
            }
        }

        // After the ID, fields and values
        // must come in pairs.
        if ((arguments.size() - 3) % 2 != 0) {
            return;
        }

        Map<String, String> fields = new LinkedHashMap<>();

        for (int i = 3; i < arguments.size(); i += 2) {

            String field = arguments.get(i);

            String value = arguments.get(i + 1);

            fields.put(
                    field,
                    value);
        }

        StreamEntry entry = new StreamEntry(
                entryId,
                fields);

        try {

            store.addStreamEntry(
                    streamKey,
                    entry);

        } catch (IllegalArgumentException e) {

            sendError(
                    outputStream,
                    e.getMessage());

            return;
        }

        // XADD returns the generated/explicit
        // entry ID as a RESP bulk string.
        sendBulkString(
                outputStream,
                entryId.toString());
    }

    private void sendError(
            OutputStream outputStream,
            String message) throws IOException {

        String response = "-ERR " + message + "\r\n";

        outputStream.write(
                response.getBytes());

        outputStream.flush();
    }

    private void sendBulkString(
            OutputStream outputStream,
            String value) throws IOException {

        byte[] bytes = value.getBytes();

        String header = "$" + bytes.length + "\r\n";

        outputStream.write(
                header.getBytes());

        outputStream.write(bytes);

        outputStream.write(
                "\r\n".getBytes());

        outputStream.flush();
    }
}