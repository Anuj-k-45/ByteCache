package command.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import command.Command;
import storage.RedisStore;
import storage.StreamEntry;
import storage.StreamId;

public class XRangeCommand implements Command {

    @Override
    public void execute(
            List<String> arguments,
            OutputStream outputStream,
            RedisStore store) throws IOException {

        if (arguments.size() < 4) {
            return;
        }

        String streamKey = arguments.get(1);
        String startIdString = arguments.get(2);
        String endIdString = arguments.get(3);

        StreamId startId;
        StreamId endId;

        try {
            startId = parseRangeStartId(startIdString);
            endId = parseRangeEndId(endIdString);

        } catch (IllegalArgumentException e) {

            sendError(
                    outputStream,
                    e.getMessage());

            return;
        }

        List<StreamEntry> entries = store.getStreamRange(
                streamKey,
                startId,
                endId);

        sendStreamEntries(
                outputStream,
                entries);
    }

    private StreamId parseRangeStartId(
            String id) {

        if (id.equals("-")) {
            return new StreamId(
                    0,
                    0);
        }

        return StreamId.parse(id);
    }

    private StreamId parseRangeEndId(
            String id) {

        if (id.equals("+")) {
            return new StreamId(
                    Long.MAX_VALUE,
                    Long.MAX_VALUE);
        }

        return StreamId.parse(id);
    }

    private void sendStreamEntries(
            OutputStream outputStream,
            List<StreamEntry> entries)
            throws IOException {

        StringBuilder response = new StringBuilder();

        response.append("*")
                .append(entries.size())
                .append("\r\n");

        for (StreamEntry entry : entries) {

            response.append("*2\r\n");

            String id = entry.getId().toString();

            response.append("$")
                    .append(
                            id.getBytes(
                                    StandardCharsets.UTF_8).length)
                    .append("\r\n");

            response.append(id)
                    .append("\r\n");

            response.append("*")
                    .append(entry.getFields().size() * 2)
                    .append("\r\n");

            for (var field : entry.getFields().entrySet()) {

                String fieldName = field.getKey();
                String fieldValue = field.getValue();

                response.append("$")
                        .append(
                                fieldName.getBytes(
                                        StandardCharsets.UTF_8).length)
                        .append("\r\n");

                response.append(fieldName)
                        .append("\r\n");

                response.append("$")
                        .append(
                                fieldValue.getBytes(
                                        StandardCharsets.UTF_8).length)
                        .append("\r\n");

                response.append(fieldValue)
                        .append("\r\n");
            }
        }

        outputStream.write(
                response.toString()
                        .getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }

    private void sendError(
            OutputStream outputStream,
            String message)
            throws IOException {

        String response = "-ERR " + message + "\r\n";

        outputStream.write(
                response.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }
}