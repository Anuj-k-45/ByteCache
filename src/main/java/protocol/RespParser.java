package protocol;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    private final ByteArrayOutputStream data = new ByteArrayOutputStream();

    public void feed(byte[] buffer, int length) {

        data.write(buffer, 0, length);
    }

    public List<List<String>> getCompleteCommands() {

        List<List<String>> commands = new ArrayList<>();

        byte[] bytes = data.toByteArray();

        int position = 0;

        while (position < bytes.length) {

            ParseResult result = parseCommand(bytes, position);

            if (result == null) {
                break;
            }

            commands.add(result.command);

            position = result.nextPosition;
        }

        if (position > 0) {

            byte[] remaining = new byte[bytes.length - position];

            System.arraycopy(
                    bytes,
                    position,
                    remaining,
                    0,
                    remaining.length);

            data.reset();

            data.write(
                    remaining,
                    0,
                    remaining.length);
        }

        return commands;
    }

    private ParseResult parseCommand(
            byte[] bytes,
            int position) {

        /*
         * A Redis command sent by redis-cli is normally:
         *
         * *2\r\n
         * $4\r\n
         * ECHO\r\n
         * $3\r\n
         * hey\r\n
         */

        if (position >= bytes.length) {
            return null;
        }

        // We currently expect a RESP Array.
        if (bytes[position] != '*') {
            throw new IllegalArgumentException(
                    "Expected RESP array");
        }

        position++;

        LineResult countResult = readLine(bytes, position);

        if (countResult == null) {
            return null;
        }

        int elementCount = Integer.parseInt(countResult.line);

        position = countResult.nextPosition;

        List<String> command = new ArrayList<>();

        for (int i = 0; i < elementCount; i++) {

            if (position >= bytes.length) {
                return null;
            }

            // We currently expect bulk strings.
            if (bytes[position] != '$') {
                throw new IllegalArgumentException(
                        "Expected RESP bulk string");
            }

            position++;

            LineResult lengthResult = readLine(bytes, position);

            if (lengthResult == null) {
                return null;
            }

            int length = Integer.parseInt(lengthResult.line);

            position = lengthResult.nextPosition;

            // Do we have the complete string?
            if (bytes.length < position + length + 2) {
                return null;
            }

            String value = new String(
                    bytes,
                    position,
                    length,
                    StandardCharsets.UTF_8);

            command.add(value);

            position += length;

            // Every bulk string ends with \r\n.
            if (bytes[position] != '\r'
                    || bytes[position + 1] != '\n') {
                throw new IllegalArgumentException(
                        "Invalid RESP bulk string");
            }

            position += 2;
        }

        return new ParseResult(
                command,
                position);
    }

    private LineResult readLine(
            byte[] bytes,
            int position) {

        for (int i = position; i < bytes.length - 1; i++) {

            if (bytes[i] == '\r'
                    && bytes[i + 1] == '\n') {

                String line = new String(
                        bytes,
                        position,
                        i - position,
                        StandardCharsets.UTF_8);

                return new LineResult(
                        line,
                        i + 2);
            }
        }

        // We don't have a complete line yet.
        return null;
    }

    private static class LineResult {

        private final String line;
        private final int nextPosition;

        private LineResult(
                String line,
                int nextPosition) {
            this.line = line;
            this.nextPosition = nextPosition;
        }
    }

    private static class ParseResult {

        private final List<String> command;
        private final int nextPosition;

        private ParseResult(
                List<String> command,
                int nextPosition) {
            this.command = command;
            this.nextPosition = nextPosition;
        }
    }
}