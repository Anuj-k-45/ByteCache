package protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespWriter {

    private final OutputStream outputStream;

    public RespWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void send(String response) throws IOException {

        outputStream.write(
                response.getBytes(
                        StandardCharsets.UTF_8));

        outputStream.flush();
    }

    public void sendBulkString(String value)
            throws IOException {

        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        String header = "$"
                + valueBytes.length
                + "\r\n";

        outputStream.write(
                header.getBytes(
                        StandardCharsets.UTF_8));

        outputStream.write(valueBytes);

        outputStream.write(
                "\r\n".getBytes(
                        StandardCharsets.UTF_8));

        outputStream.flush();
    }

    public void sendError(String message)
            throws IOException {

        send("-ERR " + message + "\r\n");
    }
}