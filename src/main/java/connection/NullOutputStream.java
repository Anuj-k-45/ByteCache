package connection;

import java.io.IOException;
import java.io.OutputStream;

public class NullOutputStream extends OutputStream {

    @Override
    public void write(int b) throws IOException {
        // Ignore output.
    }

    @Override
    public void write(
            byte[] b,
            int off,
            int len)
            throws IOException {
        // Ignore output.
    }
}