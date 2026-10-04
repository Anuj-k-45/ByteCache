package command.generic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import command.Command;
import connection.ClientContext;
import storage.RedisStore;

public class PsyncCommand implements Command {

        private static final String EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

        @Override
        public void execute(
                        List<String> arguments,
                        OutputStream outputStream,
                        RedisStore store,
                        ClientContext context)
                        throws IOException {

                // --------------------------------------------------
                // STEP 1: Send FULLRESYNC response
                // --------------------------------------------------

                String fullResync = "+FULLRESYNC "
                                + store.getReplicationId()
                                + " "
                                + store.getReplicationOffset()
                                + "\r\n";

                outputStream.write(
                                fullResync.getBytes(StandardCharsets.UTF_8));

                outputStream.flush();

                // --------------------------------------------------
                // STEP 2: Decode the empty RDB file
                // --------------------------------------------------

                byte[] rdbFile = Base64.getDecoder().decode(
                                EMPTY_RDB_BASE64);

                // --------------------------------------------------
                // STEP 3: Build RDB header
                // --------------------------------------------------

                String rdbHeader = "$"
                                + rdbFile.length
                                + "\r\n";

                byte[] rdbHeaderBytes = rdbHeader.getBytes(
                                StandardCharsets.UTF_8);

                System.out.println(
                                "Sending RDB: "
                                                + rdbFile.length
                                                + " bytes");

                // --------------------------------------------------
                // STEP 4: Send RDB header + binary data
                // --------------------------------------------------
                System.out.println(
                                "Sending RDB header: "
                                                + rdbHeader);

                outputStream.write(rdbHeaderBytes);
                outputStream.write(rdbFile);

                outputStream.flush();

                System.out.println("RDB sent to replica");

                store.addReplica(outputStream);
        }

}