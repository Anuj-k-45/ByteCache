package storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads the subset of the Redis RDB format required by ByteCache.
 *
 * Supported:
 *
 * - REDIS0011 header
 * - FA metadata sections
 * - FE database selector
 * - FB database hash table information
 * - String values (type 0)
 * - FD expiry in seconds
 * - FC expiry in milliseconds
 * - RDB size encoding
 * - Integer encoded strings C0/C1/C2
 * - FF EOF
 *
 * LZF compressed strings (C3) are not supported yet.
 */
public class RdbReader {

    private final RedisStore store;
    private final Path file;

    public RdbReader(
            RedisStore store,
            String dir,
            String dbfilename) {

        this.store = store;
        this.file = Path.of(dir, dbfilename);
    }

    /**
     * Loads the RDB file if it exists.
     *
     * If the file does not exist, the database simply
     * starts empty.
     */
    public void load() throws IOException {

        if (!Files.exists(file)) {

            System.out.println(
                    "RDB file not found: "
                            + file);

            System.out.println(
                    "Starting with an empty database.");

            return;
        }

        System.out.println(
                "Loading RDB file: "
                        + file);

        byte[] data = Files.readAllBytes(file);

        RdbInput input = new RdbInput(data);

        readHeader(input);

        readSections(input);

        System.out.println(
                "RDB loading completed.");
    }

    /**
     * Reads:
     *
     * REDIS0011
     */
    private void readHeader(
            RdbInput input)
            throws IOException {

        byte[] header = input.readBytes(9);

        String headerString = new String(
                header,
                StandardCharsets.US_ASCII);

        if (!headerString.equals("REDIS0011")) {

            throw new IOException(
                    "Invalid RDB header: "
                            + headerString);
        }
    }

    /**
     * Reads the RDB sections until EOF.
     */
    private void readSections(
            RdbInput input)
            throws IOException {

        while (true) {

            int opcode = input.readUnsignedByte();

            /*
             * Metadata section.
             */
            if (opcode == 0xFA) {

                readMetadata(input);

            }

            /*
             * Database section.
             */
            else if (opcode == 0xFE) {

                readDatabase(input);

            }

            /*
             * EOF.
             */
            else if (opcode == 0xFF) {

                /*
                 * CRC64 occupies the final 8 bytes.
                 *
                 * We do not validate it yet.
                 */
                if (input.remaining() >= 8) {
                    input.readBytes(8);
                }

                return;
            }

            else {

                throw new IOException(
                        String.format(
                                "Unknown RDB opcode: 0x%02X",
                                opcode));
            }
        }
    }

    /**
     * Reads:
     *
     * FA
     * metadata-name
     * metadata-value
     */
    private void readMetadata(
            RdbInput input)
            throws IOException {

        String name = readString(input);

        String value = readString(input);

        System.out.println(
                "RDB metadata: "
                        + name
                        + " = "
                        + value);
    }

    /**
     * Reads a database section:
     *
     * FE
     * database number
     * FB
     * hash table size
     * expiry hash table size
     * key/value pairs
     */
    private void readDatabase(
            RdbInput input)
            throws IOException {

        /*
         * Database number.
         */
        long databaseNumber = readLength(input);

        System.out.println(
                "Reading RDB database: "
                        + databaseNumber);

        /*
         * FB
         *
         * Resize DB.
         */
        int resizeOpcode = input.readUnsignedByte();

        if (resizeOpcode != 0xFB) {

            throw new IOException(
                    String.format(
                            "Expected FB opcode but found 0x%02X",
                            resizeOpcode));
        }

        /*
         * Hash table size.
         */
        long hashTableSize = readLength(input);

        /*
         * Expiry hash table size.
         */
        long expiryHashTableSize = readLength(input);

        System.out.println(
                "RDB hash table size: "
                        + hashTableSize);

        System.out.println(
                "RDB expiry hash table size: "
                        + expiryHashTableSize);

        /*
         * Now read key/value entries.
         *
         * We stop when we encounter:
         *
         * FE → another database
         * FF → EOF
         */
        while (true) {

            int opcode = input.peekUnsignedByte();

            if (opcode == 0xFE
                    || opcode == 0xFF) {

                return;
            }

            readKeyValuePair(input);
        }
    }

    /**
     * Reads one RDB key/value pair.
     *
     * Optional:
     *
     * FD + 4 byte seconds
     *
     * or
     *
     * FC + 8 byte milliseconds
     *
     * Then:
     *
     * value type
     * key
     * value
     */
    private void readKeyValuePair(
            RdbInput input)
            throws IOException {

        System.out.println(
                "DEBUG: ENTER readKeyValuePair"
                        + " position=" + input.getPosition()
                        + " opcode=0x"
                        + String.format("%02X", input.peekUnsignedByte()));

        Long expiryTimeMilliseconds = null;

        int opcode = input.peekUnsignedByte();

        /*
         * Expiry in seconds.
         */
        if (opcode == 0xFD) {

            System.out.println("DEBUG: ENTERED FD expiry branch");

            input.readUnsignedByte();

            long seconds = readUnsignedLittleEndianInt(input);

            System.out.println(
                    "DEBUG: FD seconds = " + seconds);

            expiryTimeMilliseconds = seconds * 1000L;
        } else if (opcode == 0xFC) {

            System.out.println("DEBUG: ENTERED FC expiry branch");

            input.readUnsignedByte();

            expiryTimeMilliseconds = readUnsignedLittleEndianLong(input);

            System.out.println(
                    "DEBUG: FC timestamp = "
                            + expiryTimeMilliseconds);
        } else {

            System.out.println(
                    "DEBUG: NO expiry. opcode = 0x"
                            + String.format("%02X", opcode));
        }

        /*
         * Value type.
         */
        int valueType = input.readUnsignedByte();

        System.out.println(
                "DEBUG: valueType = 0x"
                        + String.format("%02X", valueType));

        if (valueType != 0) {
            throw new IOException(
                    "Unsupported RDB value type: " + valueType);
        }

        System.out.println(
                "DEBUG: next byte before key = 0x"
                        + String.format("%02X", input.peekUnsignedByte()));

        System.out.println("DEBUG: reading key");

        String key = readString(input);

        System.out.println("DEBUG: key = " + key);

        System.out.println("DEBUG: reading value");

        String value = readString(input);

        System.out.println("DEBUG: value = " + value);

        /*
         * No expiry.
         */
        if (expiryTimeMilliseconds == null) {

            store.set(
                    key,
                    value);

            return;
        }

        /*
         * RDB stores an absolute timestamp.
         *
         * RedisStore expects a duration.
         */

        long currentTime = System.currentTimeMillis();

        long remainingMilliseconds = expiryTimeMilliseconds - currentTime;

        System.out.println(
                "DEBUG EXPIRY: key=" + key
                        + " expiry=" + expiryTimeMilliseconds
                        + " current=" + currentTime
                        + " remaining=" + remainingMilliseconds);

        /*
         * Already expired.
         */
        if (remainingMilliseconds <= 0) {

            System.out.println(
                    "DEBUG EXPIRY: " + key + " is EXPIRED");

            return;
        }

        System.out.println(
                "DEBUG EXPIRY: " + key + " is ALIVE");
        store.set(
                key,
                value,
                remainingMilliseconds);
    }

    /**
     * Reads an RDB string.
     *
     * A string can be represented as:
     *
     * 1. Normal length + bytes
     * 2. C0 = 8-bit integer
     * 3. C1 = 16-bit integer
     * 4. C2 = 32-bit integer
     * 5. C3 = LZF compressed string
     */
    private String readString(
            RdbInput input)
            throws IOException {

        int firstByte = input.readUnsignedByte();

        int encodingType = (firstByte & 0xC0) >> 6;

        /*
         * Normal string length.
         */
        if (encodingType != 3) {

            long length = readLengthFromFirstByte(
                    input,
                    firstByte);

            if (length > Integer.MAX_VALUE) {

                throw new IOException(
                        "String too large");
            }

            byte[] bytes = input.readBytes(
                    (int) length);

            return new String(
                    bytes,
                    StandardCharsets.UTF_8);
        }

        /*
         * Special encoded string.
         *
         * Low 6 bits tell us which encoding.
         */
        int specialEncoding = firstByte & 0x3F;

        switch (specialEncoding) {

            /*
             * 8-bit integer.
             */
            case 0:

                int value8 = input.readUnsignedByte();

                return Integer.toString(
                        value8);

            /*
             * 16-bit little-endian integer.
             */
            case 1:

                int value16 = readUnsignedLittleEndianShort(
                        input);

                return Integer.toString(
                        value16);

            /*
             * 32-bit little-endian integer.
             */
            case 2:

                long value32 = readUnsignedLittleEndianInt(
                        input);

                return Long.toString(
                        value32);

            /*
             * LZF compressed string.
             */
            case 3:

                throw new IOException(
                        "LZF compressed strings are not supported yet");

            default:

                throw new IOException(
                        "Unknown RDB string encoding: "
                                + specialEncoding);
        }
    }

    /**
     * Reads an RDB size encoding.
     *
     * First two bits determine the encoding:
     *
     * 00 -> remaining 6 bits
     * 01 -> next 14 bits
     * 10 -> next 4 bytes
     * 11 -> special string encoding
     */
    private long readLength(
            RdbInput input)
            throws IOException {

        int firstByte = input.readUnsignedByte();

        int encodingType = (firstByte & 0xC0) >> 6;

        /*
         * 00
         *
         * Length is contained entirely
         * in the lower 6 bits.
         */
        if (encodingType == 0) {

            return firstByte & 0x3F;
        }

        /*
         * 01
         *
         * 14-bit big-endian length.
         */
        if (encodingType == 1) {

            int secondByte = input.readUnsignedByte();

            return ((long) (firstByte & 0x3F) << 8)
                    | secondByte;
        }

        /*
         * 10
         *
         * 4-byte big-endian length.
         */
        if (encodingType == 2) {

            return readUnsignedBigEndianInt(input);
        }

        /*
         * 11 means special string encoding.
         *
         * This method expects a normal length,
         * so special encoding is invalid here.
         */
        throw new IOException(
                "Expected RDB length but found special string encoding");
    }

    /**
     * Used by readString() because the first byte
     * has already been consumed.
     */
    private long readLengthFromFirstByte(
            RdbInput input,
            int firstByte)
            throws IOException {

        int encodingType = (firstByte & 0xC0) >> 6;

        if (encodingType == 0) {

            return firstByte & 0x3F;
        }

        if (encodingType == 1) {

            int secondByte = input.readUnsignedByte();

            return ((long) (firstByte & 0x3F) << 8)
                    | secondByte;
        }

        if (encodingType == 2) {

            return readUnsignedBigEndianInt(input);
        }

        throw new IOException(
                "Invalid normal string length encoding");
    }

    /**
     * Reads a 4-byte unsigned integer
     * in big-endian order.
     */
    private long readUnsignedBigEndianInt(
            RdbInput input)
            throws IOException {

        long b1 = input.readUnsignedByte();

        long b2 = input.readUnsignedByte();

        long b3 = input.readUnsignedByte();

        long b4 = input.readUnsignedByte();

        return (b1 << 24)
                | (b2 << 16)
                | (b3 << 8)
                | b4;
    }

    /**
     * Reads a 4-byte unsigned integer
     * in little-endian order.
     */
    private long readUnsignedLittleEndianInt(
            RdbInput input)
            throws IOException {

        long b1 = input.readUnsignedByte();

        long b2 = input.readUnsignedByte();

        long b3 = input.readUnsignedByte();

        long b4 = input.readUnsignedByte();

        return b1
                | (b2 << 8)
                | (b3 << 16)
                | (b4 << 24);
    }

    /**
     * Reads an 8-byte unsigned integer
     * in little-endian order.
     *
     * Java long is signed, but timestamps used
     * by Redis are comfortably inside the positive
     * range for this project.
     */
    private long readUnsignedLittleEndianLong(
            RdbInput input)
            throws IOException {

        long result = 0;

        System.out.println("DEBUG: START reading 8-byte timestamp");

        for (int i = 0; i < 8; i++) {

            int b = input.readUnsignedByte();

            System.out.println(
                    "DEBUG: timestamp byte "
                            + i
                            + " = 0x"
                            + String.format("%02X", b));

            result |= ((long) b) << (8 * i);
        }

        System.out.println(
                "DEBUG: END timestamp = " + result);

        return result;
    }

    /**
     * Reads a 2-byte unsigned integer
     * in little-endian order.
     */
    private int readUnsignedLittleEndianShort(
            RdbInput input)
            throws IOException {

        int low = input.readUnsignedByte();

        int high = input.readUnsignedByte();

        return low | (high << 8);
    }

    /**
     * Small binary input helper.
     *
     * RDB is a binary format, so we work with
     * individual bytes rather than lines.
     */
    private static class RdbInput {

        private final byte[] data;
        private int position;

        RdbInput(byte[] data) {

            this.data = data;
            this.position = 0;
        }

        int readUnsignedByte()
                throws IOException {

            if (position >= data.length) {

                throw new IOException(
                        "Unexpected end of RDB file");
            }

            return data[position++] & 0xFF;
        }

        int peekUnsignedByte()
                throws IOException {

            if (position >= data.length) {

                throw new IOException(
                        "Unexpected end of RDB file");
            }

            return data[position] & 0xFF;
        }

        byte[] readBytes(
                int length)
                throws IOException {

            if (length < 0
                    || position + length > data.length) {

                throw new IOException(
                        "Unexpected end of RDB file");
            }

            byte[] result = new byte[length];

            System.arraycopy(
                    data,
                    position,
                    result,
                    0,
                    length);

            position += length;

            return result;
        }

        int remaining() {

            return data.length - position;
        }

        int getPosition() {
            return position;
        }
    }
}