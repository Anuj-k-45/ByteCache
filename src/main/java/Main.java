import server.RedisServer;

public class Main {

    public static void main(String[] args) {

        int port = 6379;
        boolean replica = false;

        String masterHost = null;
        int masterPort = -1;

        // RDB configuration
        String dir = ".";
        String dbfilename = "dump.rdb";

        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("--port")) {

                if (i + 1 >= args.length) {
                    System.out.println("Missing port number");
                    return;
                }

                port = Integer.parseInt(args[i + 1]);
                i++;

            } else if (args[i].equals("--replicaof")) {

                if (i + 1 >= args.length) {
                    System.out.println("Missing replicaof value");
                    return;
                }

                String[] parts = args[i + 1].split(" ");

                if (parts.length != 2) {
                    System.out.println("Invalid replicaof value");
                    return;
                }

                masterHost = parts[0];
                masterPort = Integer.parseInt(parts[1]);

                replica = true;

                i++;

            } else if (args[i].equals("--dir")) {

                if (i + 1 >= args.length) {
                    System.out.println("Missing directory path");
                    return;
                }

                dir = args[i + 1];
                i++;

            } else if (args[i].equals("--dbfilename")) {

                if (i + 1 >= args.length) {
                    System.out.println("Missing dbfilename");
                    return;
                }

                dbfilename = args[i + 1];
                i++;
            }
        }

        System.out.println(
                "DEBUG: port=" + port
                        + ", replica=" + replica
                        + ", masterHost=" + masterHost
                        + ", masterPort=" + masterPort
                        + ", dir=" + dir
                        + ", dbfilename=" + dbfilename);

        System.out.println(
                "Logs from your program will appear here!");

        RedisServer server = new RedisServer(
                port,
                replica,
                masterHost,
                masterPort,
                dir,
                dbfilename);

        server.start();
    }
}