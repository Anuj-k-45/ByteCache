import server.RedisServer;

public class Main {

  public static void main(String[] args) {

    int port = 6379;

    for (int i = 0; i < args.length; i++) {

      if (args[i].equals("--port")) {

        if (i + 1 >= args.length) {
          System.out.println("Missing port number");
          return;
        }

        port = Integer.parseInt(args[i + 1]);
        break;
      }
    }

    System.out.println("Logs from your program will appear here!");

    RedisServer server = new RedisServer(port);
    server.start();
  }
}