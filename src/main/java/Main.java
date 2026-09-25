import server.RedisServer;

public class Main {

  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    RedisServer server = new RedisServer(6379);
    server.start();
  }
}