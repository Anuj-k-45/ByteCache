import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;

    try {
      System.out.println("Server starting...");

      serverSocket = new ServerSocket(port);
      System.out.println("Server is listening...");

      serverSocket.setReuseAddress(true);

      clientSocket = serverSocket.accept();
      System.out.println("Client connected!");

      InputStream inputStream = clientSocket.getInputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
      }

      System.out.println("PONG sent to client!");

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
          System.out.println("Client disconnected!");
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
