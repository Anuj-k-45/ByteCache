import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

  public static void main(String[] args) {

    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      System.out.println("Server starting...");

      serverSocket = new ServerSocket(port);
      System.out.println("Server is listening...");

      serverSocket.setReuseAddress(true);

      // Keep accepting new clients
      while (true) {

        Socket clientSocket = serverSocket.accept();
        System.out.println("Client connected!");

        // Create a new thread for this client
        Thread clientThread = new Thread(() -> {

          try {
            InputStream inputStream = clientSocket.getInputStream();

            byte[] buffer = new byte[1024];
            int bytesRead;

            // Keep handling commands from this client
            while ((bytesRead = inputStream.read(buffer)) != -1) {

              clientSocket.getOutputStream()
                  .write("+PONG\r\n".getBytes());
            }

            clientSocket.close();
            System.out.println("Client disconnected!");

          } catch (IOException e) {
            System.out.println(
                "Client error: " + e.getMessage());
          }
        });

        // Start handling this client independently
        clientThread.start();
      }

    } catch (IOException e) {
      System.out.println("Server error: " + e.getMessage());
    }
  }
}