package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import connection.ClientConnection;
import storage.RedisStore;

public class RedisServer {

    private final int port;

    private final RedisStore store;

    private ServerSocket serverSocket;

    public RedisServer(int port) {

        this.port = port;

        this.store = new RedisStore();
    }

    public void start() {

        try {
            serverSocket = new ServerSocket(port);

            System.out.println(
                    "Server starting...");

            System.out.println(
                    "Server is listening on port "
                            + port);

            while (true) {

                Socket clientSocket = serverSocket.accept();

                System.out.println(
                        "Client connected!");

                ClientConnection clientConnection = new ClientConnection(
                        clientSocket,
                        store);

                Thread clientThread = new Thread(
                        clientConnection);

                clientThread.start();
            }

        } catch (IOException e) {

            System.out.println(
                    "Server error: "
                            + e.getMessage());
        }
    }
}