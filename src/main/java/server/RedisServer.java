package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import connection.ClientConnection;
import storage.RdbReader;
import storage.RedisStore;

public class RedisServer {

    private final int port;
    private final boolean replica;

    private final String masterHost;
    private final int masterPort;

    private final RedisStore store;

    private ServerSocket serverSocket;

    public RedisServer(
            int port,
            boolean replica,
            String masterHost,
            int masterPort,
            String dir,
            String dbfilename) {

        this.port = port;
        this.replica = replica;
        this.masterHost = masterHost;
        this.masterPort = masterPort;

        this.store = new RedisStore(
                replica,
                dir,
                dbfilename);
    }

    public boolean isReplica() {
        return replica;
    }

    public void start() {

        try {

            /*
             * Load the RDB snapshot before accepting
             * any clients.
             */
            RdbReader rdbReader = new RdbReader(
                    store,
                    store.getDir(),
                    store.getDbfilename());

            rdbReader.load();

            serverSocket = new ServerSocket(port);

            System.out.println(
                    "Server starting...");

            System.out.println(
                    "Server is listening on port "
                            + port);

            if (replica) {

                ReplicationConnection replicationConnection = new ReplicationConnection(
                        masterHost,
                        masterPort,
                        port,
                        store);

                Thread replicationThread = new Thread(
                        () -> {

                            try {

                                replicationConnection
                                        .connectAndHandshake();

                            } catch (IOException e) {

                                System.out.println(
                                        "Replication error: "
                                                + e.getMessage());
                            }
                        });

                replicationThread.start();
            }

            while (true) {

                Socket clientSocket = serverSocket.accept();

                System.out.println(
                        "Client connected!");

                ClientConnection clientConnection = new ClientConnection(
                        clientSocket,
                        store);

                Thread clientThread = new Thread(clientConnection);

                clientThread.start();
            }

        } catch (IOException e) {

            System.out.println(
                    "Server error: "
                            + e.getMessage());
        }
    }
}