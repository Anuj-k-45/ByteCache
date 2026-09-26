package connection;

import java.util.ArrayList;
import java.util.List;

public class ClientContext {

    private boolean inTransaction;
    private final List<List<String>> queuedCommands;

    public ClientContext() {
        this.queuedCommands = new ArrayList<>();
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void startTransaction() {
        inTransaction = true;
        queuedCommands.clear();
    }

    public void endTransaction() {
        inTransaction = false;
        queuedCommands.clear();
    }

    public void queueCommand(List<String> command) {
        queuedCommands.add(new ArrayList<>(command));
    }

    public List<List<String>> getQueuedCommands() {
        return queuedCommands;
    }
}