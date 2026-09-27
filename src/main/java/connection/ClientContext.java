package connection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClientContext {

    private boolean inTransaction;
    private final List<List<String>> queuedCommands;
    private final Set<String> watchedKeys;

    public ClientContext() {
        this.queuedCommands = new ArrayList<>();
        this.watchedKeys = new HashSet<>();
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

    public void watchKey(String key) {
        watchedKeys.add(key);
    }

    public Set<String> getWatchedKeys() {
        return watchedKeys;
    }
}