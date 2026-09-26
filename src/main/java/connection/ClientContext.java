package connection;

public class ClientContext {

    private boolean inTransaction;

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void startTransaction() {
        inTransaction = true;
    }

    public void endTransaction() {
        inTransaction = false;
    }
}