package storage;

public class StoredValue {

    private final String value;
    private final Long expiresAt;

    public StoredValue(String value, Long expiresAt) {
        this.value = value;
        this.expiresAt = expiresAt;
    }

    public String getValue() {
        return value;
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public boolean isExpired() {
        if (expiresAt == null) {
            return false;
        }

        return System.currentTimeMillis() >= expiresAt;
    }
}