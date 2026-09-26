package storage;

import java.util.ArrayList;
import java.util.List;

public class RedisList {

    private final List<String> elements = new ArrayList<>();

    public void add(String value) {
        elements.add(value);
    }

    public void addFirst(String value) {
        elements.add(0, value);
    }

    public int size() {
        return elements.size();
    }

    public List<String> getRange(int start, int stop) {

        int size = elements.size();

        // Convert negative indexes to positive indexes
        if (start < 0) {
            start = size + start;
        }

        if (stop < 0) {
            stop = size + stop;
        }

        // Negative index beyond the beginning
        if (start < 0) {
            start = 0;
        }

        if (stop < 0) {
            stop = 0;
        }

        // Stop cannot go beyond the end
        if (stop >= size) {
            stop = size - 1;
        }

        // Start beyond the list or start > stop
        if (start >= size || start > stop) {
            return new ArrayList<>();
        }

        return new ArrayList<>(
                elements.subList(start, stop + 1));
    }

    public String removeFirst() {
        if (elements.isEmpty()) {
            return null;
        }

        return elements.remove(0);
    }

    public List<String> removeFirst(int count) {
        List<String> removed = new ArrayList<>();

        int numberToRemove = Math.min(count, elements.size());

        for (int i = 0; i < numberToRemove; i++) {
            removed.add(elements.remove(0));
        }

        return removed;
    }

}