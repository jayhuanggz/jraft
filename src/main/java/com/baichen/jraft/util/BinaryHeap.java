package com.baichen.jraft.util;

public class BinaryHeap<T extends Comparable<T>> {

    private final int capacity;

    private Comparable<T>[] array;

    private int size;

    private final boolean inverse;

    public BinaryHeap(Comparable<T>[] array, boolean inverse, int capacity) {
        this.inverse = inverse;
        this.array = array;
        this.capacity = capacity;
        this.size = array.length;
        buildHeap();
    }


    public BinaryHeap(Comparable<T>[] array, int capacity) {
        this(array, false, capacity);
    }


    public BinaryHeap(int initialSize, int capacity) {
        this(initialSize, false, capacity);

    }

    public BinaryHeap(int initialSize, boolean inverse, int capacity) {
        this.inverse = inverse;
        this.capacity = capacity;
        this.array = new Comparable[initialSize];
        this.size = 0;
    }

    private void ensureSize(int requiredSize) {
        if (requiredSize > capacity) {
            throw new IllegalArgumentException("Heap is full, current size: " + size
                    + "requested size: " + requiredSize + ", capacity: " + capacity);
        }

        if (requiredSize > array.length) {
            Comparable<T>[] newArray = new Comparable[Math.min(capacity, Math.max(array.length << 1, requiredSize))];
            System.arraycopy(array, 0, newArray, 0, array.length);
            this.array = newArray;
        }

    }

    private void buildHeap() {
        for (int i = size >> 1; i >= 0; i--) {
            buildHeap(i);
        }
    }

    private void buildHeap(int index) {

        if (size <= 1 || index >= size << 1) {
            return;
        }

        Comparable<T> value = array[index];

        int leftIndex = (index << 1) + 1;
        int rightIndex = leftIndex + 1;

        int winnerIndex = index;

        if (leftIndex < size && compare((T) array[leftIndex], (T) value) < 0) {
            winnerIndex = leftIndex;
        }

        if (rightIndex < size && compare((T) array[rightIndex], (T) array[winnerIndex]) < 0) {
            winnerIndex = rightIndex;
        }

        if (winnerIndex != index) {
            array[index] = array[winnerIndex];
            array[winnerIndex] = value;
            buildHeap(winnerIndex);
        }
    }

    private int compare(Comparable<T> left, Comparable<T> right) {

        int val = left.compareTo((T) right);
        if (val == 0) {
            return 0;
        }
        if (inverse) {
            return -val;
        }


        return val;
    }

    public void add(Comparable<T> val) {
        ensureSize(size + 1);
        array[size++] = val;
        for (int parentIndex = (size - 1) >> 1; parentIndex >= 0; parentIndex = (parentIndex - 1) >> 1) {
            buildHeap(parentIndex);
        }
    }

    public void remove(T val) {

        int index = findIndex(val, 0);

        if (index != -1) {
            array[index] = array[size - 1];
            array[size - 1] = null;
            size--;
            buildHeap();
        }

    }

    private int findIndex(T val, int i) {
        if (i >= size) {
            return -1;
        }

        if (val == array[i]) {
            return i;
        }

        if (compare(val, array[i]) < 0) {
            return -1;
        }


        int leftIndex = (i << 1) + 1;
        int result = findIndex(val, leftIndex);

        if (result == -1) {
            int rightIndex = leftIndex + 1;
            result = findIndex(val, rightIndex);
        }

        return result;

    }

    public T pop() {

        if (size == 0) {
            return null;
        }

        Comparable<T> top = array[0];

        Comparable<T> last = array[size - 1];
        array[0] = null;
        if (size > 1) {
            array[0] = last;
        }
        array[size - 1] = null;
        size--;
        buildHeap();
        return (T) top;

    }

    public T peek() {

        if (size == 0) {
            return null;
        }

        return (T) array[0];
    }

    public void clear() {

        for (int i = 0; i < size; i++) {
            array[i] = null;
        }
        size = 0;
    }

    public void rebuildHeap() {
        buildHeap();
    }

}
