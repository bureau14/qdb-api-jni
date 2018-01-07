package net.quasardb.qdb.jni;

public class Reference<T> {
    public T value;

    public void clear() {
        this.value = null;
    }

    public T get() {
        return this.value;
    }

    public boolean isEmpty() {
        return this.value == null;
    }
}
