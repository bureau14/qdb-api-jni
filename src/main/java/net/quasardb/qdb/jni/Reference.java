package net.quasardb.qdb.jni;

public final class Reference<T> {
    public T value;

    public void clear() {
        this.value = null;
    }

    public T get() {
        assert(this.isEmpty() == false);
        return this.value;
    }

    public static <T> Reference<T> of(T value) {
        Reference<T> tmp = new Reference<T>();
        tmp.value = value;
        return tmp;
    }

    /**
     * Clears reference and returns last known value.
     */
    public T pop() {
        T tmp = this.get();
        this.clear();
        assert(this.isEmpty() == true);
        return tmp;
    }

    public void set(T value) {
        this.value = value;
    }

    public boolean isEmpty() {
        return this.value == null;
    }
}
