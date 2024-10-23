package ac.grim.grimac.api.util;

public class InitLater<T> {
    private volatile T value;

    public T get() {
        return this.value;
    }

    public void set(T value) {
        this.value = value;
    }
}
