package ac.grim.grimac.api.event;

public interface Cancellable {
    boolean isCancelled();
    void setCancelled(boolean cancelled);
}