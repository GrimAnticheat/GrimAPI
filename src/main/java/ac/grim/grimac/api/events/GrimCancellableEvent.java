package ac.grim.grimac.api.events;

public interface GrimCancellableEvent extends GrimEvent {
    boolean isCancelled();
    void setCancelled(boolean cancelled);
}
