package ac.grim.grimac.api.handler;

public interface UserHandlerHolder {

    /**
     * Retrieves the resync handler.
     * @return ResyncHandler
     */
    ResyncHandler getResyncHandler();

    /**
     * Sets the resync handler.
     * @param resyncHandler ResyncHandler
     */
    void setResyncHandler(ResyncHandler resyncHandler);

}
