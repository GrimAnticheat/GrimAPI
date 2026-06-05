package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

/**
 * Role of a persistent record component, derived from the annotation applied
 * to it. Drives codec and adapter behavior: {@link #ID} / {@link #PARTITION}
 * become indexable keys; {@link #SEARCHABLE} drives search index creation;
 * {@link #VALUE} stays in the payload.
 */
@ApiStatus.Experimental
public enum FieldKind {
    ID,
    PARTITION,
    TIMESTAMP,
    INDEXED,
    SEARCHABLE,
    VALUE
}
