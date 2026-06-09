package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum FilterOp {
    EQ, NEQ, GT, GTE, LT, LTE, IN, RANGE, EXISTS
}
