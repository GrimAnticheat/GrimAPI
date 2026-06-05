package ac.grim.grimac.internal.storage.codec;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * Build a typed, primitive-specialised accessor for one record component
 * using {@link LambdaMetafactory}. The returned {@code Object} is one of:
 * {@link ToIntFunction}, {@link ToLongFunction}, {@link ToDoubleFunction},
 * or {@link Function} parameterised by the component's reference type.
 * <p>
 * Materialised once per (record class, component) at codec init; the
 * hot-path encoder casts based on the parallel {@link TypeTag} array and
 * calls the typed function directly. JIT inlines the call — no reflection
 * framing, no boxing for primitives.
 * <p>
 * Public because per-format codec subpackages (BSON, JDBC, Redis) construct
 * their codec instances directly when an extension installs a custom
 * {@code Codecs.Provider}.
 */
@ApiStatus.Internal
public final class Accessors {

    private static final MethodHandles.Lookup ROOT_LOOKUP = MethodHandles.lookup();

    private Accessors() {}

    /**
     * @return one of ToIntFunction / ToLongFunction / ToDoubleFunction / Function,
     *         specialised on the component's static type.
     */
    public static @NotNull Object build(@NotNull Class<?> ownerType, @NotNull String accessorName, @NotNull Class<?> fieldType) {
        TypeTag tag = TypeTag.of(fieldType);
        try {
            // findVirtual on the kept public accessor name — does NOT touch the
            // RecordComponents attribute (stripped by Allatori). Record accessors
            // are always public, so grim-internal's own full-privilege ROOT_LOOKUP
            // resolves them. Using ROOT_LOOKUP (not privateLookupIn on the api
            // record class, which the loader publishes into a separate stub
            // classloader) keeps the lookup full-privilege, so LambdaMetafactory
            // accepts the caller on a Mojang-mapped Paper server — a cross-
            // classloader privateLookupIn is NOT full-privilege and trips
            // "LambdaConversionException: Invalid caller". Yields a
            // (ownerType)->fieldType handle.
            MethodHandle accessor = ROOT_LOOKUP
                .findVirtual(ownerType, accessorName, MethodType.methodType(fieldType));
            return switch (tag) {
                case INT     -> buildPrimitive(ownerType, accessor,
                                       ToIntFunction.class, "applyAsInt", int.class);
                case LONG    -> buildPrimitive(ownerType, accessor,
                                       ToLongFunction.class, "applyAsLong", long.class);
                case DOUBLE  -> buildPrimitive(ownerType, accessor,
                                       ToDoubleFunction.class, "applyAsDouble", double.class);
                // float -> double: pass the direct float accessor to
                // ToDoubleFunction. LambdaMetafactory performs the widening as
                // part of bridge generation — composing filterReturnValue
                // beforehand makes the impl handle indirect, which standard
                // metafactory rejects with LambdaConversionException.
                case FLOAT   -> buildFloatAsDouble(ownerType, accessor);
                case BOOLEAN     -> buildReference(ownerType, accessor, Boolean.class);
                case STRING      -> buildReference(ownerType, accessor, String.class);
                case BYTES       -> buildReference(ownerType, accessor, byte[].class);
                case UUID        -> buildReference(ownerType, accessor, java.util.UUID.class);
                case FLOAT_ARRAY -> buildReference(ownerType, accessor, float[].class);
                // ENUM: hand back a Function<R, Enum<?>>. The codec extracts ordinal() at
                // encode time — no MethodHandle composition with covariant return types.
                case ENUM        -> buildReference(ownerType, accessor, fieldType);
                case NESTED_SEALED -> buildReference(ownerType, accessor, fieldType);
            };
        } catch (Throwable t) {
            throw new IllegalStateException(
                "could not build accessor for " + ownerType.getName() + "." + accessorName, t);
        }
    }

    /** Build a primitive-specialised ToXxxFunction directly from the accessor. */
    private static @NotNull Object buildPrimitive(
            @NotNull Class<?> ownerType,
            @NotNull MethodHandle accessor,
            @NotNull Class<?> functionalInterface,
            @NotNull String methodName,
            @NotNull Class<?> primitiveReturn) throws Throwable {

        MethodType invokedType = MethodType.methodType(functionalInterface);
        MethodType samMethod = MethodType.methodType(primitiveReturn, Object.class);
        MethodType implType = MethodType.methodType(primitiveReturn, ownerType);
        CallSite cs = LambdaMetafactory.metafactory(
            ROOT_LOOKUP,
            methodName,
            invokedType,
            samMethod,
            accessor,
            implType);
        return cs.getTarget().invoke();
    }

    /**
     * Build a {@link ToDoubleFunction} from a {@code (R) -> float} accessor.
     * LambdaMetafactory does the float→double widen via bridge — passing the
     * direct accessor as the implementation handle is the documented form.
     */
    private static @NotNull ToDoubleFunction<?> buildFloatAsDouble(
            @NotNull Class<?> ownerType,
            @NotNull MethodHandle accessor) throws Throwable {

        MethodType invokedType = MethodType.methodType(ToDoubleFunction.class);
        MethodType samMethod = MethodType.methodType(double.class, Object.class);
        MethodType implType = MethodType.methodType(float.class, ownerType);
        CallSite cs = LambdaMetafactory.metafactory(
            ROOT_LOOKUP,
            "applyAsDouble",
            invokedType,
            samMethod,
            accessor,
            implType);
        return (ToDoubleFunction<?>) cs.getTarget().invoke();
    }

    /** Build a generic Function<R, T>. */
    private static @NotNull Function<?, ?> buildReference(
            @NotNull Class<?> ownerType,
            @NotNull MethodHandle accessor,
            @NotNull Class<?> returnType) throws Throwable {

        MethodType invokedType = MethodType.methodType(Function.class);
        MethodType samMethod = MethodType.methodType(Object.class, Object.class);
        MethodType implType = MethodType.methodType(returnType, ownerType);
        CallSite cs = LambdaMetafactory.metafactory(
            ROOT_LOOKUP,
            "apply",
            invokedType,
            samMethod,
            accessor,
            implType);
        return (Function<?, ?>) cs.getTarget().invoke();
    }
}
