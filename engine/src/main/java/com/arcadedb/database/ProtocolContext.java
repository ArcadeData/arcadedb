package com.arcadedb.database;

/**
 * Carries the originating wire protocol of the current request on a thread-local so the engine can
 * tag {@code arcadedb.query.duration} without each protocol re-implementing query timing. Wire
 * listeners {@link #set} it at the request boundary and {@link #clear} it in a finally block (worker
 * and connection threads are pooled/reused). Defaults to {@link #INTERNAL} for engine-internal and
 * embedded-API callers.
 */
public final class ProtocolContext {
  public static final String INTERNAL = "internal";

  private static final ThreadLocal<String> CURRENT = ThreadLocal.withInitial(() -> INTERNAL);

  private ProtocolContext() {
  }

  public static void set(final String protocol) {
    CURRENT.set(protocol != null ? protocol : INTERNAL);
  }

  public static String get() {
    return CURRENT.get();
  }

  public static void clear() {
    CURRENT.remove();
  }
}
