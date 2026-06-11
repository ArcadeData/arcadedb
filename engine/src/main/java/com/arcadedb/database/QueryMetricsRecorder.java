package com.arcadedb.database;

/**
 * Framework-agnostic hook the engine calls to report the duration of a query/command execution.
 * Lives in the engine (which has Micrometer only at test scope); the server registers a
 * Micrometer-backed implementation. Default is a no-op, so when no recorder is registered the
 * engine pays only a volatile read and a sentinel check per query - no timing, no allocation.
 */
@FunctionalInterface
public interface QueryMetricsRecorder {
  QueryMetricsRecorder NO_OP = (protocol, database, language, type, durationNanos) -> {
  };

  /**
   * @param protocol      originating wire protocol (e.g. http, bolt, postgres, internal)
   * @param database      database name
   * @param language      query language (sql, opencypher, ...)
   * @param type          query or command
   * @param durationNanos elapsed nanoseconds
   */
  void record(String protocol, String database, String language, String type, long durationNanos);

  /** Static registration + timing helpers, kept off the interface's public surface. */
  final class Holder {
    private static volatile QueryMetricsRecorder current = NO_OP;

    private Holder() {
    }

    public static void register(final QueryMetricsRecorder recorder) {
      current = recorder != null ? recorder : NO_OP;
    }

    public static QueryMetricsRecorder get() {
      return current;
    }

    /** Returns System.nanoTime() when a recorder is active, otherwise 0 (the "not timing" sentinel). */
    public static long startNanos() {
      return current == NO_OP ? 0L : System.nanoTime();
    }

    /** Records elapsed time since {@code start}; a {@code start} of 0 means timing was inactive and is ignored. */
    public static void record(final long start, final String database, final String language, final String type) {
      if (start == 0L)
        return;
      current.record(ProtocolContext.get(), database, language, type, System.nanoTime() - start);
    }
  }
}
