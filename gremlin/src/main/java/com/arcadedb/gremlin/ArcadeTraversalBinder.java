package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;

/**
 * Binds a tinkerpop `TraversalSource` to a named script binding. See the following tinkerpop
 * documentation for more information:
 *
 * https://tinkerpop.apache.org/docs/current/reference/#gremlin-java-dsl
 * https://tinkerpop.apache.org/docs/current/reference/#traversalstrategy
 */
public final class ArcadeTraversalBinder {

  /**
   * Thrown in `ArcadeTraversalBinder` constructor if the `GREMLIN_TRAVERSAL_BINDINGS` configuration
   * contains invalid map entries.
   *
   * The `GlobalConfiguration.GREMLIN_TRAVERSAL_BINDINGS` is typed a a raw `Map` type in order to a
   * avoid circular dependencies on the arcadedb gremlin packages. Instead of declaring types,
   * validates the entries of the map at runtime as early as possible.
   */
  static final class IllegalTraversalBindingsEntry extends IllegalArgumentException {
    private IllegalTraversalBindingsEntry(String message) {
      super(message);
    }
  }

  private final Map<String, TraversalSupplier> traversalBindings;
  private final Graph graph;

  /**
   * A lambda builder interface which accepts a `Graph` object and returns a `TraversalSource`,
   * meant for use in binding a named entry point in the `GremlinScriptEngine`.
   */
  @FunctionalInterface
  public interface TraversalSupplier {

    /**
     * Convenience function for creating a travesal supplier for a particular traversal source
     * class. More custom requirements to configure a `TraversalSource` with extensive additions
     * should instead implement the lambda `TraversalSupplier` interface.
     */
    public static <T extends TraversalSource> TraversalSupplier of(Class<T> klass) {
      return graph -> graph.traversal(klass);
    }

    TraversalSource get(Graph g);
  }

  /**
   * @throws IllegalTraversalBindingsEntry
   */
  ArcadeTraversalBinder(ArcadeGraph arcadeGraph) {
    graph = arcadeGraph;
    traversalBindings = arcadeGraph.getDatabase().getConfiguration()
        .getValue(GlobalConfiguration.GREMLIN_TRAVERSAL_BINDINGS);
    validateTraversalBindings(traversalBindings);
  }

  void bind(GremlinScriptEngine engine) {
    traversalBindings.entrySet().forEach(e -> {
          var bindingName = e.getKey();
          var supplier = e.getValue();
          engine.put(bindingName, supplier.get(graph));
        });
  }

  private static void validateTraversalBindings(Map<String, TraversalSupplier> bindings) {
    bindings.entrySet().forEach(entry -> {
          try {
            String key = String.class.cast(entry.getKey());
            TraversalSupplier supplier = TraversalSupplier.class.cast(entry.getValue());
          } catch (ClassCastException e) {
            throw new IllegalTraversalBindingsEntry(
                "illegal Map<String, TraversalSupplier> entry in GREMLIN_TRAVERSAL_BINDINGS "
                + "configuration: %s".formatted(e));
          }
        });
  }
}
