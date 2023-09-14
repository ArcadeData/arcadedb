package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;

public class ArcadeTraversalBinder {

  private final Map<String, TraversalSupplier> traversalBindings;
  private final Graph graph;

  @FunctionalInterface
  public interface TraversalSupplier {
    TraversalSource get(Graph g);
  }

  ArcadeTraversalBinder(ArcadeGraph arcadeGraph) {
    graph = arcadeGraph;
    traversalBindings = arcadeGraph.getDatabase().getConfiguration()
        .getValue(GlobalConfiguration.GREMLIN_TRAVERSAL_BINDINGS);
  }

  void bind(GremlinScriptEngine engine) {
    traversalBindings.entrySet().forEach(e -> {
          var bindingName = e.getKey();
          var supplier = e.getValue();
          engine.put(bindingName, supplier.get(graph));
        });
  }
}
