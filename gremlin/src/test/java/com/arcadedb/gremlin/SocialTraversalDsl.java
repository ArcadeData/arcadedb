package com.arcadedb.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * See tinkerpop documentation:
 *
 * https://tinkerpop.apache.org/docs/current/reference/#gremlin-java-dsl
 */
@GremlinDsl
public interface SocialTraversalDsl<S, E> extends GraphTraversal.Admin<S, E> {

  public default GraphTraversal<S, Vertex> named(String name) {
    return V().hasLabel("Person").has("name", name);
  }

  public default GraphTraversal<S, Vertex> friend(String name) {
    return out("FriendOf").hasLabel("Person").has("name", name);
  }
}
