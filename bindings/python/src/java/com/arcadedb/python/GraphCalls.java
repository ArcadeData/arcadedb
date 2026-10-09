/*
 * Python-bindings bridge: per-call glue for graph traversal and vector search results.
 *
 * Same idea as DbCalls: a JPype call costs a few microseconds of dispatch, so the paths that make one call per edge
 * or per hit are dominated by the number of calls. Iterating the Iterable that Vertex.getEdges() returns took a
 * hasNext() and a next() crossing per edge, and wrapping a vector search hit took getFirst(), getSecond(), and the
 * record lookup. These return the same objects in one crossing, as arrays whose elements Python reads without method
 * dispatch.
 *
 * Compiled into arcadedb-python-bridge.jar during the wheel build and consumed by Vertex.get_out_edges() and its
 * siblings and by the vector index search methods in the Python bindings.
 */
package com.arcadedb.python;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.List;

public final class GraphCalls {

  private GraphCalls() {
  }

  private static Edge[] drain(final Iterable<Edge> edges) {
    final List<Edge> list = new ArrayList<>();
    for (final Edge edge : edges)
      list.add(edge);
    return list.toArray(new Edge[0]);
  }

  /** The edges of {@code vertex.getEdges(Vertex.DIRECTION.OUT, labels)}, in iteration order. */
  public static Edge[] outEdges(final Vertex vertex, final String... labels) {
    return drain(vertex.getEdges(Vertex.DIRECTION.OUT, labels));
  }

  /** The edges of {@code vertex.getEdges(Vertex.DIRECTION.IN, labels)}, in iteration order. */
  public static Edge[] inEdges(final Vertex vertex, final String... labels) {
    return drain(vertex.getEdges(Vertex.DIRECTION.IN, labels));
  }

  /** The edges of {@code vertex.getEdges(Vertex.DIRECTION.BOTH, labels)}, in iteration order. */
  public static Edge[] bothEdges(final Vertex vertex, final String... labels) {
    return drain(vertex.getEdges(Vertex.DIRECTION.BOTH, labels));
  }

  /**
   * The records and scores of vector search hits: for each {@code Pair<RID, Float>}, {@code db.lookupByRID(rid, true)}
   * and the score as a double. Returns {@code {Object[] records, double[] scores}}; a record is null where the engine
   * returned null, so the caller can name that hit.
   */
  public static Object[] hits(final Database db, final Iterable<?> pairs) {
    final List<Object> records = new ArrayList<>();
    final List<Double> scores = new ArrayList<>();
    for (final Object item : pairs) {
      final Pair<?, ?> pair = (Pair<?, ?>) item;
      records.add(db.lookupByRID((RID) pair.getFirst(), true));
      scores.add(((Number) pair.getSecond()).doubleValue());
    }
    final double[] s = new double[scores.size()];
    for (int i = 0; i < s.length; i++)
      s[i] = scores.get(i);
    return new Object[] { records.toArray(), s };
  }
}
