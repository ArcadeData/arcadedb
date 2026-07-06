/*
 * Python-bindings bridge: bulk edge buffering for GraphBatch.
 *
 * GraphBatch.newEdge() from Python costs one JPype crossing per edge
 * (measured 4.9us/edge vs 0.2us Java-native — 24x). This helper accepts the
 * whole edge list as String[] RIDs (marshaled in one bulk array copy) and
 * loops Java-side, so Python pays one crossing per batch.
 *
 * Property-less edges only — the common bulk-ingest shape; edges with
 * properties keep the per-edge path.
 */
package com.arcadedb.python;

import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;

public final class EdgeBatcher {

  private EdgeBatcher() {
  }

  public static void newEdges(final GraphBatch batch, final String[] sourceRids, final String edgeType,
      final String[] destinationRids) {
    if (sourceRids.length != destinationRids.length)
      throw new IllegalArgumentException(
          "sourceRids and destinationRids must have the same length (" + sourceRids.length + " vs "
              + destinationRids.length + ")");
    for (int i = 0; i < sourceRids.length; i++)
      batch.newEdge(new RID(sourceRids[i]), edgeType, new RID(destinationRids[i]));
  }

  /**
   * Same as {@link #newEdges} but takes semicolon-joined RID lists. JPype
   * converts a Python list of N strings to String[] element-by-element
   * (measured ~4us/edge — as slow as the per-edge path); a single joined
   * string crosses in one bulk copy and is split here.
   */
  public static void newEdgesJoined(final GraphBatch batch, final String joinedSourceRids, final String edgeType,
      final String joinedDestinationRids) {
    final String[] sources = joinedSourceRids.split(";");
    final String[] destinations = joinedDestinationRids.split(";");
    newEdges(batch, sources, edgeType, destinations);
  }

  /**
   * Bulk edges WITH properties: one JSON array of rows, each
   * {"_src": "#1:0", "_dst": "#1:1", ...properties}. JSON-representable
   * property values only; the Python side falls back per-edge otherwise.
   */
  public static void newEdgesJson(final GraphBatch batch, final String edgeType, final String jsonRows) {
    final com.arcadedb.serializer.json.JSONArray rows = new com.arcadedb.serializer.json.JSONArray(jsonRows);
    for (int i = 0; i < rows.length(); i++) {
      final com.arcadedb.serializer.json.JSONObject row = rows.getJSONObject(i);
      final RID src = new RID(row.getString("_src"));
      final RID dst = new RID(row.getString("_dst"));
      final Object[] props = new Object[(row.length() - 2) * 2];
      int j = 0;
      for (final String key : row.keySet()) {
        if (key.equals("_src") || key.equals("_dst"))
          continue;
        props[j++] = key;
        props[j++] = row.isNull(key) ? null : row.get(key);
      }
      if (props.length == 0)
        batch.newEdge(src, edgeType, dst);
      else
        batch.newEdge(src, edgeType, dst, props);
    }
  }
}
