/*
 * Python-bindings bridge: bulk vertex creation for GraphBatch.
 *
 * GraphBatch.create_vertices() from Python marshals an Object[][] property
 * matrix element-by-element (~19us/vertex measured vs 6.2us Java-native).
 * This helper accepts the rows as ONE JSON string (json.dumps costs
 * ~0.2us/vertex Python-side) and returns the created RIDs as ONE
 * semicolon-joined string, so the whole batch costs two bulk string copies.
 *
 * JSON-representable property values only (str/int/float/bool/null and
 * nested lists/maps thereof) — the Python side falls back to the matrix
 * path for anything else (e.g. datetime, bytes).
 */
package com.arcadedb.python;

import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

public final class VertexBatcher {

  private VertexBatcher() {
  }

  public static String createVerticesJson(final GraphBatch batch, final String typeName, final String jsonRows) {
    final JSONArray rows = new JSONArray(jsonRows);
    final Object[][] matrix = new Object[rows.length()][];
    for (int i = 0; i < rows.length(); i++) {
      final JSONObject row = rows.getJSONObject(i);
      final Object[] flat = new Object[row.length() * 2];
      int j = 0;
      for (final String key : row.keySet()) {
        flat[j++] = key;
        flat[j++] = row.isNull(key) ? null : row.get(key);
      }
      matrix[i] = flat;
    }
    final RID[] rids = batch.createVertices(typeName, matrix);
    final StringBuilder sb = new StringBuilder(rids.length * 8);
    for (int i = 0; i < rids.length; i++) {
      if (i > 0)
        sb.append(';');
      sb.append(rids[i]);
    }
    return sb.toString();
  }
}
