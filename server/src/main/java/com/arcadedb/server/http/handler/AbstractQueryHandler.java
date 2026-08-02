/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;

import java.util.*;
import java.util.logging.Level;

import static com.arcadedb.schema.Property.RID_PROPERTY;

public abstract class AbstractQueryHandler extends DatabaseAbstractHandler {

  /**
   * Default value of {@link GlobalConfiguration#SERVER_HTTP_QUERY_DEFAULT_LIMIT}, kept as a constant for the
   * tests and the clients that referenced it before the cap became configurable. Handlers must read the
   * effective value from the server configuration via {@link #getDefaultRowLimit()}, never from this constant.
   */
  public static final int DEFAULT_LIMIT = (Integer) GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT.getDefValue();

  /**
   * Name of the response field carrying the number of rows serialized into the response.
   */
  public static final String RETURNED_FIELD = "returned";

  /**
   * Name of the response field carrying the effective row cap applied while serializing.
   */
  public static final String LIMIT_FIELD = "limit";

  /**
   * Name of the response field telling whether the cap cut the result short.
   */
  public static final String TRUNCATED_FIELD = "truncated";

  /**
   * Upper bound on how much of a command is echoed in the truncation warning, so a large payload cannot flood
   * the log: the leading characters are what identifies the query for an operator.
   */
  private static final int MAX_LOGGED_COMMAND_CHARS = 120;

  /**
   * Outcome of serializing a {@link ResultSet} into an HTTP response: how many rows reached the response and
   * whether the effective limit cut the result short.
   * <p>
   * {@code truncated} is true only when the cap was reached <i>and</i> the result set still had at least one
   * more row, so a result whose size happens to match the cap exactly is not reported as truncated. For the
   * row-oriented serializers ({@code record}, {@code studio} and the default one) the flag is exact and
   * {@code returned} never exceeds the cap. For the {@code graph} serializer the cap counts graph elements
   * rather than rows: {@code returned} can exceed the cap, because the row that reaches it is expanded whole,
   * and a last row whose expansion was cut mid-way is not reported when no further row is pending.
   */
  public record SerializationOutcome(int returned, boolean truncated) {
    static final SerializationOutcome EMPTY = new SerializationOutcome(0, false);
  }

  public AbstractQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * Resolves the row cap to apply while serializing, in decreasing order of explicitness:
   * <ol>
   * <li>the {@code limit} field of the request, when present: the caller stated the size of the page it
   * wants, and that statement wins over everything else (a value {@code <= 0} means unlimited);</li>
   * <li>otherwise the configured default, raised to the LIMIT the query itself carries when the engine
   * exposes it on the execution plan: honoring it is what keeps the HTTP surface from silently returning
   * fewer rows than the query asked for (issue #5711).</li>
   * </ol>
   * The plan LIMIT can only raise the cap, never lower it. A LIMIT smaller than the default is already
   * enforced by the engine, so capping at it would be redundant - and the plan of a query whose LIMIT is a
   * parameter can be a cached copy still carrying the value of an earlier execution, so trusting it downwards
   * would cut a result the engine had sized correctly.
   *
   * @param requestLimit {@code limit} field of the request, or {@code null} when absent
   * @param planLimit    LIMIT exposed by the execution plan, or 0 when the query carries none / the language
   *                     does not expose it
   */
  protected int resolveLimit(final Integer requestLimit, final int planLimit) {
    if (requestLimit != null)
      return requestLimit;
    final int defaultLimit = getDefaultRowLimit();
    // A non-positive default means the operator removed the cap: nothing to raise.
    return defaultLimit > 0 ? Math.max(planLimit, defaultLimit) : defaultLimit;
  }

  /**
   * Parses the textual row limit of the GET endpoint, or returns {@code null} when the parameter is absent or
   * empty - an empty parameter states nothing, so it must not be read as a value. A value that is not an
   * int-representable integer is a client error carrying the same message as the JSON field of the POST
   * endpoints, instead of the raw {@link NumberFormatException} wording of the JDK.
   */
  protected static Integer parseLimitParameter(final String raw, final String field) {
    if (raw == null || raw.isBlank())
      return null;
    try {
      return Integer.valueOf(raw.trim());
    } catch (final NumberFormatException e) {
      throw unusableLimit(field, e);
    }
  }

  /**
   * LIMIT carried by the execution plan of the given result set, or 0 when unavailable. SQL exposes it for
   * both single statements and single-statement scripts; languages that do not build an
   * {@link com.arcadedb.query.sql.executor.ExecutionPlan} (e.g. Cypher on the non-EXPLAIN path) return 0, and
   * their results are capped by the configured default with {@code truncated} reported when it bites.
   * <p>
   * A multi-statement script reports 0 today, but a value belonging to one of its statements would be
   * harmless anyway: {@link #resolveLimit} only ever raises the cap with it, so a wrong value can widen the
   * response but never cut it below what the configured default already allows.
   */
  protected int getPlanLimit(final ResultSet qResult) {
    if (qResult == null)
      return 0;
    try {
      return qResult.getExecutionPlan().map(ExecutionPlan::getLimit).orElse(0);
    } catch (final Exception e) {
      // A language whose plan computes the limit lazily may fail here: fall back to "no limit exposed".
      LogManager.instance().log(this, Level.FINE, "Cannot read the LIMIT from the execution plan", e);
      return 0;
    }
  }

  /**
   * Reports the effective cap, the number of rows serialized and whether the result was cut short, so a
   * truncated response is never indistinguishable from a complete one (issue #5711). Always present on the
   * query/command endpoints, so a client can test {@code truncated} unconditionally.
   */
  protected static void reportLimits(final JSONObject response, final int limit, final SerializationOutcome outcome) {
    response.put(LIMIT_FIELD, limit > 0 ? limit : -1);
    response.put(RETURNED_FIELD, outcome.returned());
    response.put(TRUNCATED_FIELD, outcome.truncated());
  }

  /**
   * Logs a warning when the result was cut by the configured default rather than by a cap the caller asked
   * for: that is the only truncation the client did not ask for, so an operator must be able to find it in
   * the log. Truncation against an explicit request {@code limit} (or against the query's own LIMIT) is the
   * expected outcome and stays silent.
   */
  protected void logIfTruncatedByDefault(final String databaseName, final String command, final int limit,
      final Integer requestLimit, final int planLimit, final SerializationOutcome outcome) {
    // planLimit > limit cannot happen (resolveLimit raises the cap to it), so this only skips the queries whose
    // own LIMIT is what the cap came from.
    if (!outcome.truncated() || requestLimit != null || planLimit >= limit)
      return;

    // The advice deliberately leads with the request field: a LIMIT in the query text raises the cap only for a
    // language that exposes it on the execution plan, so telling a Cypher caller to add one - it may well have
    // one already - would send it after a fix that cannot work.
    LogManager.instance().log(this, Level.WARNING,
        "Query on database '%s' returned more rows than the default HTTP limit of %d: the response has been truncated to %d rows. "
            + "Set 'limit' in the request or raise '%s'; a LIMIT written in the query raises the cap only for a language that "
            + "exposes it on the execution plan, which SQL does and Cypher does not. Query: %s", databaseName, limit,
        outcome.returned(), GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT.getKey(), abbreviateForLog(command));
  }

  /**
   * Prepares a client-supplied command to be echoed in a log line: caps the length so a large payload cannot
   * flood the log, and replaces control characters with a space so an embedded line break cannot forge log
   * lines of its own. Same concern - and same shape - as
   * {@link AbstractServerHttpHandler#sanitizeRequestId(String)} for the {@code X-Request-Id} header; allocates
   * only when the command actually needs cleaning. Package-private for direct unit testing.
   */
  static String abbreviateForLog(final String command) {
    if (command == null)
      return null;
    final int len = Math.min(command.length(), MAX_LOGGED_COMMAND_CHARS);
    StringBuilder cleaned = null;
    for (int i = 0; i < len; i++) {
      final char c = command.charAt(i);
      if (c < 0x20 || c == 0x7F) {
        if (cleaned == null)
          cleaned = new StringBuilder(len).append(command, 0, i);
        cleaned.append(' ');
      } else if (cleaned != null)
        cleaned.append(c);
    }
    final String result = cleaned != null ? cleaned.toString() : command.substring(0, len);
    return command.length() > MAX_LOGGED_COMMAND_CHARS ? result + "..." : result;
  }

  protected SerializationOutcome serializeResultSet(final Database database, final String serializer, final int limit,
      final JSONObject response, final ResultSet qResult) {
    if (qResult == null)
      return SerializationOutcome.EMPTY;

    try {
    switch (serializer) {
    case "graph": {
      // SERIALIZES THE GRAPH ELEMENTS IN VERTICES AND EDGES
      final JsonGraphSerializer serializerImpl = JsonGraphSerializer
          .createJsonGraphSerializer()
          .setExpandVertexEdges(false);
      // Don't use collection size for edges - we want COLLECT(rel) to return edge objects, not counts
      serializerImpl.setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false);

      final Set<RID> includedVertices = new HashSet<>();
      final Set<RID> includedEdges = new HashSet<>();
      final JSONArray vertices = new JSONArray();
      final JSONArray edges = new JSONArray();

      while (qResult.hasNext()) {
        final Result row = qResult.next();

        if (row.isVertex()) {
          final Vertex v = row.getVertex().get();
          if (includedVertices.add(v.getIdentity()))
            vertices.put(serializerImpl.serializeGraphElement(v));
        } else if (row.isEdge()) {
          final Edge e = row.getEdge().get();
          if (includedEdges.add(e.getIdentity()))
            edges.put(serializerImpl.serializeGraphElement(e));
        } else {
          analyzeResultContent(database, serializerImpl, includedVertices, includedEdges, vertices, edges, row, limit);
        }

        if (limit > 0 && vertices.length() + edges.length() >= limit)
          break;
      }

      response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges));
      final int serializedElements = vertices.length() + edges.length();
      return new SerializationOutcome(serializedElements, isTruncated(qResult, limit, serializedElements));
    }

    case "studio": {
      // USE BY STUDIO TO RENDER GRAPH AND TABLE AT THE SAME TIME
      final JsonGraphSerializer serializerImpl = JsonGraphSerializer.createJsonGraphSerializer()
          .setExpandVertexEdges(false);
      // Don't use collection size for edges - we want COLLECT(rel) to return edge objects, not counts (issue #3404)
      serializerImpl.setUseCollectionSize(false).setUseCollectionSizeForEdges(false);

      final Set<RID> includedVertices = new HashSet<>();
      final Set<RID> includedEdges = new HashSet<>();
      final JSONArray vertices = new JSONArray();
      final JSONArray edges = new JSONArray();
      final JSONArray records = new JSONArray();

      while (qResult.hasNext()) {
        final Result row = qResult.next();

        try {
          // Always add to records without deduplication - UNWIND queries can legitimately
          // return the same RID multiple times with different values (issue #1582)
          records.put(serializerImpl.serializeResult(database, row));

          if (row.isVertex()) {
            final Vertex v = row.getVertex().get();
            if (includedVertices.add(v.getIdentity()))
              vertices.put(serializerImpl.serializeGraphElement(v));
          } else if (row.isEdge()) {
            final Edge e = row.getEdge().get();
            if (includedEdges.add(e.getIdentity())) {
              edges.put(serializerImpl.serializeGraphElement(e));
              try {
                if (includedVertices.add(e.getIn())) {
                  vertices.put(serializerImpl.serializeGraphElement(e.getInVertex()));
                }
                if (includedVertices.add(e.getOut())) {
                  vertices.put(serializerImpl.serializeGraphElement(e.getOutVertex()));
                }
              } catch (RecordNotFoundException ex) {
                LogManager.instance().log(this, Level.SEVERE, "Record %s not found during serialization", ex.getRID());
              }
            }
          } else {
            analyzeResultContent(database, serializerImpl, includedVertices, includedEdges, vertices, edges, row, limit);
          }
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on serializing element (error=%s)", e.getMessage());
        }

        if (limit > 0 && records.length() >= limit)
          break;
      }

      // Probed before the edge-completion pass below, which does not consume the result set but must not be
      // allowed to hide whether rows were left behind.
      final boolean truncated = isTruncated(qResult, limit, records.length());

      // FILTER OUT NOT CONNECTED EDGES
      for (final Identifiable entry : includedVertices) {
        if (limit > 0 && vertices.length() + edges.length() >= limit)
          break;

        try {
          final Vertex vertex = entry.asVertex(true);

          final Iterable<Edge> vEdgesOut = vertex.getEdges(Vertex.DIRECTION.OUT);
          for (final Edge e : vEdgesOut) {
            if (includedVertices.contains(e.getIn()) && !includedEdges.contains(e.getIdentity())) {
              edges.put(serializerImpl.serializeGraphElement(e));
              includedEdges.add(e.getIdentity());
            }
          }

          final Iterable<Edge> vEdgesIn = vertex.getEdges(Vertex.DIRECTION.IN);
          for (final Edge e : vEdgesIn) {
            if (includedVertices.contains(e.getOut()) && !includedEdges.contains(e.getIdentity())) {
              edges.put(serializerImpl.serializeGraphElement(e));
              includedEdges.add(e.getIdentity());
            }
          }
        } catch (RecordNotFoundException e) {
          LogManager.instance().log(this, Level.SEVERE, "Vertex %s not found during serialization", e.getRID());
        }
      }

      response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges).put("records", records));
      return new SerializationOutcome(records.length(), truncated);
    }

    case "record":
      return serializeRows(database, qResult, limit, response, JsonSerializer.createJsonSerializer()
          .setIncludeVertexEdges(false)
          .setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false));

    default:
      return serializeRows(database, qResult, limit, response, JsonSerializer.createJsonSerializer()
          .setIncludeVertexEdges(true)
          .setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false));
    }
    } finally {
      qResult.close();
    }
  }

  /**
   * Serializes a result set as a flat array of rows, stopping at {@code limit} rows (0 or less = unlimited).
   * Exactly {@code limit} rows are emitted when the cap bites: the row that does not fit is left in the
   * result set on purpose, and is what {@link #isTruncated} probes to tell a truncated response from a
   * complete one.
   */
  private static SerializationOutcome serializeRows(final Database database, final ResultSet qResult, final int limit,
      final JSONObject response, final JsonSerializer serializerImpl) {
    final JSONArray result = new JSONArray();
    while (qResult.hasNext()) {
      final Result r = qResult.next();
      result.put(serializerImpl.serializeResult(database, r));
      if (limit > 0 && result.length() >= limit)
        break;
    }
    response.put("result", result);
    return new SerializationOutcome(result.length(), isTruncated(qResult, limit, result.length()));
  }

  /**
   * True when the cap stopped the serialization with at least one row still pending in the result set.
   * <p>
   * That pending row comes from a different place on the two surfaces, and both are exact. The GET endpoint runs
   * the query as written, so a lazy result set still has rows of its own when the cap bites. The POST endpoints
   * rewrite the command to push a LIMIT down, which would otherwise make the engine stop at exactly the cap and
   * leave the truncation undetectable, so what they push down is one row above it (see
   * {@link PostCommandHandler#truncationProbeLimit(int)}).
   */
  private static boolean isTruncated(final ResultSet qResult, final int limit, final int serialized) {
    return limit > 0 && serialized >= limit && qResult.hasNext();
  }

  protected void analyzeResultContent(final Database database, final JsonGraphSerializer serializerImpl,
      final Set<RID> includedVertices, final Set<RID> includedEdges, final JSONArray vertices, final JSONArray edges,
      final Result row, final int limit) {
    for (final String prop : row.getPropertyNames()) {
      try {
        final Object value = row.getProperty(prop);
        if (value == null)
          continue;

        if (limit > 0 && vertices.length() + edges.length() >= limit)
          break;

        if (RID_PROPERTY.equals(prop) && RID.is(value)) {
          analyzePropertyValue(database, serializerImpl, includedVertices, includedEdges, vertices, edges,
              database.newRID(value.toString()), limit);
        } else
          analyzePropertyValue(database, serializerImpl, includedVertices, includedEdges, vertices, edges, value, limit);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on serializing collection element (error=%s)", e.getMessage());
      }
    }
  }

  protected void analyzePropertyValue(final Database database, final JsonGraphSerializer serializerImpl,
      final Set<RID> includedVertices, final Set<RID> includedEdges, final JSONArray vertices, final JSONArray edges,
      final Object value, final int limit) {
    if (value instanceof Identifiable identifiable) {

      final DocumentType type;
      if (value instanceof Document document)
        type = document.getType();
      else {
        final RID rid = identifiable.getIdentity();
        type = database.getSchema().getTypeByBucketId(rid.getBucketId());
      }

      if (type instanceof LocalVertexType) {
        if (includedVertices.add(((Identifiable) value).getIdentity()))
          vertices.put(serializerImpl.serializeGraphElement(((Identifiable) value).asVertex(true)));
      } else if (type instanceof LocalEdgeType) {
        final Edge edge = ((Identifiable) value).asEdge(true);
        if (includedEdges.add(edge.getIdentity())) {
          edges.put(serializerImpl.serializeGraphElement(edge));
          try {
            if (includedVertices.add(edge.getIn())) {
              final Vertex inV = edge.getInVertex();
              vertices.put(serializerImpl.serializeGraphElement(inV));
            }
            if (includedVertices.add(edge.getOut())) {
              final Vertex outV = edge.getOutVertex();
              vertices.put(serializerImpl.serializeGraphElement(outV));
            }
          } catch (RecordNotFoundException e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on loading connecting vertices for edge %s: vertex %s not found",
                edge.getIdentity(), e.getRID());
          }
        }
      }
    } else if (value instanceof Result result) {
      analyzeResultContent(database, serializerImpl, includedVertices, includedEdges, vertices, edges, result, limit);
    } else if (value instanceof Collection<?> collection) {
      for (final Iterator<?> it = collection.iterator(); it.hasNext(); ) {
        try {
          analyzePropertyValue(database, serializerImpl, includedVertices, includedEdges, vertices, edges, it.next(), limit);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on serializing collection element (error=%s)", e.getMessage());
        }
      }
    }
  }

  protected Object mapParams(Map<String, Object> paramMap) {
    if (paramMap != null) {
      if (!paramMap.isEmpty() && paramMap.containsKey("0")) {
        // ORDINAL
        final Object[] array = new Object[paramMap.size()];
        for (int i = 0; i < array.length; ++i) {
          array[i] = paramMap.get("" + i);
        }
        return array;
      }
    } else
      paramMap = Collections.emptyMap();
    return paramMap;
  }

  /**
   * Hard ceiling on nested {@code Map}/{@code List} depth that {@link #decodeTypedJsonMarker}
   * traverses, mostly to avoid {@link StackOverflowError} on a hostile or accidentally-deep JSON
   * payload. The HTTP JSON parser already imposes its own limits, but the decoder runs after the
   * parser and only inherits whatever shape arrived; a defensive guard here keeps the contract
   * explicit. 32 levels is well past anything a real query parameter would carry.
   */
  private static final int MAX_TYPED_MARKER_DEPTH = 32;

  /**
   * Decodes typed JSON markers ({@code $bytes}, {@code $int8}) in {@code paramMap} into
   * {@code byte[]} so HTTP/JSON clients can route int8 query vectors through the encoding-aware
   * vector path (#4135). Returns the original map reference when nothing was rewritten.
   * <p>
   * {@code $bytes} accepts standard or URL-safe base64 (RFC 4648 sections 4 and 5); {@code $int8}
   * accepts a {@link List} of {@link Number}, {@code float[]}, {@code double[]}, {@code int[]}, or
   * {@code long[]} of values in {@code [-128, 127]}.
   */
  protected static Map<String, Object> decodeTypedJsonMarkers(final Map<String, Object> paramMap) {
    if (paramMap == null || paramMap.isEmpty())
      return paramMap;
    Map<String, Object> result = null;
    for (final Map.Entry<String, Object> entry : paramMap.entrySet()) {
      final Object original = entry.getValue();
      final Object decoded = decodeTypedJsonMarker(original, 0);
      if (decoded != original && result == null) {
        result = new LinkedHashMap<>(paramMap);
      }
      if (result != null)
        result.put(entry.getKey(), decoded);
    }
    return result != null ? result : paramMap;
  }

  private static Object decodeTypedJsonMarker(final Object value, final int depth) {
    if (depth > MAX_TYPED_MARKER_DEPTH)
      throw new IllegalArgumentException(
          "Parameter nesting exceeds " + MAX_TYPED_MARKER_DEPTH + " levels - typed-marker decoder refuses to recurse further.");
    if (value instanceof Map<?, ?> m && m.size() == 1) {
      final Map.Entry<?, ?> only = m.entrySet().iterator().next();
      if ("$bytes".equals(only.getKey()))
        return decodeBytesMarker(only.getValue());
      if ("$int8".equals(only.getKey()))
        return decodeInt8Marker(only.getValue());
    }
    if (value instanceof Map<?, ?> m) {
      Map<String, Object> rewritten = null;
      int idx = 0;
      for (final Map.Entry<?, ?> entry : m.entrySet()) {
        final Object key = entry.getKey();
        if (!(key instanceof String sk))
          throw new IllegalArgumentException(
              "Parameter map keys must be strings, found " + (key == null ? "null" : key.getClass().getSimpleName()));
        final Object original = entry.getValue();
        final Object decoded = decodeTypedJsonMarker(original, depth + 1);
        if (decoded != original && rewritten == null) {
          rewritten = new LinkedHashMap<>(m.size());
          int j = 0;
          for (final Map.Entry<?, ?> prior : m.entrySet()) {
            if (j == idx)
              break;
            rewritten.put((String) prior.getKey(), prior.getValue());
            j++;
          }
        }
        if (rewritten != null)
          rewritten.put(sk, decoded);
        idx++;
      }
      return rewritten != null ? rewritten : value;
    }
    if (value instanceof List<?> list) {
      List<Object> rewritten = null;
      for (int i = 0; i < list.size(); i++) {
        final Object original = list.get(i);
        final Object decoded = decodeTypedJsonMarker(original, depth + 1);
        if (decoded != original && rewritten == null) {
          rewritten = new ArrayList<>(list.size());
          for (int j = 0; j < i; j++)
            rewritten.add(list.get(j));
        }
        if (rewritten != null)
          rewritten.add(decoded);
      }
      return rewritten != null ? rewritten : value;
    }
    return value;
  }

  private static byte[] decodeBytesMarker(final Object payload) {
    if (payload == null)
      throw new IllegalArgumentException("Parameter '$bytes' value must be a base64 string, got null");
    if (!(payload instanceof String b64))
      throw new IllegalArgumentException(
          "Parameter '$bytes' value must be a base64 string, found " + payload.getClass().getSimpleName());
    // Try standard base64 (RFC 4648 section 4) first; on failure retry with URL-safe (section 5)
    // so clients using - and _ in place of + and / are accepted transparently.
    try {
      return Base64.getDecoder().decode(b64);
    } catch (final IllegalArgumentException standardErr) {
      try {
        return Base64.getUrlDecoder().decode(b64);
      } catch (final IllegalArgumentException urlSafeErr) {
        throw new IllegalArgumentException(
            "Parameter '$bytes' is not valid base64 (standard or URL-safe): " + standardErr.getMessage(), standardErr);
      }
    }
  }

  private static byte[] decodeInt8Marker(final Object payload) {
    // optimizeNumericArrays=true on the JSON parser may convert a JSON integer array into a
    // float[]/double[]; some callers may also pass int[] or long[]. Accept all four primitive
    // shapes and the boxed List<Number> form.
    if (payload instanceof List<?> list) {
      final byte[] out = new byte[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (!(elem instanceof Number n))
          throw new IllegalArgumentException(
              "Parameter '$int8' element at index " + i + " must be a number, found "
                  + (elem == null ? "null" : elem.getClass().getSimpleName()));
        out[i] = toInt8(n.doubleValue(), i);
      }
      return out;
    }
    if (payload instanceof float[] floats) {
      final byte[] out = new byte[floats.length];
      for (int i = 0; i < floats.length; i++)
        out[i] = toInt8(floats[i], i);
      return out;
    }
    if (payload instanceof double[] doubles) {
      final byte[] out = new byte[doubles.length];
      for (int i = 0; i < doubles.length; i++)
        out[i] = toInt8(doubles[i], i);
      return out;
    }
    if (payload instanceof int[] ints) {
      final byte[] out = new byte[ints.length];
      for (int i = 0; i < ints.length; i++)
        out[i] = toInt8(ints[i], i);
      return out;
    }
    if (payload instanceof long[] longs) {
      final byte[] out = new byte[longs.length];
      for (int i = 0; i < longs.length; i++)
        out[i] = toInt8(longs[i], i);
      return out;
    }
    throw new IllegalArgumentException(
        "Parameter '$int8' value must be an array of integers in [-128, 127], found "
            + (payload == null ? "null" : payload.getClass().getSimpleName()));
  }

  /** Rounds a numeric value to a signed byte, rejecting fractional or out-of-range inputs. */
  private static byte toInt8(final double v, final int index) {
    // v != Math.floor(v) catches NaN (NaN compared with anything is false, so != returns true)
    // and any value with a non-zero fractional part. Infinity slips through here but is caught by
    // the range check below since +/-Infinity exceeds [-128, 127].
    if (v != Math.floor(v))
      throw new IllegalArgumentException(
          "Parameter '$int8' element at index " + index + " is not an integer value: " + v);
    if (v < -128.0 || v > 127.0)
      throw new IllegalArgumentException(
          "Parameter '$int8' element at index " + index + " is out of byte range [-128, 127]: " + v);
    return (byte) v;
  }
}
