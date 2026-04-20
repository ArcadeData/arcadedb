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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * High-performance, declarative graph importer using a CSR-first architecture:
 * <ol>
 *   <li><b>Pass 1</b> — Process each data source once: create vertices with full properties,
 *       collect graph topology as compressed int arrays (~300 MB for 8M vertices / 15M edges).</li>
 *   <li><b>Pass 2</b> — Create all edges from the in-memory topology, one batch per edge type
 *       with bidirectional=true for full IN+OUT traversal.</li>
 * </ol>
 * <p>
 * Usage:
 * <pre>
 * GraphImporter.builder(database)
 *     .vertex("User", xmlSource, v -&gt; {
 *         v.id("Id");
 *         v.property("displayName", "DisplayName");
 *         v.intProperty("reputation", "Reputation");
 *     })
 *     .vertex("Post", xmlSource, v -&gt; {
 *         v.id("Id");
 *         v.property("title", "Title");
 *         v.edgeIn("OwnerUserId", "Posted", "User");   // User→Post
 *         v.edgeOut("ParentId", "AnswerOf", "Post");    // Post→Post (deferred)
 *     })
 *     .build()
 *     .run();
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see GraphBatch
 */
public class GraphImporter implements AutoCloseable {

  private final Database                            database;
  private final List<VertexSourceDef>               vertexSources;
  private final List<EdgeSourceDef>                 edgeSources;
  private final long                                limit;
  private final Map<String, TypeState>              typeStates     = new LinkedHashMap<>();
  private final Map<String, EdgeCollector>          edgeCollectors = new LinkedHashMap<>();
  private final Map<String, List<DeferredEdgePair>> deferredEdges  = new HashMap<>();

  private long totalVertices;
  private long totalEdges;

  private GraphImporter(final Database database, final List<VertexSourceDef> vertexSources,
                        final List<EdgeSourceDef> edgeSources, final long limit) {
    this.database = database;
    this.vertexSources = vertexSources;
    this.edgeSources = edgeSources;
    this.limit = limit;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Command-line entry point
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Usage: {@code GraphImporter <json-config-file> <database-path> [data-dir]}
   * <p>
   * The JSON file describes vertex/edge sources and mappings. File paths in the JSON are resolved
   * relative to {@code data-dir} (defaults to the JSON file's parent directory).
   * The database is created fresh (any existing data is deleted).
   */
  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: GraphImporter <json-config-file> <database-path> [data-dir]");
      System.exit(1);
    }

    final File jsonFile = new File(args[0]);
    final String dbPath = args[1];
    final String baseDir = args.length > 2 ? args[2] : jsonFile.getParent();

    final String json = new String(java.nio.file.Files.readAllBytes(jsonFile.toPath()));
    final JSONObject config = new JSONObject(json);

    com.arcadedb.utility.FileUtils.deleteRecursively(new File(dbPath));
    final Database database = new com.arcadedb.database.DatabaseFactory(dbPath).create();

    try {
      // Auto-create schema from the JSON config
      createSchemaFromConfig(database, config);

      try (final GraphImporter importer = fromJSON(database, config, baseDir)) {
        importer.run();
        System.out.printf("Vertices: %,d%nEdges   : %,d%n", importer.getVertexCount(), importer.getEdgeCount());
      }

      // Execute post-import commands (e.g., CREATE GRAPH ANALYTICAL VIEW)
      executePostImportCommands(database, config);
    } finally {
      database.close();
    }
  }

  /** Creates vertex and edge types declared in the JSON config (if they don't already exist). */
  public static void createSchemaFromConfig(final Database database, final JSONObject config) {
    database.transaction(() -> {
      if (config.has("vertices")) {
        final JSONArray vertices = config.getJSONArray("vertices");
        for (int i = 0; i < vertices.length(); i++) {
          final JSONObject vj = vertices.getJSONObject(i);
          final String typeName = vj.getString("type");
          if (!database.getSchema().existsType(typeName))
            database.getSchema().createVertexType(typeName);
          if (vj.has("edges")) {
            final JSONArray edges = vj.getJSONArray("edges");
            for (int j = 0; j < edges.length(); j++) {
              final String edgeType = edges.getJSONObject(j).getString("edge");
              if (!database.getSchema().existsType(edgeType))
                database.getSchema().createEdgeType(edgeType);
            }
          }
        }
      }
      if (config.has("edgeSources")) {
        final JSONArray edgeSources = config.getJSONArray("edgeSources");
        for (int i = 0; i < edgeSources.length(); i++) {
          final String edgeType = edgeSources.getJSONObject(i).getString("edge");
          if (!database.getSchema().existsType(edgeType))
            database.getSchema().createEdgeType(edgeType);
        }
      }
    });
  }

  /**
   * Executes post-import commands defined in the JSON config.
   * <p>
   * Format:
   * <pre>
   * "postImportCommands": [
   *   { "language": "sql", "command": "CREATE GRAPH ANALYTICAL VIEW ..." }
   * ]
   * </pre>
   */
  public static void executePostImportCommands(final Database database, final JSONObject config) {
    if (!config.has("postImportCommands"))
      return;

    final JSONArray commands = config.getJSONArray("postImportCommands");
    for (int i = 0; i < commands.length(); i++) {
      final JSONObject cmd = commands.getJSONObject(i);
      final String language = cmd.getString("language");
      final String command = cmd.getString("command");

      LogManager.instance().log(GraphImporter.class, Level.INFO, "Executing post-import command [%s]: %s", language, command);
      try {
        database.command(language, command).close();
      } catch (final Exception e) {
        LogManager.instance().log(GraphImporter.class, Level.WARNING, "Post-import command failed: %s", e, command);
      }
    }

    // Wait for any async GAV builds triggered by post-import commands
    for (final GraphAnalyticalView gav : GraphAnalyticalViewRegistry.getAll(database).values()) {
      if (gav.getStatus() != GraphAnalyticalView.Status.READY) {
        LogManager.instance().log(GraphImporter.class, Level.INFO, "Waiting for GraphAnalyticalView '%s' to finish building...", gav.getName());
        gav.awaitReady(10, java.util.concurrent.TimeUnit.MINUTES);
        LogManager.instance().log(GraphImporter.class, Level.INFO, "GraphAnalyticalView '%s' is ready", gav.getName());
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  JSON Configuration
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Creates a GraphImporter from a JSON configuration string.
   * <p>
   * Format (concise):
   * <pre>
   * {
   *   "vertices": [
   *     {
   *       "type": "User", "file": "Users.xml", "id": "Id",
   *       "properties": { "name": "DisplayName", "score": "int:Score", "active": "bool:Active" },
   *       "edges": [
   *         { "attribute": "FriendId", "edge": "Knows", "target": "User" },
   *         { "attribute": "ManagerId", "edge": "ManagedBy", "target": "User", "direction": "in" },
   *         { "attribute": "Tags", "edge": "HasTag", "target": "Tag", "split": "|" }
   *       ]
   *     }
   *   ],
   *   "edgeSources": [
   *     { "edge": "LinkedTo", "file": "Links.csv", "from": "PostId:Post", "to": "RelatedId:Post",
   *       "properties": { "linkType": "int:TypeId" } }
   *   ],
   *   "limit": 10000
   * }
   * </pre>
   * Property values: {@code "SourceAttr"} (string), {@code "int:SourceAttr"} (integer), {@code "bool:SourceAttr"} (boolean).
   * File format auto-detected from extension (.xml, .csv, .jsonl). XML defaults to attribute-based {@code <row/>};
   * add {@code "element": "book"} to read child elements as fields.
   *
   * @param database the target database (schema must be pre-created)
   * @param json     the JSON configuration string
   * @param baseDir  base directory for resolving relative file paths
   */
  public static GraphImporter fromJSON(final Database database, final String json, final String baseDir) {
    return fromJSON(database, new JSONObject(json), baseDir);
  }

  /** Creates a GraphImporter from a parsed JSON configuration. */
  public static GraphImporter fromJSON(final Database database, final JSONObject config, final String baseDir) {
    final Builder b = builder(database);

    if (config.has("limit"))
      b.limit(config.getLong("limit"));

    // Vertex sources
    if (config.has("vertices")) {
      final JSONArray vertices = config.getJSONArray("vertices");
      for (int i = 0; i < vertices.length(); i++)
        parseVertexSource(b, vertices.getJSONObject(i), baseDir);
    }

    // Edge-only sources
    if (config.has("edgeSources")) {
      final JSONArray edgeSources = config.getJSONArray("edgeSources");
      for (int i = 0; i < edgeSources.length(); i++)
        parseEdgeSource(b, edgeSources.getJSONObject(i), baseDir);
    }

    return b.build();
  }

  private static void parseVertexSource(final Builder b, final JSONObject vj, final String baseDir) {
    final String typeName = vj.getString("type");
    final RecordSource source = createRecordSource(vj, baseDir);

    b.vertex(typeName, source, v -> {
      if (vj.has("id"))
        v.id(vj.getString("id"));
      if (vj.has("nameId"))
        v.idByName(vj.getString("nameId"));
      if (vj.has("filter")) {
        final String[] parts = vj.getString("filter").split("=", 2);
        v.filter(parts[0], parts[1]);
      }
      if (vj.getBoolean("deduplicate", false))
        v.deduplicate(true);

      // Properties: { "dbName": "SourceAttr" } or { "dbName": "int:SourceAttr" }
      if (vj.has("properties")) {
        final JSONObject props = vj.getJSONObject("properties");
        for (final String propName : props.keySet()) {
          final String spec = props.getString(propName);
          parsePropertySpec(v, propName, spec);
        }
      }

      // Edges
      if (vj.has("edges")) {
        final JSONArray edges = vj.getJSONArray("edges");
        for (int j = 0; j < edges.length(); j++) {
          final JSONObject ej = edges.getJSONObject(j);
          final String attr = ej.getString("attribute");
          final String edgeType = ej.getString("edge");
          final String target = ej.getString("target");

          final boolean byName = ej.getBoolean("byName", false);
          if (ej.has("split"))
            v.splitEdge(attr, edgeType, target, ej.getString("split"));
          else if (byName && "in".equals(ej.getString("direction", "out")))
            v.edgeInByName(attr, edgeType, target);
          else if (byName)
            v.edgeOutByName(attr, edgeType, target);
          else if ("in".equals(ej.getString("direction", "out")))
            v.edgeIn(attr, edgeType, target);
          else
            v.edgeOut(attr, edgeType, target);
        }
      }
    });
  }

  private static void parseEdgeSource(final Builder b, final JSONObject ej, final String baseDir) {
    final String edgeType = ej.getString("edge");
    final RecordSource source = createRecordSource(ej, baseDir);

    b.edgeSource(edgeType, source, e -> {
      // "from": "PostId:Post" → attribute:vertexType
      final String[] fromParts = ej.getString("from").split(":");
      e.from(fromParts[0], fromParts[1]);
      final String[] toParts = ej.getString("to").split(":");
      e.to(toParts[0], toParts[1]);

      if (ej.has("properties")) {
        final JSONObject props = ej.getJSONObject("properties");
        for (final String propName : props.keySet()) {
          final String spec = props.getString(propName);
          if (spec.startsWith("int:"))
            e.intProperty(propName, spec.substring(4));
          else if (spec.startsWith("long:"))
            e.longProperty(propName, spec.substring(5));
          else if (spec.startsWith("double:"))
            e.doubleProperty(propName, spec.substring(7));
        }
      }
    });
  }

  private static void parsePropertySpec(final VertexConfig v, final String propName, final String spec) {
    if (spec.startsWith("int:"))
      v.intProperty(propName, spec.substring(4));
    else if (spec.startsWith("long:"))
      v.longProperty(propName, spec.substring(5));
    else if (spec.startsWith("double:"))
      v.doubleProperty(propName, spec.substring(7));
    else if (spec.startsWith("bool:"))
      v.boolProperty(propName, spec.substring(5));
    else if (spec.startsWith("datetime:"))
      parseDatetimeSpec(v, propName, spec.substring(9));
    else
      v.property(propName, spec);
  }

  /**
   * Parses a datetime property spec. Supports two forms:
   * <ul>
   *   <li>{@code "datetime:pickup_time"} - uses the default format {@code yyyy-MM-dd HH:mm:ss}</li>
   *   <li>{@code "datetime:yyyy-MM-dd'T'HH:mm:ss:pickup_time"} - custom format before the last {@code :attr}</li>
   * </ul>
   */
  private static void parseDatetimeSpec(final VertexConfig v, final String propName, final String rest) {
    // If rest contains a ':' it could be format:attribute, but we need to be careful
    // because datetime formats themselves contain colons (e.g., HH:mm:ss).
    // Convention: if the rest contains no format separator, it's just the attribute name.
    // To specify a format, use "datetime:FORMAT|attribute" with pipe as separator.
    final int pipe = rest.indexOf('|');
    if (pipe > 0) {
      final String format = rest.substring(0, pipe);
      final String attribute = rest.substring(pipe + 1);
      v.datetimeProperty(propName, attribute, format);
    } else {
      v.datetimeProperty(propName, rest);
    }
  }

  /** Creates the appropriate RecordSource based on file extension or explicit format. */
  private static RecordSource createRecordSource(final JSONObject config, final String baseDir) {
    final String fileName = config.getString("file");
    final String filePath = new File(baseDir, fileName).getPath();
    final String autoFormat = fileName.endsWith(".csv") ? "csv" : fileName.endsWith(".jsonl") || fileName.endsWith(".ndjson") ? "jsonl" : "xml";
    final String format = config.getString("format", autoFormat);

    switch (format) {
    case "csv":
      final char delimiter = config.getString("delimiter", ",").charAt(0);
      final int skipLines = config.getInt("skipLines", 0);
      return new CsvRowSource(filePath, delimiter, skipLines);
    case "jsonl":
      return new JsonlRowSource(filePath);
    default: // xml
      final String element = config.getString("element", "row");
      final boolean childElements = !"row".equals(element) || config.getBoolean("childElements", false);
      return new XmlRowSource(filePath, element, childElements);
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Builder (programmatic API)
  // ═══════════════════════════════════════════════════════════════════

  public static Builder builder(final Database database) {
    return new Builder(database);
  }

  public static class Builder {
    private final Database              database;
    private final List<VertexSourceDef> vertexSources = new ArrayList<>();
    private final List<EdgeSourceDef>   edgeSources   = new ArrayList<>();
    private       long                  limit;

    Builder(final Database database) {
      this.database = database;
    }

    /**
     * Add a vertex type with its data source and property/edge mappings.
     * Sources are processed in the order they are added — ensure referenced types are added first.
     */
    public Builder vertex(final String typeName, final RecordSource source, final Consumer<VertexConfig> config) {
      final VertexConfig vc = new VertexConfig(typeName);
      config.accept(vc);
      vertexSources.add(new VertexSourceDef(typeName, source, vc));
      return this;
    }

    /**
     * Add an edge-only data source (no vertices created, both endpoints must already exist).
     */
    public Builder edgeSource(final String edgeType, final RecordSource source,
                              final Consumer<EdgeSourceConfig> config) {
      final EdgeSourceConfig ec = new EdgeSourceConfig(edgeType);
      config.accept(ec);
      edgeSources.add(new EdgeSourceDef(edgeType, source, ec));
      return this;
    }

    /**
     * Max records to process per source (0 = unlimited). Useful for testing.
     */
    public Builder limit(final long maxRecords) {
      this.limit = maxRecords;
      return this;
    }

    public GraphImporter build() {
      return new GraphImporter(database, vertexSources, edgeSources, limit);
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Configuration classes
  // ═══════════════════════════════════════════════════════════════════

  public static class VertexConfig {
    final String typeName;
    String  idAttribute;
    String  nameIdAttribute;
    String  filterAttribute;
    String  filterValue;
    boolean deduplicate;
    final List<PropDef> properties = new ArrayList<>();
    final List<EdgeDef> edges      = new ArrayList<>();

    VertexConfig(final String typeName) {
      this.typeName = typeName;
    }

    /**
     * Enable deduplication: only the first row with a given id/nameId is imported as a vertex.
     * Subsequent rows with the same id/nameId are skipped. Useful when extracting a dimension
     * table from a denormalized file (e.g., extracting unique cities from a trips CSV).
     */
    public void deduplicate(final boolean enabled) {
      this.deduplicate = enabled;
    }

    /**
     * Filter rows: only rows where the attribute equals the given value are imported.
     * Enables splitting one file into multiple vertex types (e.g. Posts.xml → Question + Answer).
     * Format: {@code filter("PostTypeId", "1")} or in JSON: {@code "filter": "PostTypeId=1"}.
     */
    public void filter(final String attribute, final String value) {
      this.filterAttribute = attribute;
      this.filterValue = value;
    }

    /**
     * Primary ID attribute (integer-valued, used for edge resolution).
     */
    public void id(final String attribute) {
      this.idAttribute = attribute;
    }

    /**
     * Secondary name-based ID (string, for split-field edge resolution like tags).
     */
    public void idByName(final String attribute) {
      this.nameIdAttribute = attribute;
    }

    /**
     * Map a string property. Null source values are skipped.
     */
    public void property(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.STRING));
    }

    /**
     * Map an integer property. Missing/empty values default to 0.
     */
    public void intProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.INTEGER));
    }

    /**
     * Map a boolean property (matches "True"/"true").
     */
    public void boolProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.BOOLEAN));
    }

    /**
     * Map a long property. Missing/empty values default to 0.
     */
    public void longProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.LONG));
    }

    /**
     * Map a double property. Missing/empty values default to 0.0.
     */
    public void doubleProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.DOUBLE));
    }

    /**
     * Map a datetime property. The value is parsed as {@link LocalDateTime} using the default
     * format {@code yyyy-MM-dd HH:mm:ss} or a custom format if provided.
     */
    public void datetimeProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.DATETIME));
    }

    /**
     * Map a datetime property with a custom format pattern.
     */
    public void datetimeProperty(final String name, final String attribute, final String format) {
      properties.add(new PropDef(name, attribute, PropType.DATETIME, format));
    }

    /**
     * Incoming edge: the foreign key references a vertex that points TO this vertex.
     * Example: Post has OwnerUserId → creates edge User→Post (Posted).
     */
    public void edgeIn(final String fkAttribute, final String edgeType, final String targetType) {
      edges.add(new EdgeDef(fkAttribute, edgeType, targetType, true, false, null));
    }

    /**
     * Outgoing edge: this vertex points TO the referenced vertex.
     * Example: Answer has ParentId → creates edge Answer→Question (AnswerOf).
     */
    public void edgeOut(final String fkAttribute, final String edgeType, final String targetType) {
      edges.add(new EdgeDef(fkAttribute, edgeType, targetType, false, false, null));
    }

    /**
     * Outgoing edge resolved by name: the FK attribute value is matched against the target type's
     * nameId (string-based). Example: Trip has city="Boston" -> creates edge Trip-[InCity]->City.
     */
    public void edgeOutByName(final String fkAttribute, final String edgeType, final String targetType) {
      edges.add(new EdgeDef(fkAttribute, edgeType, targetType, false, false, null, true));
    }

    /**
     * Incoming edge resolved by name.
     */
    public void edgeInByName(final String fkAttribute, final String edgeType, final String targetType) {
      edges.add(new EdgeDef(fkAttribute, edgeType, targetType, true, false, null, true));
    }

    /**
     * Split-field edge: a delimited field (e.g. "|java|python|") creates one edge per value.
     * Values are resolved by name against the target type's nameId.
     */
    public void splitEdge(final String attribute, final String edgeType, final String targetType,
                          final String delimiter) {
      edges.add(new EdgeDef(attribute, edgeType, targetType, false, true, delimiter));
    }
  }

  public static class EdgeSourceConfig {
    final String edgeType;
    String fromAttribute, fromVertexType;
    String toAttribute, toVertexType;
    final List<PropDef> properties = new ArrayList<>();

    EdgeSourceConfig(final String edgeType) {
      this.edgeType = edgeType;
    }

    public void from(final String attribute, final String vertexType) {
      this.fromAttribute = attribute;
      this.fromVertexType = vertexType;
    }

    public void to(final String attribute, final String vertexType) {
      this.toAttribute = attribute;
      this.toVertexType = vertexType;
    }

    public void intProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.INTEGER));
    }

    public void longProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.LONG));
    }

    public void doubleProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.DOUBLE));
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Data source interface
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Pluggable data source that iterates over records.
   */
  public interface RecordSource {
    void forEach(RecordVisitor visitor) throws Exception;
  }

  @FunctionalInterface
  public interface RecordVisitor {
    void visit(RecordReader record) throws Exception;
  }

  /**
   * Read-only access to a record's attributes.
   */
  public interface RecordReader {
    String get(String attribute);

    default int getInt(final String attribute) {
      final String v = get(attribute);
      return v != null && !v.isEmpty() ? Integer.parseInt(v) : 0;
    }

    default long getLong(final String attribute) {
      final String v = get(attribute);
      return v != null && !v.isEmpty() ? Long.parseLong(v) : 0L;
    }

    default double getDouble(final String attribute) {
      final String v = get(attribute);
      return v != null && !v.isEmpty() ? Double.parseDouble(v) : 0.0;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Execution
  // ═══════════════════════════════════════════════════════════════════

  public void run() throws Exception {
    final long start = System.currentTimeMillis();

    // ── Pass 1: Create vertices + collect topology ──
    LogManager.instance().log(this, Level.INFO, "Pass 1: Vertices + Topology");

    try (final GraphBatch batch = database.batch()
        .withBidirectional(false)
        .withWAL(false)
        .withPreAllocateEdgeChunks(true)
        .withCommitEvery(0)
        .build()) {

      for (final VertexSourceDef vsd : vertexSources)
        processVertexSource(batch, vsd);
    }

    // Process edge-only sources
    for (final EdgeSourceDef esd : edgeSources)
      processEdgeSource(esd);

    // Free ID maps (edges now use internal indices)
    for (final TypeState ts : typeStates.values()) {
      ts.idToIdx = null;
      ts.nameToIdx = null;
    }

    LogManager.instance().log(this, Level.INFO, "  Topology: %,d vertices, %,d edge refs", totalVertices,
        countEdgeRefs());

    // ── Pass 2: Create edges from topology ──
    LogManager.instance().log(this, Level.INFO, "Pass 2: Edges (bidirectional)");

    for (final EdgeCollector ec : edgeCollectors.values())
      flushEdgeType(ec);

    final long elapsed = System.currentTimeMillis() - start;
    LogManager.instance().log(this, Level.INFO, "Import complete: %,d vertices, %,d edges in %d.%ds",
        totalVertices, totalEdges, elapsed / 1000, (elapsed % 1000) / 100);
  }

  public long getVertexCount() {
    return totalVertices;
  }

  public long getEdgeCount() {
    return totalEdges;
  }

  @Override
  public void close() {
    typeStates.clear();
    edgeCollectors.clear();
    deferredEdges.clear();
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Pass 1: Process vertex sources
  // ═══════════════════════════════════════════════════════════════════

  private void processVertexSource(final GraphBatch batch, final VertexSourceDef vsd) throws Exception {
    final long t = System.currentTimeMillis();
    final VertexConfig vc = vsd.config;
    final TypeState ts = new TypeState();
    typeStates.put(vc.typeName, ts);

    final IntList bk = new IntList(100_000);
    final LongList ps = new LongList(100_000);
    final List<Object> propBuf = new ArrayList<>(vc.properties.size() * 2 + 4);

    // Prepare edge collectors for this source's edge definitions
    final List<EdgeDef> resolvedEdges = new ArrayList<>();
    final List<EdgeDef> deferredEdgeDefs = new ArrayList<>();
    for (final EdgeDef ed : vc.edges) {
      // For incoming edges the actual source vertex is the target type
      if (ed.incoming)
        getOrCreateEdgeCollector(ed.edgeType, ed.targetType, vc.typeName);
      else
        getOrCreateEdgeCollector(ed.edgeType, vc.typeName, ed.targetType);
      if (ed.targetType.equals(vc.typeName) && !ed.isSplit)
        deferredEdgeDefs.add(ed);
      else
        resolvedEdges.add(ed);
    }

    // Deferred raw soId pairs for self-referencing edges
    final Map<String, IntList[]> deferredRaw = new HashMap<>();
    for (final EdgeDef ed : deferredEdgeDefs)
      deferredRaw.put(ed.edgeType, new IntList[]{new IntList(100_000), new IntList(100_000)});

    final int[] count = {0};
    database.begin();

    final String filterAttr = vc.filterAttribute;
    final String filterVal = vc.filterValue;

    vsd.source.forEach(record -> {
      if (limit > 0 && count[0] >= limit)
        return;

      // Apply row filter (e.g. PostTypeId=1 for questions only)
      if (filterAttr != null) {
        final String v = record.get(filterAttr);
        if (v == null || !v.equals(filterVal))
          return;
      }

      // Deduplication: skip if this id/nameId was already imported
      if (vc.deduplicate) {
        if (vc.idAttribute != null) {
          final int id = record.getInt(vc.idAttribute);
          if (ts.idToIdx.get(id, -1) >= 0)
            return;
        }
        if (vc.nameIdAttribute != null) {
          final String name = record.get(vc.nameIdAttribute);
          if (name != null && ts.nameToIdx.containsKey(name))
            return;
        }
      }

      // Register ID
      final int idx = count[0];
      if (vc.idAttribute != null)
        ts.idToIdx.put(record.getInt(vc.idAttribute), idx);
      if (vc.nameIdAttribute != null) {
        final String name = record.get(vc.nameIdAttribute);
        if (name != null)
          ts.nameToIdx.put(name, idx);
      }

      // Build vertex properties
      propBuf.clear();
      for (final PropDef pd : vc.properties) {
        final Object val = readProperty(record, pd);
        if (val != null) {
          propBuf.add(pd.name);
          propBuf.add(val);
        }
      }

      final MutableVertex v = batch.createVertex(vc.typeName, propBuf.toArray());
      bk.add(v.getIdentity().getBucketId());
      ps.add(v.getIdentity().getPosition());

      // Collect edges
      for (final EdgeDef ed : resolvedEdges)
        collectEdge(record, ed, vc.typeName, idx);

      // Collect deferred (self-referencing) edges as raw soIds
      for (final EdgeDef ed : deferredEdgeDefs) {
        final int fk = record.getInt(ed.fkAttribute);
        if (fk != 0) {
          final int thisSoId = record.getInt(vc.idAttribute);
          final IntList[] pair = deferredRaw.get(ed.edgeType);
          pair[0].add(thisSoId);
          pair[1].add(fk);
        }
      }

      count[0]++;
      if (count[0] % 50_000 == 0) {
        database.commit();
        database.begin();
      }
    });
    database.commit();

    ts.buckets = bk.trim();
    ts.positions = ps.trim();
    ts.count = count[0];
    totalVertices += ts.count;

    // Resolve deferred self-referencing edges (srcType == dstType == thisType)
    for (final Map.Entry<String, IntList[]> entry : deferredRaw.entrySet()) {
      final IntList[] pair = entry.getValue();
      final EdgeCollector ec = edgeCollectors.get(entry.getKey() + "|" + vc.typeName + "|" + vc.typeName);
      for (int i = 0; i < pair[0].size; i++) {
        final int si = ts.idToIdx.get(pair[0].data[i], -1);
        final int di = ts.idToIdx.get(pair[1].data[i], -1);
        if (si >= 0 && di >= 0) {
          ec.srcIdx.add(si);
          ec.dstIdx.add(di);
        }
      }
    }

    LogManager.instance().log(this, Level.INFO, "  %-12s %,d vertices (%,d ms)", vc.typeName, ts.count,
        System.currentTimeMillis() - t);
  }

  private void collectEdge(final RecordReader record, final EdgeDef ed,
                           final String thisType, final int thisIdx) {
    // Lookup by composite key matching how the collector was created
    final String srcType = ed.incoming ? ed.targetType : thisType;
    final String dstType = ed.incoming ? thisType : ed.targetType;
    final EdgeCollector ec = edgeCollectors.get(ed.edgeType + "|" + srcType + "|" + dstType);

    if (ed.isSplit) {
      // Split field: e.g. "|java|python|css|"
      final String fieldVal = record.get(ed.fkAttribute);
      if (fieldVal == null || fieldVal.length() <= 1)
        return;
      final TypeState targetTs = typeStates.get(ed.targetType);
      if (targetTs == null)
        return;
      int start = fieldVal.charAt(0) == ed.delimiter.charAt(0) ? 1 : 0;
      final char delim = ed.delimiter.charAt(0);
      int pos;
      while ((pos = fieldVal.indexOf(delim, start)) != -1) {
        if (pos > start) {
          final Integer ti = targetTs.nameToIdx.get(fieldVal.substring(start, pos));
          if (ti != null) {
            ec.srcIdx.add(thisIdx);
            ec.dstIdx.add(ti);
          }
        }
        start = pos + 1;
      }
    } else if (ed.byName) {
      final String name = record.get(ed.fkAttribute);
      if (name == null)
        return;
      final TypeState targetTs = typeStates.get(ed.targetType);
      if (targetTs == null)
        return;
      final Integer targetIdx = targetTs.nameToIdx.get(name);
      if (targetIdx == null)
        return;

      if (ed.incoming) {
        ec.srcIdx.add(targetIdx);
        ec.dstIdx.add(thisIdx);
      } else {
        ec.srcIdx.add(thisIdx);
        ec.dstIdx.add(targetIdx);
      }
    } else {
      final int fk = record.getInt(ed.fkAttribute);
      if (fk == 0)
        return;
      final TypeState targetTs = typeStates.get(ed.targetType);
      if (targetTs == null)
        return;
      final int targetIdx = targetTs.idToIdx.get(fk, -1);
      if (targetIdx < 0)
        return;

      if (ed.incoming) {
        ec.srcIdx.add(targetIdx);
        ec.dstIdx.add(thisIdx);
      } else {
        ec.srcIdx.add(thisIdx);
        ec.dstIdx.add(targetIdx);
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Pass 1: Process edge-only sources
  // ═══════════════════════════════════════════════════════════════════

  private void processEdgeSource(final EdgeSourceDef esd) throws Exception {
    final long t = System.currentTimeMillis();
    final EdgeSourceConfig cfg = esd.config;
    final TypeState fromTs = typeStates.get(cfg.fromVertexType);
    final TypeState toTs = typeStates.get(cfg.toVertexType);
    if (fromTs == null || toTs == null)
      return;

    final EdgeCollector ec = getOrCreateEdgeCollector(cfg.edgeType, cfg.fromVertexType, cfg.toVertexType);
    final int[] count = {0};

    esd.source.forEach(record -> {
      if (limit > 0 && count[0] >= limit)
        return;
      final int si = fromTs.idToIdx.get(record.getInt(cfg.fromAttribute), -1);
      final int di = toTs.idToIdx.get(record.getInt(cfg.toAttribute), -1);
      if (si >= 0 && di >= 0) {
        ec.srcIdx.add(si);
        ec.dstIdx.add(di);
        for (final PropDef pd : cfg.properties) {
          switch (pd.type) {
          case LONG:
            if (ec.longProps == null)
              ec.longProps = new HashMap<>();
            ec.longProps.computeIfAbsent(pd.name, k -> new ArrayList<>(100_000)).add(record.getLong(pd.attribute));
            break;
          case DOUBLE:
            if (ec.doubleProps == null)
              ec.doubleProps = new HashMap<>();
            ec.doubleProps.computeIfAbsent(pd.name, k -> new DoubleList(100_000)).add(record.getDouble(pd.attribute));
            break;
          default:
            if (ec.intProps == null)
              ec.intProps = new HashMap<>();
            ec.intProps.computeIfAbsent(pd.name, k -> new IntList(100_000)).add(record.getInt(pd.attribute));
            break;
          }
        }
      }
      count[0]++;
    });

    LogManager.instance().log(this, Level.INFO, "  %-12s %,d edges (%,d ms)",
        cfg.edgeType, ec.srcIdx.size, System.currentTimeMillis() - t);
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Pass 2: Create edges from topology
  // ═══════════════════════════════════════════════════════════════════

  private void flushEdgeType(final EdgeCollector ec) {
    if (ec.srcIdx.size == 0)
      return;
    final long t = System.currentTimeMillis();
    final String edgeType = ec.edgeTypeName;
    final TypeState srcTs = typeStates.get(ec.srcType);
    final TypeState dstTs = typeStates.get(ec.dstType);

    try (final GraphBatch batch = database.batch()
        .withBatchSize(500_000)
        .withBidirectional(true)
        .withWAL(false)
        .withParallelFlush(false)
        .withCommitEvery(50_000)
        .build()) {

      final int[] sSrc = ec.srcIdx.trim();
      final int[] sDst = ec.dstIdx.trim();
      final boolean hasProps = ec.hasProperties();

      for (int i = 0; i < sSrc.length; i++) {
        final RID src = new RID(srcTs.buckets[sSrc[i]], srcTs.positions[sSrc[i]]);
        final RID dst = new RID(dstTs.buckets[sDst[i]], dstTs.positions[sDst[i]]);
        if (hasProps) {
          final List<Object> props = new ArrayList<>();
          ec.appendProperties(props, i);
          batch.newEdge(src, edgeType, dst, props.toArray());
        } else {
          batch.newEdge(src, edgeType, dst);
        }
      }
    }
    totalEdges += ec.srcIdx.size;
    LogManager.instance().log(this, Level.INFO, "  %-12s %,8d (%s→%s, %,d ms)",
        edgeType, ec.srcIdx.size, ec.srcType, ec.dstType, System.currentTimeMillis() - t);
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Internal helpers
  // ═══════════════════════════════════════════════════════════════════

  private EdgeCollector getOrCreateEdgeCollector(final String edgeType, final String srcType, final String dstType) {
    // Key by (edgeType, srcType, dstType) — same edge type can connect different vertex type pairs
    // (e.g. POSTED: User→Question and POSTED: User→Answer)
    final String key = edgeType + "|" + srcType + "|" + dstType;
    return edgeCollectors.computeIfAbsent(key, k -> new EdgeCollector(edgeType, srcType, dstType));
  }

  private static final DateTimeFormatter DEFAULT_DATETIME_FMT = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
      .toFormatter();

  private static Object readProperty(final RecordReader record, final PropDef pd) {
    switch (pd.type) {
    case INTEGER:
      return record.getInt(pd.attribute);
    case LONG:
      return record.getLong(pd.attribute);
    case DOUBLE:
      return record.getDouble(pd.attribute);
    case BOOLEAN:
      return "True".equalsIgnoreCase(record.get(pd.attribute));
    case DATETIME: {
      final String v = record.get(pd.attribute);
      if (v == null)
        return null;
      final DateTimeFormatter fmt = pd.datetimeFormat != null
          ? DateTimeFormatter.ofPattern(pd.datetimeFormat)
          : DEFAULT_DATETIME_FMT;
      return LocalDateTime.parse(v, fmt);
    }
    default:
      return record.get(pd.attribute);
    }
  }

  private long countEdgeRefs() {
    long n = 0;
    for (final EdgeCollector ec : edgeCollectors.values())
      n += ec.srcIdx.size;
    return n;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Internal data structures
  // ═══════════════════════════════════════════════════════════════════

  enum PropType {STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATETIME}

  static class PropDef {
    final String   name, attribute;
    final PropType type;
    final String   datetimeFormat;

    PropDef(final String name, final String attr, final PropType type) {
      this(name, attr, type, null);
    }

    PropDef(final String name, final String attr, final PropType type, final String datetimeFormat) {
      this.name = name;
      this.attribute = attr;
      this.type = type;
      this.datetimeFormat = datetimeFormat;
    }
  }

  static class EdgeDef {
    final String  fkAttribute, edgeType, targetType;
    final boolean incoming, isSplit, byName;
    final String  delimiter;

    EdgeDef(final String fk, final String et, final String tt, final boolean in, final boolean split,
            final String delim) {
      this(fk, et, tt, in, split, delim, false);
    }

    EdgeDef(final String fk, final String et, final String tt, final boolean in, final boolean split,
            final String delim, final boolean byName) {
      this.fkAttribute = fk;
      this.edgeType = et;
      this.targetType = tt;
      this.incoming = in;
      this.isSplit = split;
      this.delimiter = delim;
      this.byName = byName;
    }
  }

  static class VertexSourceDef {
    final String       typeName;
    final RecordSource source;
    final VertexConfig config;

    VertexSourceDef(final String tn, final RecordSource s, final VertexConfig c) {
      this.typeName = tn;
      this.source = s;
      this.config = c;
    }
  }

  static class EdgeSourceDef {
    final String           edgeType;
    final RecordSource     source;
    final EdgeSourceConfig config;

    EdgeSourceDef(final String et, final RecordSource s, final EdgeSourceConfig c) {
      this.edgeType = et;
      this.source = s;
      this.config = c;
    }
  }

  static class TypeState {
    IntIntMap            idToIdx   = new IntIntMap(100_000);
    Map<String, Integer> nameToIdx = new HashMap<>();
    int[]                buckets;
    long[]               positions;
    int                  count;
  }

  static class EdgeCollector {
    final String edgeTypeName, srcType, dstType;
    final IntList srcIdx = new IntList(100_000);
    final IntList dstIdx = new IntList(100_000);
    Map<String, IntList>    intProps;
    Map<String, List<Long>> longProps;
    Map<String, DoubleList> doubleProps;

    EdgeCollector(final String edgeTypeName, final String src, final String dst) {
      this.edgeTypeName = edgeTypeName;
      this.srcType = src;
      this.dstType = dst;
    }

    boolean hasProperties() {
      return (intProps != null && !intProps.isEmpty())
          || (longProps != null && !longProps.isEmpty())
          || (doubleProps != null && !doubleProps.isEmpty());
    }

    void appendProperties(final List<Object> props, final int i) {
      if (intProps != null)
        for (final Map.Entry<String, IntList> pe : intProps.entrySet()) {
          props.add(pe.getKey());
          props.add(pe.getValue().data[i]);
        }
      if (longProps != null)
        for (final Map.Entry<String, List<Long>> pe : longProps.entrySet()) {
          props.add(pe.getKey());
          props.add(pe.getValue().get(i));
        }
      if (doubleProps != null)
        for (final Map.Entry<String, DoubleList> pe : doubleProps.entrySet()) {
          props.add(pe.getKey());
          props.add(pe.getValue().data[i]);
        }
    }
  }

  static class DeferredEdgePair {
    final int srcSoId, dstSoId;

    DeferredEdgePair(final int s, final int d) {
      this.srcSoId = s;
      this.dstSoId = d;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Primitive collections (zero boxing, minimal GC)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Open-addressing int→int hash map with Fibonacci hashing.
   */
  static final class IntIntMap {
    private static final int   EMPTY = Integer.MIN_VALUE;
    private              int[] keys, values;
    private int mask, size, threshold;

    IntIntMap(final int expected) {
      int cap = Integer.highestOneBit(Math.max(16, (int) (expected / 0.7))) << 1;
      keys = new int[cap];
      values = new int[cap];
      mask = cap - 1;
      threshold = (int) (cap * 0.7);
      Arrays.fill(keys, EMPTY);
    }

    void put(final int key, final int value) {
      if (size >= threshold)
        resize();
      int i = (key * 0x9E3779B9) & mask;
      while (keys[i] != EMPTY && keys[i] != key)
        i = (i + 1) & mask;
      if (keys[i] == EMPTY)
        size++;
      keys[i] = key;
      values[i] = value;
    }

    int get(final int key, final int def) {
      int i = (key * 0x9E3779B9) & mask;
      while (keys[i] != EMPTY) {
        if (keys[i] == key)
          return values[i];
        i = (i + 1) & mask;
      }
      return def;
    }

    private void resize() {
      final int newCap = keys.length << 1;
      final int[] ok = keys, ov = values;
      keys = new int[newCap];
      values = new int[newCap];
      mask = newCap - 1;
      threshold = (int) (newCap * 0.7);
      Arrays.fill(keys, EMPTY);
      for (int i = 0; i < ok.length; i++)
        if (ok[i] != EMPTY) {
          int j = (ok[i] * 0x9E3779B9) & mask;
          while (keys[j] != EMPTY)
            j = (j + 1) & mask;
          keys[j] = ok[i];
          values[j] = ov[i];
        }
    }
  }

  /**
   * Growable int array.
   */
  static final class IntList {
    int[] data;
    int   size;

    IntList(final int cap) {
      data = new int[cap];
    }

    void add(final int v) {
      if (size == data.length)
        data = Arrays.copyOf(data, size * 2);
      data[size++] = v;
    }

    int[] trim() {
      return size == data.length ? data : Arrays.copyOf(data, size);
    }
  }

  /**
   * Growable double array.
   */
  static final class DoubleList {
    double[] data;
    int      size;

    DoubleList(final int cap) {
      data = new double[cap];
    }

    void add(final double v) {
      if (size == data.length)
        data = Arrays.copyOf(data, size * 2);
      data[size++] = v;
    }
  }

  /**
   * Growable long array.
   */
  static final class LongList {
    long[] data;
    int    size;

    LongList(final int cap) {
      data = new long[cap];
    }

    void add(final long v) {
      if (size == data.length)
        data = Arrays.copyOf(data, size * 2);
      data[size++] = v;
    }

    long[] trim() {
      return size == data.length ? data : Arrays.copyOf(data, size);
    }
  }
}
