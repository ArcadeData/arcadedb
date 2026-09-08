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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  /**
   * A key that is not the canonical decimal text of a {@code long}, and the empty slot marker of
   * {@link LongIntMap}. {@code Long.MIN_VALUE} therefore never reaches the primitive maps: a key
   * spelled {@code "-9223372036854775808"} is kept with the textual keys, which resolve it the
   * same way, only boxed.
   *
   * @see #canonicalLong(String)
   */
  static final long NOT_CANONICAL_LONG = Long.MIN_VALUE;

  private final Database                            database;
  private final List<VertexSourceDef>               vertexSources;
  private final List<EdgeSourceDef>                 edgeSources;
  private final long                                limit;
  private final Map<String, TypeState>              typeStates     = new LinkedHashMap<>();
  private final Map<String, EdgeCollector>          edgeCollectors = new LinkedHashMap<>();

  private long totalVertices;
  private long totalEdges;
  private long unresolvedEdges;

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

    final String json = new String(Files.readAllBytes(jsonFile.toPath()));
    final JSONObject config = new JSONObject(json);

    FileUtils.deleteRecursively(new File(dbPath));
    final Database database = new DatabaseFactory(dbPath).create();

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
        gav.awaitReady(10, TimeUnit.MINUTES);
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
   * Property values: {@code "SourceAttr"} (string), {@code "int:SourceAttr"} (integer), {@code "bool:SourceAttr"} (boolean),
   * {@code "long:"}, {@code "double:"}, {@code "datetime:"}, {@code "vector:SourceAttr"} (dense {@code float[]} embedding)
   * and {@code "list:SourceAttr"} (generic list). The same prefixes apply to a vertex's {@code "properties"} and to an
   * {@code "edgeSources"} entry's {@code "properties"}.
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
        for (final String propName : props.keySet())
          parsePropertySpec(e, propName, props.getString(propName));
      }
    });
  }

  private static void parsePropertySpec(final PropertyConfig v, final String propName, final String spec) {
    if (spec.startsWith("int:"))
      v.intProperty(propName, spec.substring(4));
    else if (spec.startsWith("long:"))
      v.longProperty(propName, spec.substring(5));
    else if (spec.startsWith("double:"))
      v.doubleProperty(propName, spec.substring(7));
    else if (spec.startsWith("bool:"))
      v.boolProperty(propName, spec.substring(5));
    else if (spec.startsWith("vector:"))
      v.floatArrayProperty(propName, spec.substring(7));
    else if (spec.startsWith("list:"))
      v.listProperty(propName, spec.substring(5));
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
  private static void parseDatetimeSpec(final PropertyConfig v, final String propName, final String rest) {
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

  /**
   * Property mappings shared by vertex and edge sources, so a property spec means the same thing
   * wherever it is declared.
   */
  public abstract static class PropertyConfig {
    final List<PropDef> properties = new ArrayList<>();

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
     * Map a dense float vector property (an embedding), stored as {@code float[]}. JSONL sources
     * read the JSON array natively; the other sources parse the textual form
     * {@code "[0.1,0.2,0.3]"}. See {@link RecordReader#getFloatArray(String)}.
     * <p>
     * On a CSV source pick a delimiter the array does not contain ({@code ';'} rather than the
     * default {@code ','}): {@link CsvRowSource} splits on the delimiter with no quoting, so a
     * comma-delimited file would cut an unquoted array across several fields.
     */
    public void floatArrayProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.FLOAT_ARRAY));
    }

    /**
     * Map a generic list property (e.g. an array of tags). Use
     * {@link #floatArrayProperty(String, String)} for numeric vectors: it avoids boxing every
     * element and is what a vector index consumes without conversion. The CSV delimiter caveat on
     * {@link #floatArrayProperty(String, String)} applies here too.
     */
    public void listProperty(final String name, final String attribute) {
      properties.add(new PropDef(name, attribute, PropType.LIST));
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
  }

  public static class VertexConfig extends PropertyConfig {
    final String typeName;
    String  idAttribute;
    String  nameIdAttribute;
    String  filterAttribute;
    String  filterValue;
    boolean deduplicate;
    final List<EdgeDef> edges = new ArrayList<>();

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
     * Primary ID attribute, used to resolve edges. The key is the attribute's text exactly as the
     * source wrote it, so an integer, a value wider than an {@code int} and a string such as
     * {@code "W13696992"} all work, and two spellings the source kept apart (e.g. {@code "007"}
     * and {@code "7"}) stay two identities. An empty or absent value registers no key.
     */
    public void id(final String attribute) {
      this.idAttribute = attribute;
    }

    /**
     * Secondary ID, matched by {@code edgeOutByName}/{@code edgeInByName} and by
     * {@code splitEdge}. Declare it when a type is referenced through two different keys - a
     * numeric id from one file and a name from another; a single string key needs nothing more
     * than {@link #id(String)}.
     */
    public void idByName(final String attribute) {
      this.nameIdAttribute = attribute;
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
     * Split-field edge: a delimited field (e.g. {@code "|java|python|"}) creates one edge per
     * value, resolved by name against the target type's nameId. The wrapping delimiters are
     * optional on either end, so {@code "java|python"} yields the same two values.
     */
    public void splitEdge(final String attribute, final String edgeType, final String targetType,
                          final String delimiter) {
      edges.add(new EdgeDef(attribute, edgeType, targetType, false, true, delimiter));
    }
  }

  public static class EdgeSourceConfig extends PropertyConfig {
    final String edgeType;
    String fromAttribute, fromVertexType;
    String toAttribute, toVertexType;

    EdgeSourceConfig(final String edgeType) {
      this.edgeType = edgeType;
    }

    /**
     * Source endpoint: the attribute holding the key of a {@code vertexType} vertex, matched
     * against that type's {@link VertexConfig#id(String)} attribute by its text, whatever its type.
     */
    public void from(final String attribute, final String vertexType) {
      this.fromAttribute = attribute;
      this.fromVertexType = vertexType;
    }

    /**
     * Destination endpoint. See {@link #from(String, String)}.
     */
    public void to(final String attribute, final String vertexType) {
      this.toAttribute = attribute;
      this.toVertexType = vertexType;
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

    /**
     * Reads an attribute as a dense {@code float} vector (an embedding). Returns {@code null} when
     * the attribute is missing or empty, so the property is simply not set on the record.
     * <p>
     * The default implementation parses the textual form ({@code "[0.1,0.2,0.3]"}), which is how
     * flat formats such as CSV and XML carry a vector. Sources with a native array representation
     * (JSONL) override this to build the {@code float[]} without going through a string.
     * <p>
     * {@code float[]} rather than {@code List<Double>} is deliberate: a 768-dimension embedding
     * costs ~3KB as {@code float[]} against ~18KB as boxed doubles, and it is the exact
     * representation {@link VectorUtils#toFloatArray(Object)} hands to a
     * vector index, so indexing the imported property converts nothing.
     */
    default float[] getFloatArray(final String attribute) {
      final String v = get(attribute);
      if (v == null || v.isEmpty())
        return null;
      checkNotSplit(attribute, v);
      return VectorUtils.toFloatArray(v);
    }

    /**
     * Reads an attribute as a generic list (e.g. an array of tags). Returns {@code null} when the
     * attribute is missing or empty. The default implementation parses the textual JSON array form;
     * sources with a native array representation override it.
     */
    default List<Object> getList(final String attribute) {
      final String v = get(attribute);
      if (v == null || v.isEmpty())
        return null;
      checkNotSplit(attribute, v);
      return new JSONArray(v).toList();
    }

    /**
     * A textual array that opens but never closes was almost certainly cut in half by the source's
     * own field separator: {@link CsvRowSource} splits on the delimiter with no quoting, so an
     * unquoted {@code [0.1,0.2,0.3]} in a comma-delimited file arrives as {@code "[0.1"}. Parsing
     * that fragment fails with "does not hold a numeric array", which is true but points at the
     * data rather than at the delimiter, so say which one it is.
     */
    private static void checkNotSplit(final String attribute, final String value) {
      final String trimmed = value.trim();
      final boolean opens = trimmed.startsWith("[");
      if (opens == trimmed.endsWith("]"))
        return;
      throw new IllegalArgumentException("Attribute '" + attribute + "' holds "
          + (opens ? "the start of an array that never closes" : "the end of an array that never opened") + " ("
          + trimmed + "). A delimited source splits an unquoted array across fields when the delimiter also separates "
          + "the array's elements: use a delimiter the values do not contain, such as ';'");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Execution
  // ═══════════════════════════════════════════════════════════════════

  public void run() throws Exception {
    final long start = System.currentTimeMillis();

    validateEdgeTargets();

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
    for (int i = 0; i < edgeSources.size(); i++)
      processEdgeSource(edgeSources.get(i), i);

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
    if (unresolvedEdges > 0)
      LogManager.instance().log(this, Level.WARNING,
          "%,d edges named an identity no vertex carries and were skipped: check that the referenced rows "
              + "are not filtered out and that both files spell the identity the same way", unresolvedEdges);
  }

  /**
   * Every edge names the vertex type at its other end, and an edge whose target type is not there
   * to resolve against contributes nothing at all - it used to do so silently, leaving a smaller
   * graph and no diagnostic, which is the same failure a mistyped type name produces. Nothing has
   * been written when this runs, so the name is reported as the configuration mistake it is.
   * <p>
   * A vertex source resolves a reference against the types imported so far, so naming another
   * vertex source only works when that source is declared first; a self-reference is resolved once
   * the source has been read to the end and needs no such ordering. A standalone edge source runs
   * after every vertex source and can name any of them.
   * <p>
   * Passing this is what lets {@link #collectEdge} and {@link #processEdgeSource} read a
   * {@link TypeState} without testing it for null: every type an edge names has one by the time
   * they run, and a name that could not is an error here rather than an edge quietly dropped.
   */
  private void validateEdgeTargets() {
    // One source per vertex type: processVertexSource() replaces the type's TypeState rather than
    // appending to it, so a second source for a name already taken leaves pass 2 resolving edges
    // collected against the first source's row indices into the second source's row arrays - an
    // out-of-bounds read, or worse an edge silently pointing at an unrelated vertex. Splitting one
    // type across files is a plausible thing to try, so it is refused rather than left to corrupt
    final Set<String> allTypes = new HashSet<>(vertexSources.size());
    final Set<String> typesWithId = new HashSet<>(vertexSources.size());
    final Set<String> typesWithNameId = new HashSet<>(vertexSources.size());
    for (final VertexSourceDef vsd : vertexSources) {
      if (!allTypes.add(vsd.typeName))
        throw new IllegalArgumentException("Vertex type '" + vsd.typeName + "' is declared by more than one "
            + "vertex source, which is not supported: one source imports a type. Give the sources distinct "
            + "type names, or read the files through a single source");
      if (vsd.config.idAttribute != null)
        typesWithId.add(vsd.typeName);
      if (vsd.config.nameIdAttribute != null)
        typesWithNameId.add(vsd.typeName);
    }

    final Set<String> importedSoFar = new HashSet<>(vertexSources.size());
    for (final VertexSourceDef vsd : vertexSources) {
      for (final EdgeDef ed : vsd.config.edges) {
        if (!ed.targetType.equals(vsd.typeName) && !importedSoFar.contains(ed.targetType))
          throw new IllegalArgumentException("Edge '" + ed.edgeType + "' declared on vertex source '"
              + vsd.typeName + "' targets vertex type '" + ed.targetType + "', which "
              + (allTypes.contains(ed.targetType) ?
              "is imported after it: declare the vertex source of '" + ed.targetType + "' before '" + vsd.typeName
                  + "', because a vertex source resolves references against the types already imported" :
              "no vertex source imports. Declared vertex types: " + allTypes));

        final boolean resolvesByName = ed.byName || ed.isSplit;
        if (!(resolvesByName ? typesWithNameId : typesWithId).contains(ed.targetType))
          throw new IllegalArgumentException("Edge '" + ed.edgeType + "' declared on vertex source '"
              + vsd.typeName + "' resolves against the " + (resolvesByName ? "idByName()" : "id()")
              + " of vertex type '" + ed.targetType + "', which declares none");
      }
      importedSoFar.add(vsd.typeName);
    }

    for (final EdgeSourceDef esd : edgeSources) {
      checkEdgeSourceEndpoint(esd, esd.config.fromVertexType, "from", allTypes, typesWithId);
      checkEdgeSourceEndpoint(esd, esd.config.toVertexType, "to", allTypes, typesWithId);
    }
  }

  /**
   * A standalone edge source resolves both endpoints through {@link VertexConfig#id(String)} - a
   * type that declares only {@code idByName()} would match nothing, row after row. Since the
   * unified index takes a string key, such a type wants {@code id()} on that same attribute;
   * {@code idByName()} is for a type referenced through two different keys.
   */
  private static void checkEdgeSourceEndpoint(final EdgeSourceDef esd, final String vertexType,
                                              final String endpoint, final Set<String> allTypes,
                                              final Set<String> typesWithId) {
    if (vertexType == null)
      throw new IllegalArgumentException("Edge source '" + esd.edgeType + "' declares no '" + endpoint
          + "' endpoint: call " + endpoint + "(attribute, vertexType) on it");
    if (!allTypes.contains(vertexType))
      throw new IllegalArgumentException("Edge source '" + esd.edgeType + "' resolves its '" + endpoint
          + "' endpoint against vertex type '" + vertexType + "', which no vertex source imports. "
          + "Declared vertex types: " + allTypes);
    if (!typesWithId.contains(vertexType))
      throw new IllegalArgumentException("Edge source '" + esd.edgeType + "' resolves its '" + endpoint
          + "' endpoint against the id() of vertex type '" + vertexType + "', which declares none: an edge "
          + "source matches id(), not idByName(), and id() takes a string key just as well");
  }

  public long getVertexCount() {
    return totalVertices;
  }

  public long getEdgeCount() {
    return totalEdges;
  }

  /**
   * Edges that could not be created because an endpoint named a key no vertex of the referenced
   * type carries. One per edge, not per endpoint: a row of an edge source whose {@code from} and
   * {@code to} both fail to resolve is one edge lost, and counts once - while a split field, where
   * every value is an edge of its own, counts once per value that resolved to nothing. The edge is
   * skipped - there is nothing to attach it to - but the count is what tells a caller that the
   * graph it got is smaller than the file it handed over, rather than leaving the import to look
   * complete.
   */
  public long getUnresolvedEdgeCount() {
    return unresolvedEdges;
  }

  @Override
  public void close() {
    typeStates.clear();
    edgeCollectors.clear();
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
      // A self-referencing edge can point at a row further down the same file, so its target key
      // is resolved after the pass. That includes a split field: resolving it inline would silently
      // keep only the references that happen to point backwards
      if (ed.targetType.equals(vc.typeName))
        deferredEdgeDefs.add(ed);
      else
        resolvedEdges.add(ed);
    }

    // One buffer of unresolved target keys per deferred definition, positionally aligned with
    // deferredEdgeDefs: two definitions of the same edge type must not share a buffer
    final List<DeferredSelfEdges> deferredSelf = new ArrayList<>(deferredEdgeDefs.size());
    for (int i = 0; i < deferredEdgeDefs.size(); i++)
      deferredSelf.add(new DeferredSelfEdges());

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

      // The raw text is the key: an identity is whatever the source wrote, so reading it as an int
      // would reject a string key and truncate one wider than an int. Read once - deduplication
      // looks the same key up that registration then stores
      final String id = vc.idAttribute != null ? identity(record, vc.idAttribute) : null;
      final String nameId = vc.nameIdAttribute != null ? identity(record, vc.nameIdAttribute) : null;

      // Deduplication: skip if this id/nameId was already imported
      if (vc.deduplicate && (ts.idToIdx.get(id) >= 0 || ts.nameToIdx.get(nameId) >= 0))
        return;

      final int idx = count[0];
      ts.idToIdx.put(id, idx);
      ts.nameToIdx.put(nameId, idx);

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

      // Collect deferred (self-referencing) edges: this row's index is already final, only the
      // target key has to wait for the rest of the file
      for (int i = 0; i < deferredEdgeDefs.size(); i++) {
        final EdgeDef ed = deferredEdgeDefs.get(i);
        final String fieldVal = ed.isSplit ? record.get(ed.fkAttribute) : identity(record, ed.fkAttribute);
        if (fieldVal == null)
          continue;
        final DeferredSelfEdges deferred = deferredSelf.get(i);
        if (ed.isSplit)
          collectSplitKeys(fieldVal, ed.delimiter.charAt(0), deferred, idx);
        else {
          deferred.srcIdx.add(idx);
          deferred.targetKeys.add(fieldVal);
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
    for (int i = 0; i < deferredEdgeDefs.size(); i++) {
      final EdgeDef ed = deferredEdgeDefs.get(i);
      final EdgeCollector ec = edgeCollectors.get(ed.edgeType + "|" + vc.typeName + "|" + vc.typeName);
      final IdIndex index = ed.byName || ed.isSplit ? ts.nameToIdx : ts.idToIdx;
      unresolvedEdges += deferredSelf.get(i).resolveInto(index, ec, ed.incoming);
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
      // Split field: e.g. "|java|python|css|". Kept in step with collectSplitKeys(), which walks a
      // field the same way for a self-referencing split - a shared walker would have to hand each
      // value to a closure, and this runs once per row
      final String fieldVal = record.get(ed.fkAttribute);
      if (fieldVal == null || fieldVal.isEmpty())
        return;
      final TypeState targetTs = typeStates.get(ed.targetType);
      final char delim = ed.delimiter.charAt(0);
      int start = fieldVal.charAt(0) == delim ? 1 : 0;
      int pos;
      while (start < fieldVal.length()) {
        pos = fieldVal.indexOf(delim, start);
        // the convention wraps the field in delimiters, but a last value without a closing one is
        // still a value: it used to be dropped, and not even counted as an edge lost
        final int end = pos == -1 ? fieldVal.length() : pos;
        if (end > start) {
          final int ti = targetTs.nameToIdx.get(fieldVal.substring(start, end));
          if (ti >= 0) {
            ec.srcIdx.add(thisIdx);
            ec.dstIdx.add(ti);
          } else
            unresolvedEdges++;
        }
        start = end + 1;
      }
    } else {
      // An absent or empty attribute means "this row has no such reference" and is not an
      // unresolved endpoint. It is the only way to say so: 0 used to double as that marker, which
      // made a vertex whose key really is 0 impossible to point at
      final String key = identity(record, ed.fkAttribute);
      if (key == null)
        return;
      final TypeState targetTs = typeStates.get(ed.targetType);
      final int targetIdx = (ed.byName ? targetTs.nameToIdx : targetTs.idToIdx).get(key);
      if (targetIdx < 0) {
        unresolvedEdges++;
        return;
      }

      if (ed.incoming) {
        ec.srcIdx.add(targetIdx);
        ec.dstIdx.add(thisIdx);
      } else {
        ec.srcIdx.add(thisIdx);
        ec.dstIdx.add(targetIdx);
      }
    }
  }

  /**
   * Reads an identity attribute, reporting an attribute that is absent or empty as {@code null} -
   * "this row carries no such key". The readers disagree on which of the two an empty field is:
   * {@link CsvRowSource} already hands back {@code null}, while {@link JsonlRowSource} and
   * {@link XmlRowSource} hand back the empty string for {@code "id": ""} and {@code id=""}. Left to
   * each reader, an empty id would be a key of its own, and every row carrying one would collide on
   * it - the last silently winning any edge that referenced it.
   */
  private static String identity(final RecordReader record, final String attribute) {
    final String value = record.get(attribute);
    return value == null || value.isEmpty() ? null : value;
  }

  /**
   * Appends one deferred key per value of a delimited field (e.g. {@code "|java|python|"}), all
   * sharing the same source vertex.
   */
  private static void collectSplitKeys(final String fieldVal, final char delimiter,
                                       final DeferredSelfEdges deferred, final int thisIdx) {
    if (fieldVal.isEmpty())
      return;
    int start = fieldVal.charAt(0) == delimiter ? 1 : 0;
    int pos;
    while (start < fieldVal.length()) {
      pos = fieldVal.indexOf(delimiter, start);
      final int end = pos == -1 ? fieldVal.length() : pos;
      if (end > start) {
        deferred.srcIdx.add(thisIdx);
        deferred.targetKeys.add(fieldVal.substring(start, end));
      }
      start = end + 1;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Pass 1: Process edge-only sources
  // ═══════════════════════════════════════════════════════════════════

  private void processEdgeSource(final EdgeSourceDef esd, final int sourceIndex) throws Exception {
    final long t = System.currentTimeMillis();
    final EdgeSourceConfig cfg = esd.config;
    final TypeState fromTs = typeStates.get(cfg.fromVertexType);
    final TypeState toTs = typeStates.get(cfg.toVertexType);

    // Own collector per edge source, never the one vertex-derived edges of the same type and
    // endpoints share. A collector's property buffers are indexed by the collector-wide edge index,
    // and only an edge source contributes properties: sharing would start its buffers at index 0
    // against a srcIdx that already holds the vertex-derived rows, silently assigning this source's
    // values to those edges and then running off the end of the buffer. Two edge sources declaring
    // the same edge type with different property sets would collide the same way
    final EdgeCollector ec = getOrCreateEdgeCollector(cfg.edgeType, cfg.fromVertexType, cfg.toVertexType,
        "src" + sourceIndex);
    final int[] count = {0};
    final long unresolvedBefore = unresolvedEdges;

    esd.source.forEach(record -> {
      if (limit > 0 && count[0] >= limit)
        return;
      final int si = fromTs.idToIdx.get(identity(record, cfg.fromAttribute));
      final int di = toTs.idToIdx.get(identity(record, cfg.toAttribute));
      if (si < 0 || di < 0)
        unresolvedEdges++;
      else {
        ec.srcIdx.add(si);
        ec.dstIdx.add(di);
        for (final PropDef pd : cfg.properties) {
          switch (pd.type) {
          case INTEGER:
            if (ec.intProps == null)
              ec.intProps = new HashMap<>();
            ec.intProps.computeIfAbsent(pd.name, k -> new IntList(BUFFER_INITIAL_CAPACITY)).add(readInt(record, pd));
            break;
          case LONG:
            if (ec.longProps == null)
              ec.longProps = new HashMap<>();
            ec.longProps.computeIfAbsent(pd.name, k -> new ArrayList<>(BUFFER_INITIAL_CAPACITY)).add(readLong(record, pd));
            break;
          case DOUBLE:
            if (ec.doubleProps == null)
              ec.doubleProps = new HashMap<>();
            ec.doubleProps.computeIfAbsent(pd.name, k -> new DoubleList(BUFFER_INITIAL_CAPACITY)).add(readDouble(record, pd));
            break;
          default:
            // STRING, BOOLEAN, DATETIME, FLOAT_ARRAY and LIST all go through the same reader
            // vertices use, so a spec means the same thing on an edge source as on a vertex
            if (ec.objProps == null)
              ec.objProps = new HashMap<>();
            ec.objProps.computeIfAbsent(pd.name, k -> new ArrayList<>(BUFFER_INITIAL_CAPACITY)).add(readProperty(record, pd));
            break;
          }
        }
      }
      count[0]++;
    });

    final long unresolved = unresolvedEdges - unresolvedBefore;
    LogManager.instance().log(this, Level.INFO, "  %-12s %,d edges (%,d ms)",
        cfg.edgeType, ec.srcIdx.size, System.currentTimeMillis() - t);
    if (unresolved > 0)
      LogManager.instance().log(this, Level.WARNING,
          "  %-12s %,d rows name an identity no %s vertex carries: those edges were skipped",
          cfg.edgeType, unresolved,
          cfg.fromVertexType.equals(cfg.toVertexType) ? cfg.fromVertexType : cfg.fromVertexType + "/" + cfg.toVertexType);
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
    return getOrCreateEdgeCollector(edgeType, srcType, dstType, "");
  }

  /**
   * Key by (edgeType, srcType, dstType) — the same edge type can connect different vertex type pairs
   * (e.g. POSTED: User→Question and POSTED: User→Answer) — plus a discriminator that keeps each edge
   * source's rows in a collector of its own. Vertex-derived edges carry no properties, so they all
   * share the empty discriminator.
   * <p>
   * The parts are joined with a pipe, which a schema type name cannot contain, so two different
   * triples cannot collide on one key.
   */
  private EdgeCollector getOrCreateEdgeCollector(final String edgeType, final String srcType, final String dstType,
                                                 final String discriminator) {
    final String key = edgeType + "|" + srcType + "|" + dstType + (discriminator.isEmpty() ? "" : "|" + discriminator);
    return edgeCollectors.computeIfAbsent(key, k -> new EdgeCollector(edgeType, srcType, dstType));
  }

  private static final DateTimeFormatter DEFAULT_DATETIME_FMT = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
      .toFormatter(Locale.ENGLISH);

  private static Object readProperty(final RecordReader record, final PropDef pd) {
    switch (pd.type) {
    case INTEGER:
      return readInt(record, pd);
    case LONG:
      return readLong(record, pd);
    case DOUBLE:
      return readDouble(record, pd);
    case BOOLEAN:
      // get() returns a String, so there is no format to get wrong: anything but "True" is false
      return "True".equalsIgnoreCase(record.get(pd.attribute));
    case FLOAT_ARRAY:
      try {
        return record.getFloatArray(pd.attribute);
      } catch (final IllegalArgumentException | JSONException e) {
        throw badValue(pd, "a vector", "a numeric array", e);
      }
    case LIST:
      try {
        return record.getList(pd.attribute);
      } catch (final IllegalArgumentException | JSONException e) {
        throw badValue(pd, "a list", "an array", e);
      }
    case DATETIME: {
      final String v = record.get(pd.attribute);
      // Empty means "not set", as it does in the RecordReader defaults above: getInt/getLong/
      // getDouble answer 0 and getFloatArray/getList answer null for an empty value, so a blank
      // cell in an optional datetime column must not abort the import either. null is already this
      // branch's "not set" answer and both call sites drop it, so returning it needs nothing else.
      // A value that is present but not whitespace-free is still a data error: isEmpty(), not
      // isBlank(), is what every accessor above tests (#7265)
      if (v == null || v.isEmpty())
        return null;
      // DateUtils.getFormatter(), not DateTimeFormatter.ofPattern(): the latter binds the JVM default locale, so the
      // same file imported on two machines would parse a textual month/day name differently, or not at all (#7144)
      final DateTimeFormatter fmt = pd.datetimeFormat != null
          ? DateUtils.getFormatter(pd.datetimeFormat)
          : DEFAULT_DATETIME_FMT;
      try {
        return LocalDateTime.parse(v, fmt);
      } catch (final DateTimeParseException e) {
        throw badValue(pd, "a datetime", "one", e);
      }
    }
    default:
      return record.get(pd.attribute);
    }
  }

  // The numeric readers return primitives, so an edge source can fill its primitive buffers without
  // boxing every value through readProperty, and still report a bad value the same way.

  private static int readInt(final RecordReader record, final PropDef pd) {
    try {
      return record.getInt(pd.attribute);
    } catch (final IllegalArgumentException | JSONException e) {
      throw badValue(pd, "an integer", "one", e);
    }
  }

  private static long readLong(final RecordReader record, final PropDef pd) {
    try {
      return record.getLong(pd.attribute);
    } catch (final IllegalArgumentException | JSONException e) {
      throw badValue(pd, "a long", "one", e);
    }
  }

  private static double readDouble(final RecordReader record, final PropDef pd) {
    try {
      return record.getDouble(pd.attribute);
    } catch (final IllegalArgumentException | JSONException e) {
      throw badValue(pd, "a double", "one", e);
    }
  }

  /**
   * Names the property and the source attribute, so a bad value in a bulk load says which mapping to
   * look at instead of surfacing as a bare NumberFormatException or JSONException from inside the
   * row loop. Only the exceptions a bad value actually produces are caught at the call sites, so an
   * unrelated bug inside a custom {@link RecordReader} still surfaces as itself.
   */
  private static IllegalArgumentException badValue(final PropDef pd, final String declaredAs, final String expected,
                                                   final Exception cause) {
    return new IllegalArgumentException(
        "Property '" + pd.name + "' is declared as " + declaredAs + " but attribute '" + pd.attribute
            + "' does not hold " + expected + " (" + cause.getMessage() + ")", cause);
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

  /**
   * Two places dispatch on this: {@link #readProperty} materializes a value for a vertex, and
   * {@code processEdgeSource} routes INTEGER, LONG and DOUBLE into the primitive buffers an edge
   * collector keeps for them and everything else through {@code readProperty} into {@code objProps}.
   * A new type added here needs a case in the first and, unless it belongs in a primitive buffer,
   * nothing in the second - the default branch already carries it.
   */
  enum PropType {STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATETIME, FLOAT_ARRAY, LIST}

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
    IdIndex idToIdx   = new IdIndex();
    IdIndex nameToIdx = new IdIndex();
    int[]   buckets;
    long[]  positions;
    int     count;
  }

  static class EdgeCollector {
    final String edgeTypeName, srcType, dstType;
    final IntList srcIdx = new IntList(BUFFER_INITIAL_CAPACITY);
    final IntList dstIdx = new IntList(BUFFER_INITIAL_CAPACITY);
    Map<String, IntList>      intProps;
    Map<String, List<Long>>   longProps;
    Map<String, DoubleList>   doubleProps;
    // Everything the primitive lists above cannot hold without boxing it anyway: strings, booleans,
    // datetimes, vectors and lists. A null entry keeps the index aligned with srcIdx for a row where
    // the attribute was missing, and is skipped when the edge is created.
    Map<String, List<Object>> objProps;

    EdgeCollector(final String edgeTypeName, final String src, final String dst) {
      this.edgeTypeName = edgeTypeName;
      this.srcType = src;
      this.dstType = dst;
    }

    boolean hasProperties() {
      return (intProps != null && !intProps.isEmpty())
          || (longProps != null && !longProps.isEmpty())
          || (doubleProps != null && !doubleProps.isEmpty())
          || (objProps != null && !objProps.isEmpty());
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
      if (objProps != null)
        for (final Map.Entry<String, List<Object>> pe : objProps.entrySet()) {
          final Object value = pe.getValue().get(i);
          if (value != null) {
            props.add(pe.getKey());
            props.add(value);
          }
        }
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Primitive collections (zero boxing, minimal GC)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Reads {@code text} as the canonical decimal form of a {@code long}, returning
   * {@link #NOT_CANONICAL_LONG} when it is not one. Neither allocates nor throws: an identity
   * column is read once per row, and {@code Long.parseLong} in a {@code try} block would fill in a
   * stack trace for every row of a string-keyed file.
   * <p>
   * "Canonical" is what keeps the primitive fast path from merging keys the source kept apart:
   * {@code "007"} and {@code "7"} are two different identities, so only the form
   * {@code Long.toString} would produce is allowed to become a number. Everything else - leading
   * zeros, a leading {@code +}, {@code "-0"}, surrounding space, anything non-numeric - stays text.
   */
  static long canonicalLong(final String text) {
    if (text == null)
      return NOT_CANONICAL_LONG;
    final int len = text.length();
    if (len == 0)
      return NOT_CANONICAL_LONG;

    final boolean negative = text.charAt(0) == '-';
    final int first = negative ? 1 : 0;
    if (len - first < 1 || len - first > 19)
      return NOT_CANONICAL_LONG;

    final char firstDigit = text.charAt(first);
    if (firstDigit < '0' || firstDigit > '9')
      return NOT_CANONICAL_LONG;
    // "0" is canonical, "00" and "007" are not, and "-0" is not the canonical form of zero
    if (firstDigit == '0' && (len - first > 1 || negative))
      return NOT_CANONICAL_LONG;

    // Accumulate negatively: the negative range is the wider one, so this overflows only on values
    // that genuinely do not fit, and Long.MIN_VALUE itself is rejected because it is the sentinel
    long value = 0;
    for (int i = first; i < len; i++) {
      final char c = text.charAt(i);
      if (c < '0' || c > '9')
        return NOT_CANONICAL_LONG;
      if (value < -922337203685477580L)
        return NOT_CANONICAL_LONG;
      value *= 10;
      final int digit = c - '0';
      if (value < Long.MIN_VALUE + digit)
        return NOT_CANONICAL_LONG;
      value -= digit;
    }
    if (value == NOT_CANONICAL_LONG)
      return NOT_CANONICAL_LONG;
    return negative ? value : -value;
  }

  /**
   * Maps a vertex identity, as the source spelled it, to the vertex's index within its type.
   * <p>
   * Behaves exactly like a {@code Map<String, Integer>} - two keys are the same identity when their
   * text is the same - but stores a key that is the canonical decimal form of a {@code long}
   * unboxed, which is what almost every real import consists of. The numeric side starts as an
   * {@code int} map and widens to a {@code long} one only when a key that does not fit arrives, so
   * an import whose keys fit in an {@code int} pays exactly what it paid before; a textual key
   * lands in a {@link HashMap} that is not allocated at all until one appears.
   */
  static final class IdIndex {
    /**
     * {@link IntIntMap} reserves {@code Integer.MIN_VALUE} to mark an empty slot, so that one value
     * goes to the {@code long} map instead of being stored as an {@code int}.
     */
    private static final int INT_KEY_MIN = Integer.MIN_VALUE + 1;

    private IntIntMap            intKeys;
    private LongIntMap           longKeys;
    private Map<String, Integer> textKeys;

    void put(final String key, final int idx) {
      if (key == null || key.isEmpty())
        return;
      final long numeric = canonicalLong(key);
      if (numeric == NOT_CANONICAL_LONG) {
        if (textKeys == null)
          textKeys = new HashMap<>();
        textKeys.put(key, idx);
      } else
        putNumeric(numeric, idx);
    }

    /**
     * @return the vertex index, or -1 when the key is null or empty (the row carries no such
     * reference) or no vertex was registered under it
     */
    int get(final String key) {
      if (key == null || key.isEmpty())
        return -1;
      final long numeric = canonicalLong(key);
      if (numeric != NOT_CANONICAL_LONG)
        return getNumeric(numeric);
      return textKeys == null ? -1 : textKeys.getOrDefault(key, -1);
    }

    int getNumeric(final long key) {
      if (longKeys != null)
        return longKeys.get(key, -1);
      if (intKeys == null || key < INT_KEY_MIN || key > Integer.MAX_VALUE)
        return -1;
      return intKeys.get((int) key, -1);
    }

    private void putNumeric(final long key, final int idx) {
      if (longKeys == null && key >= INT_KEY_MIN && key <= Integer.MAX_VALUE) {
        if (intKeys == null)
          // Sized like the edge buffers rather than for a large import: the map doubles on growth,
          // so a big source reaches its size in a handful of rehashes, while a schema with many
          // small types no longer pays a multi-megabyte table per type for a few hundred keys
          intKeys = new IntIntMap(BUFFER_INITIAL_CAPACITY);
        intKeys.put((int) key, idx);
        return;
      }
      if (longKeys == null)
        widenToLongKeys();
      longKeys.put(key, idx);
    }

    /** One-shot, on the first key outside the {@code int} range: rehashes what is already there. */
    private void widenToLongKeys() {
      longKeys = new LongIntMap(intKeys == null ? BUFFER_INITIAL_CAPACITY : intKeys.size());
      if (intKeys != null) {
        intKeys.copyInto(longKeys);
        intKeys = null;
      }
    }
  }

  /**
   * Target keys of self-referencing edges, held until the source has been read to the end because
   * the row they point at may still be ahead. The source index is already final and stays an
   * {@code int}; only the key has to survive the pass.
   */
  static final class DeferredSelfEdges {
    final IntList srcIdx     = new IntList(BUFFER_INITIAL_CAPACITY);
    final KeyList targetKeys = new KeyList(BUFFER_INITIAL_CAPACITY);

    /**
     * Resolves every buffered key against {@code index} and appends the edges to {@code ec}.
     *
     * @return how many keys matched no vertex
     */
    int resolveInto(final IdIndex index, final EdgeCollector ec, final boolean incoming) {
      int unresolved = 0;
      int textPos = 0;
      for (int i = 0; i < srcIdx.size; i++) {
        final long numeric = targetKeys.keys.data[i];
        final int di = numeric == NOT_CANONICAL_LONG ?
            index.get(targetKeys.text.get(textPos++)) :
            index.getNumeric(numeric);
        if (di < 0) {
          unresolved++;
          continue;
        }
        if (incoming) {
          ec.srcIdx.add(di);
          ec.dstIdx.add(srcIdx.data[i]);
        } else {
          ec.srcIdx.add(srcIdx.data[i]);
          ec.dstIdx.add(di);
        }
      }
      return unresolved;
    }
  }

  /**
   * An append-only list of identity keys that keeps the canonical numeric ones in a primitive
   * array and boxes only the rest. A textual key is marked in place with
   * {@link #NOT_CANONICAL_LONG} and appended to {@link #text}, which stays null while there is
   * none: replaying the list is a single forward walk, so the two run in step without a per-entry
   * back-reference.
   */
  static final class KeyList {
    final LongList     keys;
    List<String>       text;

    KeyList(final int cap) {
      keys = new LongList(cap);
    }

    void add(final String key) {
      final long numeric = canonicalLong(key);
      if (numeric == NOT_CANONICAL_LONG) {
        if (text == null)
          text = new ArrayList<>();
        text.add(key);
      }
      keys.add(numeric);
    }
  }

  /**
   * Open-addressing long→int hash map with Fibonacci hashing, the widened twin of
   * {@link IntIntMap}.
   */
  static final class LongIntMap {
    private static final long   EMPTY = Long.MIN_VALUE;
    private              long[] keys;
    private              int[]  values;
    private int mask;
    private int shift;
    private int size;
    private int threshold;

    LongIntMap(final int expected) {
      final int cap = Integer.highestOneBit(Math.max(16, (int) (expected / 0.7))) << 1;
      keys = new long[cap];
      values = new int[cap];
      capacity(cap);
      Arrays.fill(keys, EMPTY);
    }

    private void capacity(final int cap) {
      mask = cap - 1;
      shift = Long.SIZE - Integer.numberOfTrailingZeros(cap);
      threshold = (int) (cap * 0.7);
    }

    void put(final long key, final int value) {
      if (size >= threshold)
        resize();
      int i = hash(key);
      while (keys[i] != EMPTY && keys[i] != key)
        i = (i + 1) & mask;
      if (keys[i] == EMPTY)
        size++;
      keys[i] = key;
      values[i] = value;
    }

    int get(final long key, final int def) {
      int i = hash(key);
      while (keys[i] != EMPTY) {
        if (keys[i] == key)
          return values[i];
        i = (i + 1) & mask;
      }
      return def;
    }

    /**
     * Fibonacci hashing: the top {@code log2(capacity)} bits of the product, which every bit of the
     * key influences. Any lower window is a trap, because a key can zero it. Masking the low bits
     * of {@code key * odd} makes the slot a function of the key's low bits alone; a fixed
     * {@code >>> 32} reads better but still leaves a key with 45 trailing zeros zeroing bits 32
     * through 44 of the product, sending every such key to slot 0. Ids allocated in blocks or
     * carrying a fixed stride are ordinary, and either way the map degrades to a linear scan.
     */
    int hash(final long key) {
      return (int) ((key * 0x9E3779B97F4A7C15L) >>> shift);
    }

    private void resize() {
      final int newCap = keys.length << 1;
      final long[] ok = keys;
      final int[] ov = values;
      keys = new long[newCap];
      values = new int[newCap];
      capacity(newCap);
      Arrays.fill(keys, EMPTY);
      for (int i = 0; i < ok.length; i++)
        if (ok[i] != EMPTY) {
          int j = hash(ok[i]);
          while (keys[j] != EMPTY)
            j = (j + 1) & mask;
          keys[j] = ok[i];
          values[j] = ov[i];
        }
    }
  }

  /**
   * Open-addressing int→int hash map with Fibonacci hashing.
   */
  static final class IntIntMap {
    // see LongIntMap.hash(): the same scramble, and the same reason for taking the high bits
    private static final int   EMPTY = Integer.MIN_VALUE;
    private              int[] keys;
    private              int[] values;
    private int mask;
    private int shift;
    private int size;
    private int threshold;

    int hash(final int key) {
      return (int) ((key * 0x9E3779B97F4A7C15L) >>> shift);
    }

    private void capacity(final int cap) {
      mask = cap - 1;
      shift = Long.SIZE - Integer.numberOfTrailingZeros(cap);
      threshold = (int) (cap * 0.7);
    }

    int size() {
      return size;
    }

    /** Rehashes every entry into {@code target}, for {@link IdIndex}'s one-shot widening. */
    void copyInto(final LongIntMap target) {
      for (int i = 0; i < keys.length; i++)
        if (keys[i] != EMPTY)
          target.put(keys[i], values[i]);
    }

    IntIntMap(final int expected) {
      final int cap = Integer.highestOneBit(Math.max(16, (int) (expected / 0.7))) << 1;
      keys = new int[cap];
      values = new int[cap];
      capacity(cap);
      Arrays.fill(keys, EMPTY);
    }

    void put(final int key, final int value) {
      if (size >= threshold)
        resize();
      int i = hash(key);
      while (keys[i] != EMPTY && keys[i] != key)
        i = (i + 1) & mask;
      if (keys[i] == EMPTY)
        size++;
      keys[i] = key;
      values[i] = value;
    }

    int get(final int key, final int def) {
      int i = hash(key);
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
      capacity(newCap);
      Arrays.fill(keys, EMPTY);
      for (int i = 0; i < ok.length; i++)
        if (ok[i] != EMPTY) {
          int j = hash(ok[i]);
          while (keys[j] != EMPTY)
            j = (j + 1) & mask;
          keys[j] = ok[i];
          values[j] = ov[i];
        }
    }
  }

  /**
   * Initial capacity of an {@link EdgeCollector}'s buffers. Each edge source gets a collector of its
   * own, so a file with several small sources of the same edge type holds several sets of these.
   * Every list here doubles on growth: a large source reaches its size in a handful of copies, and
   * a small one no longer sits on a buffer it will never fill.
   */
  static final int BUFFER_INITIAL_CAPACITY = 4_096;

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
