package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile.MODE;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeleteTest {
  private static final String DB_NAME = "testDB";
  private static final String ROOT_PASSWORD = "playwithdata";
  private static final String ROOT_DIR = "arcade_db_delete_test";
  private static final List<String> TMP_DIRS = List.of("backups", "log", "databases", "config");
  private static ArcadeDBServer SERVER = null;
  private static RemoteDatabase DATABASE = null;
  private static Path ROOT_PATH = null;

  // Schema
  private static final String VERTEX_DUCT = "duct";
  private static final String EDGE_HIERARCHY_DUCT_DUCT = "hierarchy_duct_duct";
  private static final String PROPERTY_INTERNAL_ID = "internal_id";
  private static final String PROPERTY_INTERNAL_FROM = "internal_from";
  private static final String PROPERTY_INTERNAL_TO = "internal_to";
  private static final String PROPERTY_SWAP = "swap";
  private static final String PROPERTY_ORDER_NUMBER = "order_number";
  private static final String PROPERTY_RID = "@rid";

  // Use case params
  private static final String DUCT_ELID = "duct_elid";
  private static final String SUB_DUCT_ELID_1 = "sub_duct_elid_1";
  private static final String SUB_DUCT_ELID_2 = "sub_duct_elid_2";
  private static final String SUB_DUCT_ELID_3 = "sub_duct_elid_3";
  private static final String SUB_DUCT_ELID_4 = "sub_duct_elid_4";
  private static final String SUB_DUCT_ELID_5 = "sub_duct_elid_5";
  private static final String SUB_DUCT_ELID_6 = "sub_duct_elid_6";
  private static final String OBJECT_OWNER = "owner";
  private static final String SWAP = "N";
  private static final String ORDER_NUMBER = "1";
  private static final List<String> ALL_DUCT_ELIDS = List.of(DUCT_ELID, SUB_DUCT_ELID_1, SUB_DUCT_ELID_2, SUB_DUCT_ELID_3, SUB_DUCT_ELID_4, SUB_DUCT_ELID_5, SUB_DUCT_ELID_6);
  private static final List<String> SUB_DUCT_ELIDS = List.of(SUB_DUCT_ELID_1, SUB_DUCT_ELID_2, SUB_DUCT_ELID_3, SUB_DUCT_ELID_4, SUB_DUCT_ELID_5, SUB_DUCT_ELID_6);

  public static void main(String[] args) {
    try {
      new Thread(() -> {
        prepareTestDirectory();
        startArcadeDBServer();
        initRemoteDatabase();
        setupDatabaseSchema();
        insertInitialData();
        executeTestCase();
      }).start();
      Thread.currentThread().join();
    } catch (Exception e) {
      System.err.print("\nERROR: " + e.getMessage());
    } finally {
      closeDatabase();
      stopServer();
    }
  }

  private static void setupDatabaseSchema() {
    System.out.print("\nSetting up database schema.");
    createVertexType(VERTEX_DUCT);
    createHierarchyEdgeType(EDGE_HIERARCHY_DUCT_DUCT);
  }

  private static void insertInitialData() {
    System.out.print("\nInserting initial data.");
    /**
     *             duct
     * 		      ||..|
     * 		  sub_duct_1..6
     */
    insertTestCaseVertices();
    insertTestCaseEdges();
  }

  private static void executeTestCase() {
    System.out.print("\nExecuting test case.");
    final String where = "((internal_from = ?) AND (internal_to = ?) AND (swap = ?) AND (order_number = ?)) OR "
        + "((internal_from = ?) AND (internal_to = ?) AND (swap = ?) AND (order_number = ?)) OR "
        + "((internal_from = ?) AND (internal_to = ?) AND (swap = ?) AND (order_number = ?)) OR "
        + "((internal_from = ?) AND (internal_to = ?) AND (swap = ?) AND (order_number = ?)) OR "
        + "((internal_from = ?) AND (internal_to = ?) AND (swap = ?) AND (order_number = ?)) OR "
        + "((internal_from = ?) AND (internal_to = ?) AND (swap = ?) AND (order_number = ?))";
    deleteEdge(EDGE_HIERARCHY_DUCT_DUCT, where,
        List.of(DUCT_ELID, SUB_DUCT_ELID_1, SWAP, ORDER_NUMBER,
            DUCT_ELID, SUB_DUCT_ELID_2, SWAP, ORDER_NUMBER,
            DUCT_ELID, SUB_DUCT_ELID_3, SWAP, ORDER_NUMBER,
            DUCT_ELID, SUB_DUCT_ELID_4, SWAP, ORDER_NUMBER,
            DUCT_ELID, SUB_DUCT_ELID_5, SWAP, ORDER_NUMBER,
            DUCT_ELID, SUB_DUCT_ELID_6, SWAP, ORDER_NUMBER));
    final Map<String, String> allDuctIdMap = queryIdMap(VERTEX_DUCT, ALL_DUCT_ELIDS);
    createHierarchyEdge(SUB_DUCT_ELIDS.stream().map(subDuctElid -> new CreateHierarchyEdgeRecord(EDGE_HIERARCHY_DUCT_DUCT, allDuctIdMap.get(DUCT_ELID),  allDuctIdMap.get(subDuctElid), DUCT_ELID, subDuctElid, ORDER_NUMBER)).toList());
  }

  private static void insertTestCaseVertices() {
    createVertex(ALL_DUCT_ELIDS.stream().map(elid -> new CreateVertexRecord(VERTEX_DUCT, elid)).toList());
  }

  private static void insertTestCaseEdges() {
    // query vertex's rid(s)
    final Map<String, String> ductIdMap = queryIdMap(VERTEX_DUCT, ALL_DUCT_ELIDS);
    // duct <-..> sub_duct(s)
    final String ductRid = ductIdMap.get(DUCT_ELID);
    final List<CreateHierarchyEdgeRecord> records = new ArrayList<>();
    SUB_DUCT_ELIDS.forEach(subDuctElid -> records.add(new CreateHierarchyEdgeRecord(EDGE_HIERARCHY_DUCT_DUCT, ductRid, ductIdMap.get(subDuctElid), DUCT_ELID, subDuctElid, ORDER_NUMBER)));
    createHierarchyEdge(records);
  }

  private static Map<String, String> queryIdMap(final String label, final Collection<String> internalIds) {
    final List<String> properties = List.of(PROPERTY_RID, PROPERTY_INTERNAL_ID);
    final List<String> arguments = new ArrayList<>();
    final String placeholders = internalIds.stream() //
        .map(attr -> {
          arguments.add(attr);
          return "?";
        }).collect(Collectors.joining(", "));
    final String where = "%s in [%s]".formatted(PROPERTY_INTERNAL_ID, placeholders);
    return command("sql", "select %s from %s where %s".formatted(String.join(", ", properties), label, where), arguments)
        .stream() //
        .map(result -> Map.entry((String) result.getProperty(PROPERTY_INTERNAL_ID), (String) result.getProperty(PROPERTY_RID)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static void createVertex(final List<CreateVertexRecord> records) {
    final List<InsertVertexRecord> insertRecords = records.stream().map(record -> new InsertVertexRecord(record.vertex,
        Map.of(PROPERTY_INTERNAL_ID, record.internal_id))).toList();
    insertVertex(insertRecords);
  }

  private record CreateVertexRecord(String vertex, String internal_id) {}

  private static void createHierarchyEdge(final List<CreateHierarchyEdgeRecord> records) {
    final List<CreateEdgeRecord> createEdgeRecords = records.stream().map(record -> new CreateEdgeRecord(record.edge, record.fromRid, record.toRid,
        Map.of(PROPERTY_INTERNAL_FROM, record.internal_from, PROPERTY_INTERNAL_TO, record.internal_to, PROPERTY_SWAP, SWAP, PROPERTY_ORDER_NUMBER, record.orderNumber))).toList();
    createEdge(createEdgeRecords);
  }

  private record CreateHierarchyEdgeRecord(String edge, String fromRid, String toRid, String internal_from, String internal_to, String orderNumber) {}

  private static void insertVertex(final List<InsertVertexRecord> records) {
    final List<String> arguments = new ArrayList<>();
    final List<String> commands = records.stream().map(record -> {
      arguments.addAll(record.properties.values());
      final String valuesPlaceholder = record.properties.values().stream().map(attr -> "?").collect(Collectors.joining(", "));
      return "insert into %s (%s) values (%s)".formatted(record.into, String.join(", ", record.properties.keySet()), valuesPlaceholder);
    }).toList();
    command("sqlscript", String.join("; ", commands), arguments);
  }

  private record InsertVertexRecord(String into, Map<String, String> properties) {}

  private static void createEdge(final List<CreateEdgeRecord> records) {
    final List<String> arguments = new ArrayList<>();
    final List<String> commands = records.stream().map(record -> {
      arguments.add(record.fromRid);
      arguments.add(record.toRid);
      final List<String> placeholders = record.properties.entrySet() //
          .stream() //
          .map(entry -> {
            arguments.add(entry.getValue());
            return "%s = ?".formatted(entry.getKey());
          }).toList();
      return "create edge %s from ? to ? set %s".formatted(record.edge, String.join(", ", placeholders));
    }).toList();
    command("sqlscript", String.join("; ", commands), arguments);
  }

  private record CreateEdgeRecord(String edge, String fromRid, String toRid, Map<String, String> properties) {}

  private static ResultSet deleteEdge(final String edge, final String where, final List<String> params) {
    return command("sql", "delete from %s where %s".formatted(edge, where), params);
  }

  private static void createVertexType(final String label) {
    command("sql", "create vertex type %s if not exists".formatted(label));
    command("sql", "alter type %s BucketSelectionStrategy `thread`".formatted(label));
    createIndexedStringProperty(label, PROPERTY_INTERNAL_ID, true);
  }

  private static void createHierarchyEdgeType(final String label) {
    createEdgeType(label);
    createStringProperty(label, PROPERTY_SWAP);
    createStringProperty(label, PROPERTY_ORDER_NUMBER);
    createIndexedStringProperty(label, PROPERTY_INTERNAL_FROM, false);
    createIndexedStringProperty(label, PROPERTY_INTERNAL_TO, false);
    createIndex(label, List.of(PROPERTY_INTERNAL_FROM, PROPERTY_INTERNAL_TO, PROPERTY_SWAP, PROPERTY_ORDER_NUMBER), true);
  }

  private static void createEdgeType(final String label) {
    command("sql", "create edge type %s if not exists".formatted(label));
    command("sql", "alter type %s BucketSelectionStrategy `thread`".formatted(label));
  }

  private static void createStringProperty(final String label, final String property) {
    createProperty(label, property, "STRING");
  }

  private static void createIndexedStringProperty(final String label, final String property, final boolean unique) {
    createStringProperty(label, property);
    createIndex(label, Collections.singletonList(property), unique);
  }

  private static void createProperty(final String label, final String property, final String type) {
    command("sql", "create property " + label + "." + property + " " + type);
  }

  private static void createIndex(final String label, final List<String> properties, final boolean unique) {
    command("sql", "create index on %s (%s) %s".formatted(label, String.join(",", properties), unique ? "UNIQUE" : "NOTUNIQUE"));
  }

  private static ResultSet command(final String language, final String command) {
    		System.out.print("\nCommand: " + command);
    return DATABASE.command(language, command);
  }

  private static ResultSet command(final String language, final String command, Collection<String> parameters) {
    		System.out.print("\nCommand: " + command + "; Parameters: " + parameters);
    return DATABASE.command(language, command, parameters.toArray());
  }

  private static void closeDatabase() {
    if (DATABASE != null && DATABASE.isOpen()) {
      DATABASE.close();
    }
    DATABASE = null;
  }

  private static void stopServer() {
    if (SERVER != null && SERVER.isStarted()) {
      SERVER.stop();
    }
    SERVER = null;
  }

  private static void prepareTestDirectory() {
    System.out.print("\nPreparing test directory.");
    ROOT_PATH = Paths.get(ROOT_DIR);
    clearTestDirectory(ROOT_PATH);
    createDirectoryIfNotExists(ROOT_PATH);
    createTmpDirectories(ROOT_PATH);
  }

  private static void createTmpDirectories(final Path rootPath) {
    TMP_DIRS.stream().map(rootPath::resolve).forEach(DeleteTest::createDirectoryIfNotExists);
  }

  private static void createDirectoryIfNotExists(final Path dir) {
    if (Files.exists(dir)) {
      return;
    }
    try {
      Files.createDirectory(dir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void startArcadeDBServer() {
    System.out.print("\nStarting ArcadeDB server.");
    final ContextConfiguration config = new ContextConfiguration(
        Map.of(GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey(), ROOT_PASSWORD, GlobalConfiguration.SERVER_ROOT_PATH.getKey(), ROOT_PATH.toAbsolutePath().toString()));
    SERVER = new ArcadeDBServer(config);
    SERVER.start();
    SERVER.createDatabase(DB_NAME, MODE.READ_WRITE);
  }

  private static void initRemoteDatabase() {
    System.out.print("\nInitializing remote database.");
    DATABASE = new RemoteDatabase("localhost", 2480, DB_NAME, "root", ROOT_PASSWORD);
  }

  private static void clearTestDirectory(final Path rootPath) {
    TMP_DIRS.stream() //
        .map(rootPath::resolve) //
        .flatMap(path -> {
          try {
            return Files.walk(path).sorted(Comparator.reverseOrder());
          } catch (IOException e) {
            System.err.println("Failed to enter: " + e.getMessage());
            return null;
          }
        }) //
        .filter(Objects::nonNull) //
        .forEach(path -> {
          try {
            Files.delete(path);
          } catch (IOException e) {
            System.err.println("Failed to delete: " + e.getMessage());
          }
        });
  }
}
