/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin;

import com.arcadedb.cypher.ArcadeCypher;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.gremlin.service.ArcadeServiceRegistry;
import com.arcadedb.gremlin.service.VectorNeighborsFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.DefaultGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.opencypher.gremlin.traversal.CustomPredicate;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;

/**
 * Created by Enrico Risa on 30/07/2018.
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn("com.arcadedb.gremlin.process.DebugProcessSuite")
@Graph.OptIn("com.arcadedb.gremlin.structure.DebugStructureSuite")
public class ArcadeGraph implements Graph, Closeable {

  public static final  String CONFIG_DIRECTORY    = "gremlin.arcadedb.directory";
  private static final int    GREMLIN_SERVER_PORT = 8182;

  //private final   ArcadeVariableFeatures graphVariables = new ArcadeVariableFeatures();
  private final        ArcadeGraphTransaction    transaction;
  protected final      BasicDatabase             database;
  protected final      BaseConfiguration         configuration  = new BaseConfiguration();
  private final static Iterator<Vertex>          EMPTY_VERTICES = Collections.emptyIterator();
  private final static Iterator<Edge>            EMPTY_EDGES    = Collections.emptyIterator();
  protected            Features                  features       = new ArcadeGraphFeatures();
  private              GremlinLangScriptEngine   gremlinJavaEngine;
  private              GremlinGroovyScriptEngine gremlinGroovyEngine;
  private              ServiceRegistry           serviceRegistry;
  private              GraphTraversalSource      traversal;

  static {
    TraversalStrategies.GlobalCache.registerStrategies(ArcadeGraph.class,
        TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()//
            .addStrategies(//
                ArcadeIoRegistrationStrategy.instance(),//
                new ArcadeTraversalStrategy())//
    );
  }

  protected ArcadeGraph(final Configuration configuration) {
    this.configuration.copy(configuration);
    final String directory = this.configuration.getString(CONFIG_DIRECTORY);

    Database db = DatabaseFactory.getActiveDatabaseInstance(directory);
    if (db == null) {
      final DatabaseFactory factory = new DatabaseFactory(directory);
      if (!factory.exists())
        db = factory.create();
      else
        db = factory.open();
    }

    this.database = db;

    this.transaction = new ArcadeGraphTransaction(this);
    init();
  }

  protected ArcadeGraph(final BasicDatabase database) {
    this.database = database;
    this.transaction = new ArcadeGraphTransaction(this);
    init();
  }

  @Override
  public Features features() {
    return features;
  }

  public static ArcadeGraph open(final Configuration configuration) {
    if (null == configuration)
      throw Graph.Exceptions.argumentCanNotBeNull("configuration");
    if (!configuration.containsKey(CONFIG_DIRECTORY))
      throw new IllegalArgumentException("Arcade configuration requires that the %s be set".formatted(CONFIG_DIRECTORY));
    return new ArcadeGraph(configuration);
  }

  public static ArcadeGraph open(final String directory) {
    final Configuration config = new BaseConfiguration();
    config.setProperty(CONFIG_DIRECTORY, directory);
    return open(config);
  }

  public static ArcadeGraph open(final BasicDatabase database) {
    return new ArcadeGraph(database);
  }

  public ArcadeCypher cypher(final String query) {
    return cypher(query, Collections.emptyMap());
  }

  public ArcadeCypher cypher(final String query, final Map<String, Object> parameters) {
    ArcadeCypher arcadeCypher = new ArcadeCypher(this, query);
    arcadeCypher.setParameters(parameters);
    return arcadeCypher;
  }

  public ArcadeGremlin gremlin(final String query) {
    return new ArcadeGremlin(this, query);
  }

  /**
   * Returns a Gremlin traversal. This traversal is executed outside the database's transaction if any.
   */
  public GraphTraversalSource traversal() {
    if (traversal != null)
      return traversal;

    if (database instanceof RemoteDatabase remoteDatabase) {
      try {
        final List<String> remoteAddresses = new ArrayList<>();

        remoteAddresses.add(remoteDatabase.getLeaderAddress());
        remoteAddresses.addAll(remoteDatabase.getReplicaAddresses());

        final String[] hosts = new String[remoteAddresses.size()];
        for (int i = 0; i < remoteAddresses.size(); i++)
          hosts[i] = HostUtil.parseHostAddress(remoteAddresses.getFirst(), "" + GREMLIN_SERVER_PORT)[0];

        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
            new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

        Cluster cluster = Cluster.build().enableSsl(false).addContactPoints(hosts).port(GREMLIN_SERVER_PORT)
            .credentials(remoteDatabase.getUserName(), remoteDatabase.getUserPassword()).serializer(serializer).create();

        traversal = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, database.getName()));
      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "GremlinServer plugin not available on ArcadeDB server(s). Using slower remote implementation.");
        traversal = Graph.super.traversal();
      }
    } else
      traversal = Graph.super.traversal();

    return traversal;
  }

  public ArcadeSQL sql(final String query) {
    return new ArcadeSQL(this, query);
  }

  @Override
  public ArcadeVertex addVertex(final String label) {
    return (ArcadeVertex) Graph.super.addVertex(label);
  }

  @Override
  public ArcadeVertex addVertex(final Object... keyValues) {
    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();
    this.tx().readWrite();

    String typeName = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    final String bucketName;
    if (typeName.startsWith("bucket:")) {
      bucketName = typeName.substring("bucket:".length());
      final DocumentType type = database.getSchema().getTypeByBucketName(bucketName);
      if (type == null)
        typeName = null;
      else
        typeName = type.getName();
    } else
      bucketName = null;

    if (!this.database.getSchema().existsType(typeName))
      this.database.getSchema().createVertexType(typeName);
    else if (!(this.database.getSchema().getType(typeName) instanceof VertexType))
      throw new IllegalArgumentException("Type '" + typeName + "' is not a vertex");

    final MutableVertex modifiableVertex = this.database.newVertex(typeName);
    final ArcadeVertex vertex = new ArcadeVertex(this, modifiableVertex, keyValues);
    if (bucketName != null)
      modifiableVertex.save(bucketName);
    else
      modifiableVertex.save();
    return vertex;
  }

  @Override
  public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
    throw Graph.Exceptions.graphComputerNotSupported();
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    throw Graph.Exceptions.graphComputerNotSupported();
  }

  @Override
  public Iterator<Vertex> vertices(final Object... vertexIds) {
    tx().readWrite();

    if (vertexIds.length == 0) {
      final Collection<? extends DocumentType> types = this.database.getSchema().getTypes();
      final Set<Bucket> buckets = new HashSet<>();
      for (final DocumentType t : types)
        if (t instanceof VertexType)
          buckets.addAll(t.getBuckets(true));

      if (buckets.isEmpty())
        return EMPTY_VERTICES;

      // BUILD THE QUERY
      final StringBuilder query = new StringBuilder("select from bucket:[");
      int i = 0;
      for (final Bucket b : buckets) {
        if (i > 0)
          query.append(", ");
        query.append("`");
        query.append(b.getName());
        query.append("`");
        ++i;
      }
      query.append("]");

      final ResultSet resultset = this.database.query("sql", query.toString());
      return resultset.stream().filter((a) -> a.getIdentity().isEmpty() || database.existsRecord(a.getIdentity().get()))
          .map(result -> (Vertex) new ArcadeVertex(this, (com.arcadedb.graph.Vertex) (result.toElement()))).iterator();
    }

    final List<Vertex> resultSet = new ArrayList<>(vertexIds.length);

    for (final Object o : vertexIds) {
      final RID rid;
      switch (o) {
      case RID iD -> rid = iD;
      case Vertex vertex -> {
        final Object objectId = vertex.id();
        if (objectId != null)
          rid = objectId instanceof RID rid1 ? rid1 : new RID(database, objectId.toString());
        else
          continue;
      }
      case String string -> rid = new RID(database, string);
      case null, default -> {
        continue;
      }
      }

      try {
        final Record r = database.lookupByRID(rid, true);
        if (r instanceof com.arcadedb.graph.Vertex vertex) {
          resultSet.add(new ArcadeVertex(this, vertex));
        }
      } catch (final RecordNotFoundException e) {
        // NP, IGNORE IT
      }
    }

    return resultSet.iterator();
  }

  @Override
  public Iterator<Edge> edges(final Object... edgeIds) {
    tx().readWrite();

    if (edgeIds.length == 0) {

      final Collection<? extends DocumentType> types = this.database.getSchema().getTypes();
      final Set<Bucket> buckets = new HashSet<>();
      for (final DocumentType t : types)
        if (t instanceof EdgeType)
          buckets.addAll(t.getBuckets(true));

      if (buckets.isEmpty())
        return EMPTY_EDGES;

      // BUILD THE QUERY
      final StringBuilder query = new StringBuilder("select from bucket:[");
      int i = 0;
      for (final Bucket b : buckets) {
        if (i > 0)
          query.append(", ");
        query.append("`");
        query.append(b.getName());
        query.append("`");
        ++i;
      }
      query.append("]");

      final ResultSet resultSet = this.database.query("sql", query.toString());
      return resultSet.stream().filter((a) -> a.getIdentity().isEmpty() || database.existsRecord(a.getIdentity().get()))
          .map(result -> (Edge) new ArcadeEdge(this, (com.arcadedb.graph.Edge) result.toElement())).iterator();

    }

    final List<Edge> resultSet = new ArrayList<>(edgeIds.length);

    for (final Object o : edgeIds) {
      final RID rid;
      switch (o) {
      case RID iD -> rid = iD;
      case Edge edge -> {
        final Object objectId = edge.id();
        if (objectId != null)
          rid = objectId instanceof RID rid1 ? rid1 : new RID(database, objectId.toString());
        else
          continue;
      }
      case String string -> rid = new RID(database, string);
      case null, default -> {
        continue;
      }
      }

      try {
        final Record r = database.lookupByRID(rid, true);
        if (r instanceof com.arcadedb.graph.Edge edge)
          resultSet.add(new ArcadeEdge(this, edge));
      } catch (final RecordNotFoundException e) {
        // NP, IGNORE IT
      }
    }

    return resultSet.iterator();
  }

  @Override
  public <I extends Io> I io(final Io.Builder<I> builder) {
    return (I) Graph.super.io(builder.onMapper(mb -> mb.addRegistry(new ArcadeIoRegistry(this.database))));
  }

  @Override
  public Transaction tx() {
    return transaction;
  }

  @Override
  public void close() {
    gremlinJavaEngine = null;
    if (gremlinGroovyEngine != null) {
      gremlinGroovyEngine.reset();
      gremlinGroovyEngine = null;
    }

    if (this.database != null) {
      if (this.database.isTransactionActive())
        this.database.commit();

      this.database.close();

      ArcadeCypher.closeDatabase(this);
    }
  }

  public void drop() {
    gremlinJavaEngine = null;
    if (gremlinGroovyEngine != null) {
      gremlinGroovyEngine.reset();
      gremlinGroovyEngine = null;
    }

    if (this.database != null) {
      if (!this.database.isOpen())
        FileUtils.deleteRecursively(new File(this.database.getDatabasePath()));
      else {
        if (this.database.isTransactionActive())
          this.database.rollback();
        this.database.drop();
      }
    }
  }

  @Override
  public Variables variables() {
    throw Graph.Exceptions.variablesNotSupported();
  }

  @Override
  public Configuration configuration() {
    return configuration;
  }

  protected void deleteElement(final ArcadeElement element) {
    database.deleteRecord(element.getBaseElement().getRecord());
  }

  public BasicDatabase getDatabase() {
    return database;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final ArcadeGraph that = (ArcadeGraph) o;
    return Objects.equals(database, that.database);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database);
  }

  @Override
  public String toString() {
    return StringFactory.graphString(this, database.getName());
  }

  public static com.arcadedb.graph.Vertex.DIRECTION mapDirection(final Direction direction) {
    return switch (direction) {
      case OUT -> com.arcadedb.graph.Vertex.DIRECTION.OUT;
      case IN -> com.arcadedb.graph.Vertex.DIRECTION.IN;
      case BOTH -> com.arcadedb.graph.Vertex.DIRECTION.BOTH;
    };
  }

  public GremlinLangScriptEngine getGremlinJavaEngine() {
    return gremlinJavaEngine;
  }

  public GremlinGroovyScriptEngine getGremlinGroovyEngine() {
    return gremlinGroovyEngine;
  }

  @Override
  public ServiceRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  public ArcadeVertex getVertexFromRecord(final Identifiable record) {
    return new ArcadeVertex(this, record.asVertex());
  }

  public ArcadeEdge getEdgeFromRecord(final Identifiable record) {
    return new ArcadeEdge(this, record.asEdge());
  }

  private void init() {
    // INITIALIZE CYPHER
    final ImportGremlinPlugin.Builder importPlugin = ImportGremlinPlugin.build();
    importPlugin.classImports(Math.class, ArcadeCustomFunctions.class, CustomPredicate.class);
    importPlugin.methodImports(List.of("java.lang.Math#*", "com.arcadedb.gremlin.ArcadeCustomFunctions#*"));

    // INITIALIZE JAVA ENGINE
    gremlinJavaEngine = new GremlinLangScriptEngine(importPlugin.create().getCustomizers().get());
    gremlinJavaEngine.getFactory().setCustomizerManager(new DefaultGremlinScriptEngineManager());

    // INITIALIZE GROOVY ENGINE
    gremlinGroovyEngine = new GremlinGroovyScriptEngine(importPlugin.create().getCustomizers().get());
    gremlinGroovyEngine.getFactory().setCustomizerManager(new DefaultGremlinScriptEngineManager());

    serviceRegistry = new ArcadeServiceRegistry(this);
    serviceRegistry.registerService(new VectorNeighborsFactory(this));
  }
}
