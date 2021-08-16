/*
 * Copyright 2021 Arcade Data Ltd
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

package org.apache.tinkerpop.gremlin.arcadedb;

import com.arcadedb.database.RID;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.arcadedb.structure.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.SerializationTest;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.apache.tinkerpop.gremlin.structure.VertexTest;
import org.apache.tinkerpop.gremlin.structure.io.IoGraphTest;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.util.*;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.asList;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeGraphProvider extends AbstractGraphProvider {
  protected static final Map<Class<?>, List<String>> IGNORED_TESTS;

  static {
    IGNORED_TESTS = new HashMap<>();
    IGNORED_TESTS.put(TransactionTest.class, asList("shouldExecuteWithCompetingThreads"));
    IGNORED_TESTS.put(VertexTest.BasicVertexTest.class, Arrays.asList("shouldNotGetConcurrentModificationException"));
    //This tests become broken after gremlin 3.2.4
    IGNORED_TESTS.put(SerializationTest.GraphSONV3d0Test.class, Arrays.asList("shouldSerializeTraversalMetrics"));
    IGNORED_TESTS.put(ProfileTest.Traversals.class, Arrays.asList("testProfileStrategyCallback", "testProfileStrategyCallbackSideEffect"));
    IGNORED_TESTS.put(IoGraphTest.class, Arrays.asList("shouldReadWriteClassicToFileWithHelpers[graphml]", "shouldReadWriteModernToFileWithHelpers[graphml]"));
  }

  private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
    add(ArcadeEdge.class);
    add(ArcadeElement.class);
    add(ArcadeGraph.class);
    add(ArcadeVariableFeatures.class);
    add(ArcadeProperty.class);
    add(ArcadeVertex.class);
    add(ArcadeVertexProperty.class);
    add(RID.class);
  }};

  @Override
  public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
    if (IGNORED_TESTS.containsKey(test) && IGNORED_TESTS.get(test).contains(testMethodName))
      throw new AssumptionViolatedException("Ignored Test");

    if (testMethodName.contains("graphson"))
      throw new AssumptionViolatedException("graphson support not implemented");

    if (testMethodName.contains("gryo"))
      throw new AssumptionViolatedException("gryo support not implemented");

    final String directory = makeTestDirectory(graphName, test, testMethodName);

    return new HashMap<String, Object>() {{
      put(Graph.GRAPH, ArcadeGraph.class.getName());
      put("name", graphName);
      put(ArcadeGraph.CONFIG_DIRECTORY, directory);
    }};
  }

  @Override
  public void clear(Graph graph, Configuration configuration) throws Exception {

    if (graph != null)
      ((ArcadeGraph) graph).drop();

    if (configuration != null && configuration.containsKey(ArcadeGraph.CONFIG_DIRECTORY)) {
      // this is a non-in-sideEffects configuration so blow away the directory
      final File graphDirectory = new File(configuration.getString(ArcadeGraph.CONFIG_DIRECTORY));
      deleteDirectory(graphDirectory);
    }
  }

  @Override
  public Set<Class> getImplementations() {
    return IMPLEMENTATIONS;
  }

  protected String makeTestDirectory(final String graphName, final Class<?> test, final String testMethodName) {
    return this.getWorkingDirectory() + File.separator + cleanPathSegment(this.getClass().getSimpleName()) + File.separator + cleanPathSegment(
        test.getSimpleName()) + File.separator + cleanPathSegment(graphName) + File.separator + cleanParameters(cleanPathSegment(testMethodName));
  }

  public static String cleanPathSegment(final String toClean) {
    String cleaned = toClean.replaceAll("[.\\\\/,{}:*?\"<>|\\[\\]\\(\\)]", "");
    if (cleaned.length() == 0) {
      throw new IllegalStateException("Path segment " + toClean + " has not valid characters and is thus empty");
    } else {
      return cleaned;
    }
  }
}
