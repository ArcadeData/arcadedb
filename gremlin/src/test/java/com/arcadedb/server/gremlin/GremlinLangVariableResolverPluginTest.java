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
package com.arcadedb.server.gremlin;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.VariableResolverPlugin;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The {@code gremlin-lang} engine the Gremlin Server is configured with resolves the variables of a script against the values of the
 * request: a graph or a traversal source is not one, whatever name it is bound under.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinLangVariableResolverPluginTest {
  private static final String PLUGIN = GremlinLangVariableResolverPlugin.class.getName();

  @Test
  void pluginIsRegisteredFirstOnTheEngineOfDefaultSettings() {
    final Settings settings = new Settings();
    assertThat(settings.scriptEngines).doesNotContainKey("gremlin-lang");

    GremlinLangVariableResolverPlugin.registerIn(settings);

    assertThat(settings.scriptEngines).containsKey("gremlin-groovy").containsKey("gremlin-lang");
    assertThat(settings.scriptEngines.get("gremlin-lang").plugins.keySet()).containsExactly(PLUGIN);
  }

  @Test
  void pluginIsRegisteredWhenTheSettingsHaveNoEngines() {
    final Settings settings = new Settings();
    settings.scriptEngines = null;

    GremlinLangVariableResolverPlugin.registerIn(settings);

    assertThat(settings.scriptEngines.get("gremlin-lang").plugins.keySet()).containsExactly(PLUGIN);
  }

  @Test
  void pluginPrecedesAndKeepsTheOtherPluginsOfTheEngine() {
    final Settings settings = new Settings();
    final Settings.ScriptEngineSettings engine = new Settings.ScriptEngineSettings();
    engine.plugins.put(VariableResolverPlugin.class.getName(), Map.of("resolver", "NullVariableResolver"));
    engine.plugins.put("org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin", new HashMap<>());
    settings.scriptEngines.put("gremlin-lang", engine);

    GremlinLangVariableResolverPlugin.registerIn(settings);

    assertThat(settings.scriptEngines.get("gremlin-lang").plugins.keySet())
        .containsExactly(PLUGIN, VariableResolverPlugin.class.getName(), "org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin");
    assertThat(settings.scriptEngines.get("gremlin-lang").plugins.get(VariableResolverPlugin.class.getName()))
        .containsEntry("resolver", "NullVariableResolver");
  }

  @Test
  void registeringTwiceLeavesOnePlugin() {
    final Settings settings = new Settings();

    GremlinLangVariableResolverPlugin.registerIn(settings);
    GremlinLangVariableResolverPlugin.registerIn(settings);

    assertThat(settings.scriptEngines.get("gremlin-lang").plugins.keySet()).containsExactly(PLUGIN);
  }

  @Test
  void graphAndTraversalSourceAreNotVariablesOfAScript() throws Exception {
    final GremlinExecutor executor = buildExecutor(new Settings());
    try {
      final Throwable graph = catchThrowable(() -> executor.eval("g.V().has('name', registeredGraph)", "gremlin-lang", new HashMap<>()).get());
      final Throwable source = catchThrowable(() -> executor.eval("g.V().has('name', registeredSource)", "gremlin-lang", new HashMap<>()).get());
      final Throwable typed = catchThrowable(() -> executor.eval("g.V().hasLabel(registeredGraph)", "gremlin-lang", new HashMap<>()).get());
      final Throwable missing = catchThrowable(() -> executor.eval("g.V().has('name', missingName)", "gremlin-lang", new HashMap<>()).get());

      assertThat(graph).isInstanceOf(ExecutionException.class).hasStackTraceContaining("No variable found for registeredGraph");
      assertThat(source).hasStackTraceContaining("No variable found for registeredSource");
      // WHERE THE ARGUMENT HAS A TYPE, TOO: THE VALUE IS NEVER HANDED TO THE STEP
      assertThat(typed).hasStackTraceContaining("No variable found for registeredGraph");
      assertThat(missing).hasStackTraceContaining("No variable found for missingName");

      // THE ANSWER FOR A GRAPH, A TRAVERSAL SOURCE AND A NAME NOTHING IS BOUND TO IS THE SAME ONE
      assertThat(rootMessage(graph).replace("registeredGraph", "<name>")).isEqualTo(rootMessage(missing).replace("missingName", "<name>"));
      assertThat(rootMessage(source).replace("registeredSource", "<name>")).isEqualTo(rootMessage(missing).replace("missingName", "<name>"));
    } finally {
      executor.close();
    }
  }

  @Test
  void resolverConfiguredInTheSettingsDoesNotReplaceIt() throws Exception {
    // A RESOLVER THAT ANSWERS EVERY VARIABLE WITH NULL, CONFIGURED FOR THE ENGINE BEFORE THE PLUGIN IS REGISTERED: THE ENGINE TAKES THE FIRST ONE
    final Settings settings = new Settings();
    final Settings.ScriptEngineSettings engine = new Settings.ScriptEngineSettings();
    engine.plugins.put(VariableResolverPlugin.class.getName(), Map.of("resolver", "NullVariableResolver"));
    settings.scriptEngines.put("gremlin-lang", engine);

    final GremlinExecutor executor = buildExecutor(settings);
    try {
      assertThat(catchThrowable(() -> executor.eval("g.V().has('name', registeredGraph)", "gremlin-lang", new HashMap<>()).get()))
          .hasStackTraceContaining("No variable found for registeredGraph");
      assertThat(catchThrowable(() -> executor.eval("g.V().has('name', missingName)", "gremlin-lang", new HashMap<>()).get()))
          .hasStackTraceContaining("No variable found for missingName");
    } finally {
      executor.close();
    }
  }

  @Test
  void valuesOfTheRequestAreVariablesOfAScript() throws Exception {
    final GremlinExecutor executor = buildExecutor(new Settings());
    try {
      final Bindings request = new SimpleBindings(new HashMap<>(Map.of("x", 7, "registeredGraph", "sent")));
      assertThat(executor.eval("g.V().has('name', x)", "gremlin-lang", request).get()).isInstanceOf(Traversal.class);

      // A VALUE THE REQUEST SENT UNDER THE NAME OF A GRAPH REPLACES IT: IT IS WHAT THE SCRIPT READS
      assertThat(executor.eval("g.V().has('name', registeredGraph)", "gremlin-lang", request).get()).isInstanceOf(Traversal.class);
      assertThat(executor.eval("g.V().hasLabel(registeredGraph)", "gremlin-lang", request).get()).isInstanceOf(Traversal.class);
    } finally {
      executor.close();
    }
  }

  private static GremlinExecutor buildExecutor(final Settings settings) {
    GremlinLangVariableResolverPlugin.registerIn(settings);

    final Bindings globals = new SimpleBindings();
    globals.put("g", EmptyGraph.instance().traversal());
    globals.put("registeredGraph", EmptyGraph.instance());
    globals.put("registeredSource", EmptyGraph.instance().traversal());

    return GremlinExecutor.build().globalBindings(globals).addPlugins("gremlin-lang", settings.scriptEngines.get("gremlin-lang").plugins)
        .create();
  }

  private static String rootMessage(final Throwable error) {
    Throwable cause = error;
    while (cause.getCause() != null && cause.getCause() != cause)
      cause = cause.getCause();
    return String.valueOf(cause.getMessage());
  }
}
