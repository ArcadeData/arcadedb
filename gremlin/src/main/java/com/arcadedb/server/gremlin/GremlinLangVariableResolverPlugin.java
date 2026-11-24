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

import com.arcadedb.log.LogManager;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.VariableResolverCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.VariableResolverPlugin;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.Settings.ScriptEngineSettings;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Sets what the variables of a {@code gremlin-lang} script resolve to: the values the request supplied, never a graph or a traversal
 * source.
 * <p>
 * The Gremlin executor evaluates a script against the global bindings of the server plus the ones of the request, and the global
 * bindings hold a graph and a traversal source for every database the Gremlin Server has registered, under the name of the database.
 * Those are not values a script can use: a {@code gremlin-lang} variable stands where a literal does. A script naming one is therefore
 * answered like one naming a variable that does not exist, with the same message, whichever database the name belongs to.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class GremlinLangVariableResolverPlugin extends AbstractGremlinPlugin {
  static final String ENGINE_NAME = ArcadeGremlinAuthorizer.GREMLIN_LANG;

  private static final String                             NAME     = "arcadedb.gremlinLangVariables";
  private static final GremlinLangVariableResolverPlugin INSTANCE = new GremlinLangVariableResolverPlugin();

  private GremlinLangVariableResolverPlugin() {
    super(NAME, Set.of(ENGINE_NAME), new VariableResolverCustomizer(Resolver::new));
  }

  /** The way the Gremlin executor builds a plugin from the {@code scriptEngines} section of the settings. */
  public static GremlinLangVariableResolverPlugin instance() {
    return INSTANCE;
  }

  /**
   * Adds the plugin to the {@code gremlin-lang} engine of the settings, ahead of every other plugin: the engine takes the first
   * variable resolver it is given, so a resolver configured in {@code gremlin-server.yaml} does not replace this one.
   */
  static void registerIn(final Settings settings) {
    if (settings.scriptEngines == null)
      settings.scriptEngines = new HashMap<>();
    final ScriptEngineSettings engine = settings.scriptEngines.computeIfAbsent(ENGINE_NAME, name -> new ScriptEngineSettings());

    final Map<String, Map<String, Object>> plugins = new LinkedHashMap<>();
    plugins.put(GremlinLangVariableResolverPlugin.class.getName(), new HashMap<>());
    if (engine.plugins != null) {
      if (engine.plugins.containsKey(VariableResolverPlugin.class.getName()))
        LogManager.instance().log(GremlinLangVariableResolverPlugin.class, Level.WARNING,
            "The variable resolver configured for '%s' in gremlin-server.yaml is not used: graphs and traversal sources are not variables of a script",
            null, ENGINE_NAME);
      plugins.putAll(engine.plugins);
    }
    engine.plugins = plugins;
  }

  /**
   * Resolves a variable against the bindings of the request, like TinkerPop's own resolver, except that a graph or a traversal source
   * is not a value: it resolves as a variable that is not bound.
   */
  static final class Resolver extends VariableResolver.DirectVariableResolver {
    Resolver(final Map<String, Object> variables) {
      super(variables);
    }

    @Override
    public Object apply(final String name, final GremlinParser.VariableContext context) {
      final Object value = super.apply(name, context);
      if (value instanceof Graph || value instanceof TraversalSource)
        return VariableResolver.NoVariableResolver.instance().apply(name, context);
      return value;
    }
  }
}
