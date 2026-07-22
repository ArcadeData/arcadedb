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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.utility.CollectionUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;

import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Gremlin Expression builder.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeGremlin extends ArcadeQuery {
  private static Long timeout;

  protected ArcadeGremlin(final ArcadeGraph graph, final String query) {
    super(graph, query);
  }

  @Override
  public ResultSet execute() {
    if (graph.database instanceof RemoteDatabase)
      return graph.database.command("gremlin", query);

    try {
      final boolean profileExecution = parameters != null && parameters.containsKey("$profileExecution") ?
          (Boolean) parameters.remove("$profileExecution") :
          false;

      final Iterator<?> resultSet = executeStatement();

      ExecutionPlan executionPlan = null;
      if (profileExecution) {
        final String originalQuery = query;
        query += ".profile()";
        try {
          final Iterator<?> profilerResultSet = executeStatement();
          if (profilerResultSet.hasNext()) {
            final Object result = profilerResultSet.next();
            executionPlan = new ExecutionPlan() {
              @Override
              public List<ExecutionStep> getSteps() {
                return null;
              }

              @Override
              public String prettyPrint(int depth, int indent) {
                return result.toString();
              }

              @Override
              public Result toResult() {
                return null;
              }
            };
          }
        } catch (Exception e) {
          // NO EXECUTION PLAN
        } finally {
          query = originalQuery;
        }
      }

      final ExecutionPlan activeExecutionPlan = executionPlan;

      final IteratorResultSet result = new IteratorResultSet(new Iterator() {
        @Override
        public boolean hasNext() {
          return resultSet.hasNext();
        }

        @Override
        public Object next() {
          final Object next = resultSet.next();
          if (next instanceof Document document)
            return new ResultInternal(document);
          else if (next instanceof ArcadeElement<?> element)
            return new ResultInternal(element.getBaseElement());
          else if (next instanceof Map) {
            final Map<String, Object> stringMap = getStringObjectMap((Map<Object, Object>) next);

            return new ResultInternal(stringMap);
          }
          return new ResultInternal(CollectionUtils.singletonMap("result", next));
        }
      }) {
        @Override
        public Optional<ExecutionPlan> getExecutionPlan() {
          return activeExecutionPlan != null ? Optional.of(activeExecutionPlan) : Optional.empty();
        }
      };

      return result;

    } catch (final ScriptException e) {
      // eval() both builds the traversal and, for eager terminal steps such as .next()/.value(), iterates it.
      // A ScriptException can therefore be either a genuine parse/build failure (e.g. a Groovy closure like
      // `filter { ... }` or any syntax the secure gremlin-lang engine rejects, root cause GremlinParserException)
      // or a runtime execution error surfaced during eager iteration (e.g. `.next()` on an empty traversal raises
      // NoSuchElementException). Only the former is a client-side parsing error: it maps to HTTP 400 with the real
      // parser message. A runtime error must stay a CommandExecutionException so it is not misreported as invalid
      // syntax. See issues #5201 (parse) and #5219 (runtime NoSuchElementException misclassified as parse).
      if (isParsingFailure(e))
        throw new CommandParsingException("Error on parsing gremlin query: " + e.getMessage(), e);
      final Throwable root = getRootCause(e);
      final String reason = root.getMessage() != null ? root.getMessage() : root.getClass().getName();
      throw new CommandExecutionException("Error on executing gremlin query: " + reason, e);
    } catch (final Exception e) {
      throw new CommandExecutionException("Error on executing command", e);
    }
  }

  /**
   * Distinguishes a genuine Gremlin parse/compilation failure from a runtime error surfaced by an eager terminal
   * step during eval(). Parse failures from the secure gremlin-lang (java) engine surface as
   * {@code GremlinParserException}; the legacy Groovy engine reports them as a compilation error. Anything else
   * (e.g. {@link java.util.NoSuchElementException} from {@code .next()} on an empty traversal) is a runtime error.
   */
  private static boolean isParsingFailure(final Throwable e) {
    for (Throwable c = e; c != null && c != c.getCause(); c = c.getCause()) {
      final String className = c.getClass().getName();
      if (className.equals("org.apache.tinkerpop.gremlin.language.grammar.GremlinParserException")
          || className.equals("org.codehaus.groovy.control.MultipleCompilationErrorsException")
          || className.equals("org.codehaus.groovy.control.CompilationFailedException"))
        return true;
    }
    return false;
  }

  private static Throwable getRootCause(final Throwable e) {
    Throwable root = e;
    while (root.getCause() != null && root.getCause() != root)
      root = root.getCause();
    return root;
  }

  public static Map<String, Object> getStringObjectMap(final Map<Object, Object> originalMap) {
    // TRANSFORM TO A MAP WITH STRINGS AS KEYS
    final Map<Object, Object> map = originalMap;
    final Map<String, Object> stringMap = new LinkedHashMap<>(originalMap.size());

    for (Map.Entry<Object, Object> entry : map.entrySet())
      stringMap.put(entry.getKey().toString(), entry.getValue());
    return stringMap;
  }

  public QueryEngine.AnalyzedQuery parse() {
    try {
      // ANALYSIS-ONLY: THE ANALYZE() PATH (e.g. HA FOLLOWER IDEMPOTENCY CHECK) DOES NOT RECEIVE THE PARAMETER
      // BINDINGS, SO USE THE NULL-TOLERANT JAVA ENGINE TO BUILD THE TRAVERSAL SHAPE WITHOUT REQUIRING THEM. #5187
      final DefaultGraphTraversal<?,?> resultSet = (DefaultGraphTraversal<?,?>) executeStatement(true);

      boolean idempotent = true;
      final EnumSet<OperationType> ops = EnumSet.noneOf(OperationType.class);
      for (final Object step : resultSet.getSteps()) {
        if (step instanceof Mutating) {
          idempotent = false;
          if (step instanceof AddVertexStep || step instanceof AddVertexStartStep
              || step instanceof AddEdgeStep || step instanceof AddEdgeStartStep)
            ops.add(OperationType.CREATE);
          else if (step instanceof DropStep)
            ops.add(OperationType.DELETE);
          else if (step instanceof AddPropertyStep)
            ops.add(OperationType.UPDATE);
          else {
            // Unknown mutating step: assume all write types
            ops.add(OperationType.CREATE);
            ops.add(OperationType.UPDATE);
            ops.add(OperationType.DELETE);
          }
        }
      }

      if (idempotent)
        ops.add(OperationType.READ);

      final boolean isIdempotent = idempotent;
      final Set<OperationType> operationTypes = Set.copyOf(ops);

      return new QueryEngine.AnalyzedQuery() {
        @Override
        public boolean isIdempotent() {
          return isIdempotent;
        }

        @Override
        public boolean isDDL() {
          return false;
        }

        @Override
        public Set<OperationType> getOperationTypes() {
          return operationTypes;
        }
      };
    } catch (Exception e) {
      throw new CommandParsingException("Error on parsing command", e);
    }
  }

  public Long getTimeout() {
    return timeout;
  }

  public ArcadeQuery setTimeout(final long timeout, final TimeUnit unit) {
    ArcadeGremlin.timeout = unit.toMillis(timeout);
    return this;
  }

  protected String getEffectiveEngine() {
    final BasicDatabase database = graph.getDatabase();
    return database instanceof Database d ?
        d.getConfiguration().getValueAsString(GlobalConfiguration.GREMLIN_ENGINE) :
        GlobalConfiguration.GREMLIN_ENGINE.getValueAsString();
  }

  private Iterator<?> executeStatement() throws ScriptException {
    return executeStatement(false);
  }

  private Iterator<?> executeStatement(final boolean analysis) throws ScriptException {
    String gremlinEngine = getEffectiveEngine();

    if ("auto".equals(gremlinEngine) || "java".equals(gremlinEngine)) {
      // TRY THE NATIVE JAVA ENGINE FIRST
      try {
        return executeStatement("java", analysis);
      } catch (ScriptException e) {
        if ("java".equals(gremlinEngine))
          // STRICT JAVA MODE (THE SECURE DEFAULT): NEVER FALL BACK TO THE INSECURE GROOVY ENGINE, REGARDLESS OF
          // PARAMETERS. THE GROOVY ENGINE IS VULNERABLE TO RCE (SEE GHSA-wcm5-4wjm-9wj3): A QUERY THE GREMLIN-LANG
          // PARSER REJECTS (E.G. A GROOVY CLOSURE `filter { ... }`) MUST SURFACE AS A PARSING ERROR, NOT BE
          // SILENTLY EXECUTED AS GROOVY. USE 'auto' (OR 'groovy') EXPLICITLY TO OPT IN TO THE GROOVY FALLBACK.
          throw e;

        // AUTO MODE ONLY: FALL BACK TO GROOVY FOR COMPATIBILITY (E.G. QUERIES THE gremlin-lang GRAMMAR CANNOT
        // PARSE AFTER TinkerPop 3.8.0 RESTRICTED PARAMETER PLACEMENT). 'auto' IS DOCUMENTED AS NOT RECOMMENDED
        // FOR SECURITY-CRITICAL DEPLOYMENTS.
        LogManager.instance()
            .log(this, Level.FINE, "The gremlin query '%s' could not be parsed by the Java engine, falling back to the `groovy` engine", e, query);
      }
      gremlinEngine = "groovy";
    }

    return executeStatement(gremlinEngine, analysis);
  }

  private Iterator<?> executeStatement(final String gremlinEngine, final boolean analysis) throws ScriptException {
    final Object result;
    if ("java".equals(gremlinEngine)) {
      // USE THE NATIVE GREMLIN PARSER. THE ANALYSIS ENGINE TOLERATES UNBOUND PARAMETERS (#5187).
      final GremlinLangScriptEngine gremlinEngineImpl = analysis ?
          graph.getGremlinJavaAnalysisEngine() :
          graph.getGremlinJavaEngine();

      final SimpleBindings bindings = new SimpleBindings();
      bindings.put("g", graph.traversal());
      if (parameters != null)
        bindings.putAll(parameters);

      result = gremlinEngineImpl.eval(query, bindings);

    } else if ("groovy".equals(gremlinEngine)) {
      // GROOVY ENGINE - INSECURE, LOG WARNING
      LogManager.instance().log(this, Level.WARNING,
          """
          SECURITY WARNING: Using insecure Groovy Gremlin engine. This engine is vulnerable to Remote Code Execution (RCE) attacks. \
          Authenticated users can execute arbitrary operating system commands. DO NOT USE IN PRODUCTION. \
          Switch to the secure Java engine (arcadedb.gremlin.engine=java) immediately.""");

      final GremlinGroovyScriptEngine gremlinEngineImpl = graph.getGremlinGroovyEngine();

      final SimpleBindings bindings = new SimpleBindings();
      bindings.put("g", graph.traversal());
      if (parameters != null)
        bindings.putAll(parameters);
      result = gremlinEngineImpl.eval(query, bindings);

    } else
      throw new IllegalArgumentException("Gremlin engine '" + gremlinEngine + "' not supported");

    if (result instanceof GraphTraversal<?,?> traversal)
      return traversal;

    return Set.of(result).iterator();
  }
}
