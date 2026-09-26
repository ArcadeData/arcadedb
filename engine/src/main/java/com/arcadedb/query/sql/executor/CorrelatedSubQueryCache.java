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
package com.arcadedb.query.sql.executor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.MethodCall;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.query.sql.parser.Statement;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remembers the result of a correlated subquery per distinct binding of the outer variables it reads, so that when
 * many outer rows feed it the same values the subquery runs once per binding instead of once per row (issue #8400).
 * One instance lives for one execution of the enclosing statement.
 * <p>
 * <b>What the key is.</b> A correlated subquery only sees its outer row through the outer {@link CommandContext}: a
 * variable resolved up the context hierarchy ({@code $uf}, the implicit {@code current} a function falls back to), or
 * a variable read on the parent context itself ({@code $parent.uf}, {@code $parent.$current}). Rather than guessing
 * those references from the query text, every run of the subquery executes in a {@link TrackingContext} that records
 * each such read as it happens. The key is the value of every read recorded so far, taken on the outer context.
 * <p>
 * <b>Why that is sound.</b> The set of reads can grow from one run to the next: a branch taken for one row can read a
 * variable another row never reaches. Every cached entry is therefore keyed on the union of all reads seen so far,
 * and the cache is dropped whenever a run adds a new one. On a hit, every variable the producing run read has the
 * value it had then; given a deterministic statement over an unchanged database, the run would take the same path and
 * produce the same rows. The two premises are enforced, not assumed:
 * <ul>
 * <li>{@link #isCacheable(Statement)} admits only read-only statements whose every function and method call is a
 * built-in that answers the same within one execution (no {@code uuid()}, {@code randomInt()}, clock, {@code eval()}
 * or user-defined function);</li>
 * <li>{@link DatabaseInternal#getModificationCount()} is sampled around every lookup and store, so a change made to
 * the database while the caller is still iterating - an update between two {@code next()} calls - drops the
 * cache.</li>
 * </ul>
 * A run that reaches the outer context through a path the tracker cannot follow (a bare {@code $parent} handed to a
 * function, {@code $root}, a write to the outer context) disables the cache for the rest of the execution.
 * <p>
 * <b>Threading.</b> Not thread-safe, by design: {@link #lookup} and {@link #store} are called only by the one thread
 * pulling rows through the owning {@link LetQueryStep}. The one concurrent part is a run's {@link Tracker}, which
 * parallel-scan workers write to through their copies of the run's context, and which is thread-safe.
 */
public final class CorrelatedSubQueryCache {
  /** A subquery result bigger than this is recomputed rather than retained, to keep the cache's heap bounded. */
  static final int MAX_CACHED_ROWS_PER_ENTRY = 1_000;

  /**
   * Built-in functions whose answer can differ between two calls with the same arguments inside one execution, or
   * that evaluate a query string the static check cannot see into. Every other call must resolve to a built-in.
   * Every registered built-in is classified either here or in LetQueryStepCorrelatedResultCacheTest's reviewed list,
   * which fails on a newly registered name until someone decides which side it belongs on.
   */
  static final Set<String> NON_REPEATABLE_FUNCTIONS = Set.of("randomint", "math_random", "uuid", "sysdate", "eval", "promql");
  /**
   * Built-in methods not trusted: {@code remove}/{@code removeAll} change a collection (today on a defensive copy, not
   * relied upon here), and {@code transform} resolves the methods it applies from its string arguments at run time,
   * the same blind spot {@code eval()} is excluded for.
   */
  static final Set<String> UNTRUSTED_METHODS        = Set.of("remove", "removeall", "transform");
  /** Read-only graph traversal methods, resolved at run time as the built-in graph functions of the same name. */
  private static final Set<String> GRAPH_METHODS            = Set.of("out", "in", "both", "oute", "ine", "bothe", "outv", "inv",
      "bothv");

  private static final ClassValue<Field[]> AST_FIELDS = new ClassValue<>() {
    @Override
    protected Field[] computeValue(final Class<?> type) {
      final List<Field> fields = new ArrayList<>();
      for (Class<?> c = type; c != null && SimpleNode.class.isAssignableFrom(c); c = c.getSuperclass())
        for (final Field f : c.getDeclaredFields()) {
          if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive() || f.getType() == String.class)
            continue;
          f.setAccessible(true);
          fields.add(f);
        }
      return fields.toArray(new Field[0]);
    }
  };

  private final int                          maxEntries;
  private final Map<List<Object>, List<Result>> entries;
  private final Set<Dependency>              dependencies = new LinkedHashSet<>();
  private       long                         modificationCount;
  private       boolean                      disabled;
  private       long                         hits;
  private       long                         misses;

  CorrelatedSubQueryCache(final int maxEntries, final long modificationCount) {
    this.maxEntries = maxEntries;
    this.modificationCount = modificationCount;
    this.entries = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<List<Object>, List<Result>> eldest) {
        return size() > CorrelatedSubQueryCache.this.maxEntries;
      }
    };
  }

  /**
   * Returns a cache for one execution of {@code subQuery}, or {@code null} when its result cannot be safely reused:
   * the cache is disabled by configuration, the database does not track modifications, or the subquery or the
   * statement enclosing it (whose own calls run between two rows' subqueries) fails {@link #isCacheable(Statement)}.
   */
  static CorrelatedSubQueryCache create(final Statement subQuery, final Statement enclosingStatement, final DatabaseInternal database,
      final int maxEntries) {
    if (maxEntries <= 0 || database == null)
      return null;
    final long modificationCount = database.getModificationCount();
    if (modificationCount < 0)
      return null;
    if (!isCacheable(subQuery) || (enclosingStatement != null && !isCacheable(enclosingStatement)))
      return null;
    return new CorrelatedSubQueryCache(maxEntries, modificationCount);
  }

  /**
   * Whether a statement's result is a function of its correlated inputs and the database content alone: every
   * statement in it is read-only and every function or method call is a built-in that answers the same within one
   * execution. Walks the whole parsed tree reflectively, so a call nested anywhere (a projection, a WHERE, a
   * FROM-subquery, a nested LET) is seen without every node type having to implement a visitor.
   */
  static boolean isCacheable(final Statement statement) {
    if (statement == null)
      return false;
    Boolean cacheable = statement.resultCacheable;
    if (cacheable == null) {
      cacheable = isCacheable(statement, new IdentityHashMap<>());
      statement.resultCacheable = cacheable;
    }
    return cacheable;
  }

  private static boolean isCacheable(final Object node, final IdentityHashMap<Object, Boolean> visited) {
    if (node instanceof SimpleNode simpleNode) {
      if (visited.put(simpleNode, Boolean.TRUE) != null)
        return true;
      if (!isNodeCacheable(simpleNode))
        return false;
      for (final Field f : AST_FIELDS.get(simpleNode.getClass())) {
        final Object value;
        try {
          value = f.get(simpleNode);
        } catch (final IllegalAccessException e) {
          return false;
        }
        if (value != null && !isCacheable(value, visited))
          return false;
      }
    } else if (node instanceof Collection<?> collection) {
      for (final Object item : collection)
        if (!isCacheable(item, visited))
          return false;
    } else if (node instanceof Map<?, ?> map) {
      for (final Map.Entry<?, ?> entry : map.entrySet())
        if (!isCacheable(entry.getKey(), visited) || !isCacheable(entry.getValue(), visited))
          return false;
    } else if (node instanceof Object[] array) {
      for (final Object item : array)
        if (!isCacheable(item, visited))
          return false;
    }
    return true;
  }

  private static boolean isNodeCacheable(final SimpleNode node) {
    if (node instanceof Statement statement)
      return statement.isIdempotent();

    if (node instanceof FunctionCall call) {
      if (call.getName() == null)
        return false;
      final String name = call.getName().getStringValue().toLowerCase(Locale.ENGLISH);
      if (NON_REPEATABLE_FUNCTIONS.contains(name))
        return false;
      // date() WITHOUT ARGUMENTS READS THE CLOCK, date('2020-01-01', ...) PARSES ITS ARGUMENTS
      if ("date".equals(name) && call.getParams().isEmpty())
        return false;
      // A NAME THE ENGINE DID NOT REGISTER ITSELF IS A USER-DEFINED, LIBRARY OR APPLICATION FUNCTION: ITS BEHAVIOUR IS UNKNOWN
      return DefaultSQLFunctionFactory.getInstance().isBuiltIn(name);
    }

    if (node instanceof MethodCall call) {
      final String name = call.methodName == null ? null : call.methodName.getStringValue().toLowerCase(Locale.ENGLISH);
      if (name == null)
        return false;
      // GRAPH TRAVERSAL METHODS (.out(), .inE(), ...) ARE NOT IN THE METHOD REGISTRY: THEY RESOLVE AS FUNCTIONS. LISTED
      // HERE RATHER THAN READ FROM MethodCall.isCacheable(), WHICH ANSWERS PLAN-CACHEABILITY, NOT PURITY
      if (GRAPH_METHODS.contains(name))
        return true;
      return !UNTRUSTED_METHODS.contains(name) && DefaultSQLMethodFactory.getInstance().isBuiltIn(name);
    }

    return true;
  }

  /**
   * Looks up the result cached for the binding the outer context holds now.
   *
   * @return a fresh list holding the cached rows, or {@code null} on a miss (the caller then runs the subquery in a
   * context from {@link #newContext(CommandContext)} and hands the result to {@link #store})
   */
  List<Result> lookup(final CommandContext outer, final DatabaseInternal database) {
    if (disabled)
      return null;

    final long current = database.getModificationCount();
    if (current != modificationCount) {
      entries.clear();
      modificationCount = current;
    }

    final List<Result> cached = entries.get(keyOf(outer));
    if (cached == null) {
      ++misses;
      return null;
    }
    ++hits;
    // A NEW LIST PER ROW: THE LET VALUE IS EXPOSED AS ROW METADATA AND AS A CONTEXT VARIABLE, BOTH OF WHICH A LATER
    // STEP COULD CHANGE WITHOUT THAT CHANGE REACHING THE ROWS THAT SHARE THE BINDING
    return new ArrayList<>(cached);
  }

  /** A context for one run of the subquery, whose reads of the outer context {@link #store} can key the result on. */
  TrackingContext newContext(final CommandContext outer) {
    final TrackingContext context = new TrackingContext(new Tracker());
    context.setDatabase(outer.getDatabase());
    context.setParentWithoutOverridingChild(outer);
    return context;
  }

  /** Remembers {@code result}, produced by a run in {@code context}, for the binding the outer context holds now. */
  void store(final TrackingContext context, final CommandContext outer, final DatabaseInternal database, final List<Result> result) {
    if (disabled)
      return;

    final Tracker tracker = context.tracker;
    if (tracker.untrackable) {
      // THE RUN REACHED THE OUTER CONTEXT THROUGH A PATH THE TRACKER CANNOT FOLLOW: NO KEY IS PROVABLY COMPLETE
      disabled = true;
      entries.clear();
      return;
    }

    final long current = database.getModificationCount();
    if (current != modificationCount) {
      // THE DATABASE CHANGED WHILE THE SUBQUERY RAN: THE RESULT MAY MIX BOTH STATES, SO IT IS NOT A SAFE ENTRY
      entries.clear();
      modificationCount = current;
      return;
    }

    if (!dependencies.containsAll(tracker.reads)) {
      // A NEW READ: EVERY EXISTING ENTRY WAS KEYED WITHOUT IT, SO NONE OF THEM CAN BE TRUSTED ANY MORE
      dependencies.addAll(tracker.reads);
      entries.clear();
    }

    if (result.size() <= MAX_CACHED_ROWS_PER_ENTRY)
      entries.put(keyOf(outer), new ArrayList<>(result));
  }

  boolean isDisabled() {
    return disabled;
  }

  long getHits() {
    return hits;
  }

  long getMisses() {
    return misses;
  }

  Set<Dependency> getDependencies() {
    return dependencies;
  }

  private List<Object> keyOf(final CommandContext outer) {
    final Object[] values = new Object[dependencies.size()];
    int i = 0;
    for (final Dependency dependency : dependencies)
      values[i++] = dependency.valueIn(outer);
    // Arrays.asList: EQUALITY AND HASH OVER THE ELEMENTS, NULLS INCLUDED
    return Arrays.asList(values);
  }

  /** How a run reached a value of the outer context, and so how to read the same value again for the key. */
  enum Access {
    /** Resolved up the context hierarchy from inside the subquery ({@code $uf}, the implicit {@code current}). */
    HIERARCHY,
    /** {@code $parent.name}: {@link CommandContext#getVariable(String)} on the outer context itself. */
    VARIABLE,
    /** {@link CommandContext#getVariablePath(String)} on the outer context itself. */
    PATH
  }

  record Dependency(Access access, String name) {
    Object valueIn(final CommandContext outer) {
      return switch (access) {
        case HIERARCHY -> {
          final Object value = outer instanceof BasicCommandContext basic ? basic.getVariableFromParentHierarchy(name) : null;
          // THE SAME FALLBACK getVariable() APPLIES WHEN NO CONTEXT HOLDS THE NAME
          yield value != null || outer.getDatabase() == null ? value : outer.getDatabase().getGlobalVariable(name);
        }
        case VARIABLE -> outer.getVariable(name);
        case PATH -> outer.getVariablePath(name);
      };
    }
  }

  /** The reads of one run, shared by the run's context and every copy of it a parallel scan hands to a worker. */
  static final class Tracker {
    final    Set<Dependency> reads = ConcurrentHashMap.newKeySet();
    volatile boolean         untrackable;
  }

  /** The first segment of a variable name or path, without its {@code $}: {@code "$parent.uf"} gives {@code "parent"}. */
  private static String firstSegment(final String name) {
    final String n = name.startsWith("$") ? name.substring(1) : name;
    final int dot = n.indexOf('.');
    return dot > -1 ? n.substring(0, dot) : n;
  }

  private static boolean isParentOrRoot(final String name) {
    if (name == null)
      return false;
    final String first = firstSegment(name);
    return "PARENT".equalsIgnoreCase(first) || "ROOT".equalsIgnoreCase(first);
  }

  /** A name the outer context answers with something other than one variable: itself, an ancestor, or all variables. */
  private static boolean escapesOneVariable(final String name) {
    return name == null || isParentOrRoot(name) || "CONTEXT".equalsIgnoreCase(firstSegment(name));
  }

  /**
   * The context one run of the subquery executes in. It is an ordinary child of the outer context, except that every
   * read which leaves it for the outer context is recorded, and {@link #getParent()} hands out a {@link ParentView}
   * that records the reads made on the parent directly ({@code $parent.name}).
   */
  static final class TrackingContext extends BasicCommandContext {
    private final Tracker    tracker;
    private       ParentView parentView;

    TrackingContext(final Tracker tracker) {
      this.tracker = tracker;
    }

    @Override
    protected Object getVariableFromParentHierarchy(final String name) {
      if (variables == null || !variables.containsKey(name))
        tracker.reads.add(new Dependency(Access.HIERARCHY, name));
      return super.getVariableFromParentHierarchy(name);
    }

    @Override
    public CommandContext getParent() {
      if (parent == null)
        return null;
      if (parentView == null || parentView.outer != parent)
        parentView = new ParentView(parent, tracker);
      return parentView;
    }

    @Override
    public Object getVariable(final String name, final Object defaultValue) {
      // THE SUPERCLASS ANSWERS "$parent" AND "$root" FROM ITS parent FIELD, AROUND getParent(): RESOLVE A BARE
      // "$parent" THROUGH THE VIEW, AND GIVE UP TRACKING ON ANYTHING ELSE THAT WALKS UP
      if (isParentOrRoot(name)) {
        final String n = name.startsWith("$") ? name.substring(1) : name;
        if ("PARENT".equalsIgnoreCase(n))
          return parent != null ? getParent() : defaultValue;
        tracker.untrackable = true;
      }
      return super.getVariable(name, defaultValue);
    }

    @Override
    public Object getVariablePath(final String name, final Object defaultValue) {
      if (isParentOrRoot(name))
        tracker.untrackable = true;
      return super.getVariablePath(name, defaultValue);
    }

    @Override
    public CommandContext getContextDeclaredVariable(final String varName) {
      final CommandContext declaring = super.getContextDeclaredVariable(varName);
      if (declaring != null && declaring != this)
        // THE CALLER MAY WRITE THROUGH IT: A SIDE EFFECT ON THE OUTER CONTEXT A CACHE HIT WOULD SKIP
        tracker.untrackable = true;
      return declaring;
    }

    @Override
    public CommandContext copy() {
      return copyInto(new TrackingContext(tracker));
    }
  }

  /**
   * What {@code $parent} evaluates to inside a tracked run: the outer context, with each variable read on it recorded
   * and anything that could escape the tracker (walking further up, listing or writing variables) flagged instead.
   */
  static final class ParentView implements CommandContext {
    private final CommandContext outer;
    private final Tracker        tracker;

    ParentView(final CommandContext outer, final Tracker tracker) {
      this.outer = outer;
      this.tracker = tracker;
    }

    private void escape() {
      tracker.untrackable = true;
    }

    @Override
    public Object getVariable(final String name) {
      return getVariable(name, null);
    }

    @Override
    public Object getVariable(final String name, final Object defaultValue) {
      if (escapesOneVariable(name))
        escape();
      else
        tracker.reads.add(new Dependency(Access.VARIABLE, name));
      return outer.getVariable(name, defaultValue);
    }

    @Override
    public Object getVariablePath(final String name) {
      return getVariablePath(name, null);
    }

    @Override
    public Object getVariablePath(final String name, final Object defaultValue) {
      if (escapesOneVariable(name))
        escape();
      else
        tracker.reads.add(new Dependency(Access.PATH, name));
      return outer.getVariablePath(name, defaultValue);
    }

    @Override
    public CommandContext setVariable(final String name, final Object value) {
      escape();
      return outer.setVariable(name, value);
    }

    @Override
    public CommandContext incrementVariable(final String name) {
      escape();
      return outer.incrementVariable(name);
    }

    @Override
    public Map<String, Object> getVariables() {
      escape();
      return outer.getVariables();
    }

    @Override
    public CommandContext getParent() {
      escape();
      return outer.getParent();
    }

    @Override
    public CommandContext setParent(final CommandContext parentContext) {
      escape();
      return outer.setParent(parentContext);
    }

    @Override
    public CommandContext setChild(final CommandContext context) {
      escape();
      return outer.setChild(context);
    }

    @Override
    public CommandContext copy() {
      escape();
      return outer.copy();
    }

    @Override
    public CommandContext getContextDeclaredVariable(final String varName) {
      escape();
      return outer.getContextDeclaredVariable(varName);
    }

    @Override
    public void declareScriptVariable(final String varName) {
      escape();
      outer.declareScriptVariable(varName);
    }

    @Override
    public CommandContext setCachedValue(final String key, final Object value) {
      escape();
      return outer.setCachedValue(key, value);
    }

    @Override
    public void setInputParameters(final Map<String, Object> inputParameters) {
      escape();
      outer.setInputParameters(inputParameters);
    }

    @Override
    public void setStatistics(final QueryStatistics statistics) {
      escape();
      outer.setStatistics(statistics);
    }

    @Override
    public void setConfiguration(final ContextConfiguration configuration) {
      escape();
      outer.setConfiguration(configuration);
    }

    @Override
    public CommandContext setProfiling(final boolean profilingEnabled) {
      escape();
      return outer.setProfiling(profilingEnabled);
    }

    @Override
    public void setCommandDeadline(final long deadlineEpochMillis, final String description, final boolean yieldPartialResults) {
      escape();
      outer.setCommandDeadline(deadlineEpochMillis, description, yieldPartialResults);
    }

    // READ-ONLY ACCESSORS WHOSE ANSWER DOES NOT CHANGE FROM ONE OUTER ROW TO THE NEXT

    @Override
    public Object getCachedValue(final String key) {
      return outer.getCachedValue(key);
    }

    @Override
    public long getRegexDeadline() {
      return outer.getRegexDeadline();
    }

    @Override
    public long getCommandTimeout() {
      return outer.getCommandTimeout();
    }

    @Override
    public long getCommandDeadline() {
      return outer.getCommandDeadline();
    }

    @Override
    public String getCommandDeadlineDescription() {
      return outer.getCommandDeadlineDescription();
    }

    @Override
    public boolean isCommandDeadlinePartial() {
      return outer.isCommandDeadlinePartial();
    }

    @Override
    public boolean isProfiling() {
      return outer.isProfiling();
    }

    @Override
    public Map<String, Object> getInputParameters() {
      return outer.getInputParameters();
    }

    @Override
    public DatabaseInternal getDatabase() {
      return outer.getDatabase();
    }

    @Override
    public QueryStatistics getStatistics() {
      return outer.getStatistics();
    }

    @Override
    public boolean isScriptVariableDeclared(final String varName) {
      return outer.isScriptVariableDeclared(varName);
    }

    @Override
    public ContextConfiguration getConfiguration() {
      return outer.getConfiguration();
    }

    @Override
    public String toString() {
      return outer.toString();
    }
  }
}
