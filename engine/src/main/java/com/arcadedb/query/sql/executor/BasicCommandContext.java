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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map. Supports parent/child context to build a tree
 * of contexts. If a variable is not found on current object the search is applied recursively on child contexts.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class BasicCommandContext implements CommandContext {
  protected       DatabaseInternal    database;
  protected       boolean             recordMetrics           = false;
  protected       CommandContext      parent;
  protected       CommandContext      child;
  protected       Map<String, Object> variables;
  protected       Map<String, Object> inputParameters;
  protected final Set<String>         declaredScriptVariables = new HashSet<>();
  protected final AtomicLong          resultsProcessed        = new AtomicLong(0);

  public Object getVariable(final String name) {
    return getVariable(name, null);
  }

  public Object getVariable(String name, final Object iDefault) {
    if (name == null)
      return iDefault;

    final Object result;

    if (name.startsWith("$"))
      name = name.substring(1);

    final String firstPart;
    firstPart = name;

    if (firstPart.equalsIgnoreCase("CONTEXT"))
      result = getVariables();
    else if (firstPart.equalsIgnoreCase("PARENT"))
      result = parent;
    else if (firstPart.equalsIgnoreCase("ROOT")) {
      CommandContext p = this;
      while (p.getParent() != null)
        p = p.getParent();
      result = p;
    } else {
      if (variables != null && variables.containsKey(firstPart))
        result = variables.get(firstPart);
      else {
        if (child != null)
          result = child.getVariable(firstPart);
        else
          result = getVariableFromParentHierarchy(firstPart);
      }
    }

    return result != null ? result : iDefault;
  }

  protected Object getVariableFromParentHierarchy(final String varName) {
    if (this.variables != null && variables.containsKey(varName)) {
      return variables.get(varName);
    }
    if (parent != null && parent instanceof BasicCommandContext) {
      return ((BasicCommandContext) parent).getVariableFromParentHierarchy(varName);
    }
    return null;
  }

  public CommandContext setVariable(String name, final Object value) {
    if (name == null)
      return null;

    if (name.startsWith("$"))
      name = name.substring(1);

    init();

    if (variables.containsKey(name)) {
      variables.put(name, value);//this is a local existing variable, so it's bound to current context
    } else if (parent != null && parent instanceof BasicCommandContext && ((BasicCommandContext) parent).hasVariable(name)) {
      parent.setVariable(name, value);// it is an existing variable in parent context, so it's bound to parent context
    } else {
      variables.put(name, value); //it's a new variable, so it's created in this context
    }

    return this;
  }

  boolean hasVariable(final String name) {
    if (variables != null && variables.containsKey(name)) {
      return true;
    }
    if (parent != null && parent instanceof BasicCommandContext) {
      return ((BasicCommandContext) parent).hasVariable(name);
    }
    return false;
  }

  @Override
  public CommandContext incrementVariable(String name) {
    if (name != null) {
      if (name.startsWith("$"))
        name = name.substring(1);

      init();

      final Object v = variables.get(name);
      if (v == null)
        variables.put(name, 1);
      else if (v instanceof Number)
        variables.put(name, ((Number) v).longValue() + 1);
      else
        throw new IllegalArgumentException("Variable '" + name + "' is not a number, but: " + v.getClass());
    }
    return this;
  }

  public long updateMetric(final String iName, final long iValue) {
    if (!recordMetrics)
      return -1;

    init();
    Long value = (Long) variables.get(iName);
    if (value == null)
      value = iValue;
    else
      value = value + iValue;
    variables.put(iName, value);
    return value;
  }

  /**
   * Returns a read-only map with all the variables.
   */
  public Map<String, Object> getVariables() {
    final HashMap<String, Object> map = new HashMap<>();
    if (child != null)
      map.putAll(child.getVariables());

    if (variables != null)
      map.putAll(variables);

    return map;
  }

  /**
   * Set the inherited context avoid to copying all the values every time.
   */
  public CommandContext setChild(final CommandContext iContext) {
    if (iContext == null) {
      if (child != null) {
        // REMOVE IT
        child.setParent(null);
        child = null;
      }

    } else if (child != iContext) {
      // ADD IT
      child = iContext;
      iContext.setParent(this);
    }
    return this;
  }

  public CommandContext getParent() {
    return parent;
  }

  public CommandContext setParent(final CommandContext iParentContext) {
    if (parent != iParentContext) {
      parent = iParentContext;
      if (parent != null)
        parent.setChild(this);
    }
    return this;
  }

  public CommandContext setParentWithoutOverridingChild(final CommandContext iParentContext) {
    if (parent != iParentContext) {
      parent = iParentContext;
    }
    return this;
  }

  @Override
  public String toString() {
    return getVariables().toString();
  }

  public boolean isRecordingMetrics() {
    return recordMetrics;
  }

  public CommandContext setRecordingMetrics(final boolean recordMetrics) {
    this.recordMetrics = recordMetrics;
    return this;
  }

  @Override
  public CommandContext copy() {
    final BasicCommandContext copy = new BasicCommandContext();
    copy.init();

    if (variables != null && !variables.isEmpty())
      copy.variables.putAll(variables);

    copy.recordMetrics = recordMetrics;
    copy.parent = parent;
    copy.child = child;
    return copy;
  }

  private void init() {
    if (variables == null)
      variables = new HashMap<>();
  }

  public Map<String, Object> getInputParameters() {
    if (inputParameters != null) {
      return inputParameters;
    }

    return parent == null ? null : parent.getInputParameters();
  }

  public void setInputParameters(final Map<String, Object> inputParameters) {
    this.inputParameters = inputParameters;
  }

  public void setInputParameters(final Object[] args) {
    this.inputParameters = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        this.inputParameters.put(String.valueOf(i), args[i]);
      }
    }
  }

  /**
   * Returns the number of results processed. This is intended to be used with LIMIT in SQL statements
   */
  public AtomicLong getResultsProcessed() {
    return resultsProcessed;
  }

  public DatabaseInternal getDatabase() {
    if (database != null) {
      return database;
    }
    if (parent != null) {
      return parent.getDatabase();
    }
    return null;
  }

  public CommandContext setDatabase(final Database database) {
    this.database = (DatabaseInternal) database;
    return this;
  }

  /**
   * TODO OPTIMIZE THIS
   */
  public static int getLowerIndexOf(final String iText, final int iBeginOffset, final String... iToSearch) {
    int lowest = -1;
    for (final String toSearch : iToSearch) {
      boolean singleQuote = false;
      boolean doubleQuote = false;
      boolean backslash = false;
      for (int i = iBeginOffset; i < iText.length(); i++) {
        if (lowest == -1 || i < lowest) {
          if (backslash && (iText.charAt(i) == '\'' || iText.charAt(i) == '"')) {
            backslash = false;
            continue;
          }
          if (iText.charAt(i) == '\\') {
            backslash = true;
            continue;
          }
          if (iText.charAt(i) == '\'' && !doubleQuote) {
            singleQuote = !singleQuote;
            continue;
          }
          if (iText.charAt(i) == '"' && !singleQuote) {
            doubleQuote = !doubleQuote;
            continue;
          }

          if (!singleQuote && !doubleQuote && iText.startsWith(toSearch, i)) {
            lowest = i;
          }
        }
      }
    }

    return lowest;
  }

  @Override
  public void declareScriptVariable(final String varName) {
    this.declaredScriptVariables.add(varName);
  }

  @Override
  public boolean isScriptVariableDeclared(String varName) {
    if (varName == null || varName.length() == 0) {
      return false;
    }
    String dollarVar = varName;
    if (!dollarVar.startsWith("$")) {
      dollarVar = "$" + varName;
    }
    varName = dollarVar.substring(1);
    if (variables != null && (variables.containsKey(varName) || variables.containsKey(dollarVar))) {
      return true;
    }
    return declaredScriptVariables.contains(varName) || declaredScriptVariables.contains(dollarVar) || (parent != null && parent.isScriptVariableDeclared(
        varName));
  }
}
