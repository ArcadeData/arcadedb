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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map. Supports parent/child context to build a tree
 * of contexts. If a variable is not found on current object the search is applied recursively on child contexts.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class BasicCommandContext implements CommandContext {
  protected       DatabaseInternal     database;
  protected       boolean              recordMetrics           = false;
  protected       CommandContext       parent;
  protected       CommandContext       child;
  protected       Map<String, Object>  variables;
  protected       Map<String, Object>  inputParameters;
  protected       ContextConfiguration configuration           = new ContextConfiguration();
  protected final Set<String>          declaredScriptVariables = new HashSet<>();
  protected final AtomicLong           resultsProcessed        = new AtomicLong(0);

  @Override
  public Object getVariablePath(final String name) {
    return getVariablePath(name, null);
  }

  @Override
  public Object getVariablePath(String name, final Object iDefault) {
    if (name == null)
      return iDefault;

    Object result;

    int pos = name.indexOf('.');
    String firstPart = pos > -1 ? name.substring(0, pos) : name;
    String otherParts = pos > -1 ? name.substring(pos + 1) : "";

    if (firstPart.startsWith("$"))
      firstPart = firstPart.substring(1);

    if (firstPart.equalsIgnoreCase("CONTEXT"))
      result = getVariables();
    else if (firstPart.equalsIgnoreCase("PARENT")) {
      if (parent != null && !otherParts.isEmpty())
        return parent.getVariablePath(otherParts);
      result = parent;

    } else if (firstPart.equalsIgnoreCase("ROOT")) {
      CommandContext p = this;
      while (p.getParent() != null)
        p = p.getParent();

      if (p != null && !otherParts.isEmpty())
        return p.getVariablePath(otherParts);

      result = p;
    } else {
      if (variables != null && variables.containsKey(firstPart))
        result = variables.get(firstPart);
      else {
        if (child != null)
          result = child.getVariablePath(firstPart);
        else
          result = getVariableFromParentHierarchy(firstPart);
      }

      while (!otherParts.isEmpty()) {
        pos = otherParts.indexOf('.');
        firstPart = pos > -1 ? otherParts.substring(0, pos) : otherParts;
        otherParts = pos > -1 ? otherParts.substring(pos + 1) : "";

        if (result instanceof Result)
          result = ((Result) result).getProperty(firstPart);
        else if (result instanceof Map)
          result = ((Map) result).get(firstPart);
        else if (result instanceof Document)
          result = ((Document) result).get(firstPart);

      }
    }

    return result != null ? result : iDefault;
  }

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

    variables.put(name, value);

    return this;
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
    if (inputParameters != null)
      return inputParameters;

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

  public DatabaseInternal getDatabase() {
    if (database != null)
      return database;

    if (parent != null)
      return parent.getDatabase();

    return null;
  }

  public CommandContext setDatabase(final Database database) {
    this.database = (DatabaseInternal) database;
    return this;
  }

  @Override
  public void declareScriptVariable(final String varName) {
    this.declaredScriptVariables.add(varName);
  }

  @Override
  public boolean isScriptVariableDeclared(String varName) {
    if (varName == null || varName.length() == 0)
      return false;

    String dollarVar = varName;
    if (!dollarVar.startsWith("$"))
      dollarVar = "$" + varName;

    varName = dollarVar.substring(1);
    if (variables != null && (variables.containsKey(varName) || variables.containsKey(dollarVar)))
      return true;

    return declaredScriptVariables.contains(varName) || declaredScriptVariables.contains(dollarVar) || (parent != null && parent.isScriptVariableDeclared(
        varName));
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public void setConfiguration(final ContextConfiguration configuration) {
    this.configuration = configuration;
  }
}
