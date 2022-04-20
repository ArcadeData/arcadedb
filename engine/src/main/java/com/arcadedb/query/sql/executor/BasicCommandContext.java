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
import com.arcadedb.database.Document;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map. Supports parent/child context to build a tree
 * of contexts. If a variable is not found on current object the search is applied recursively on child contexts.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class BasicCommandContext implements CommandContext {
  protected DatabaseInternal    database;
  protected boolean             recordMetrics           = false;
  protected CommandContext      parent;
  protected CommandContext      child;
  protected Map<String, Object> variables;
  protected Map<String, Object> inputParameters;
  protected Set<String>         declaredScriptVariables = new HashSet<>();

  protected final AtomicLong resultsProcessed = new AtomicLong(0);

  public BasicCommandContext() {
  }

  public Object getVariable(String iName) {
    return getVariable(iName, null);
  }

  public Object getVariable(String iName, final Object iDefault) {
    if (iName == null)
      return iDefault;

    Object result;

    if (iName.startsWith("$"))
      iName = iName.substring(1);

    int pos = getLowerIndexOf(iName, 0, ".", "[");

    String firstPart;
    String lastPart;
    if (pos > -1) {
      firstPart = iName.substring(0, pos);
      if (iName.charAt(pos) == '.')
        pos++;
      lastPart = iName.substring(pos);
      if (firstPart.equalsIgnoreCase("PARENT") && parent != null) {
        // UP TO THE PARENT
        if (lastPart.startsWith("$"))
          result = parent.getVariable(lastPart.substring(1));
        else
//          result = ODocumentHelper.getFieldValue(parent, lastPart);
          result = parent.getVariable(lastPart);

        return result != null ? result : iDefault;

      } else if (firstPart.equalsIgnoreCase("ROOT")) {
        CommandContext p = this;
        while (p.getParent() != null)
          p = p.getParent();

        if (lastPart.startsWith("$"))
          result = p.getVariable(lastPart.substring(1));
        else
//          result = ODocumentHelper.getFieldValue(p, lastPart, this);
          result = p.getVariable(lastPart);

        return result != null ? result : iDefault;
      }
    } else {
      firstPart = iName;
      lastPart = null;
    }

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

    if (pos > -1) {
      result = ((Document) result).get(lastPart);
    }

    return result != null ? result : iDefault;
  }

  protected Object getVariableFromParentHierarchy(String varName) {
    if (this.variables != null && variables.containsKey(varName)) {
      return variables.get(varName);
    }
    if (parent != null && parent instanceof BasicCommandContext) {
      return ((BasicCommandContext) parent).getVariableFromParentHierarchy(varName);
    }
    return null;
  }

  public CommandContext setVariable(String iName, final Object iValue) {
    if (iName == null)
      return null;

    if (iName.startsWith("$"))
      iName = iName.substring(1);

    init();

    int pos = getHigherIndexOf(iName, 0, ".", "[");
    if (pos > -1) {
      Object nested = getVariable(iName.substring(0, pos));
      if (nested instanceof CommandContext)
        ((CommandContext) nested).setVariable(iName.substring(pos + 1), iValue);
    } else {
      if (variables.containsKey(iName)) {
        variables.put(iName, iValue);//this is a local existing variable, so it's bound to current context
      } else if (parent != null && parent instanceof BasicCommandContext && ((BasicCommandContext) parent).hasVariable(iName)) {
        parent.setVariable(iName, iValue);// it is an existing variable in parent context, so it's bound to parent context
      } else {
        variables.put(iName, iValue); //it's a new variable, so it's created in this context
      }
    }
    return this;
  }

  boolean hasVariable(String iName) {
    if (variables != null && variables.containsKey(iName)) {
      return true;
    }
    if (parent != null && parent instanceof BasicCommandContext) {
      return ((BasicCommandContext) parent).hasVariable(iName);
    }
    return false;
  }

  @Override
  public CommandContext incrementVariable(String iName) {
    if (iName != null) {
      if (iName.startsWith("$"))
        iName = iName.substring(1);

      init();

      int pos = getHigherIndexOf(iName, 0, ".", "[");
      if (pos > -1) {
        Object nested = getVariable(iName.substring(0, pos));
        if (nested instanceof CommandContext)
          ((CommandContext) nested).incrementVariable(iName.substring(pos + 1));
      } else {
        final Object v = variables.get(iName);
        if (v == null)
          variables.put(iName, 1);
        else if (v instanceof Number)
          variables.put(iName, ((Number) v).longValue() + 1);
        else
          throw new IllegalArgumentException("Variable '" + iName + "' is not a number, but: " + v.getClass());
      }
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
  public void beginExecution(final long iTimeout, final TIMEOUT_STRATEGY iStrategy) {
//    if (iTimeout > 0) {
    // MANAGES THE TIMEOUT
    //long executionStartedOn = System.currentTimeMillis();
    //      timeoutStrategy = iStrategy;
//    }
  }

  public boolean checkTimeout() {
//    if (timeoutMs > 0) {
//      if (System.currentTimeMillis() - executionStartedOn > timeoutMs) {
//        // TIMEOUT!
//        switch (timeoutStrategy) {
//        case RETURN:
//          return false;
//        case EXCEPTION:
//          throw new PTimeoutException("Command execution timeout exceed (" + timeoutMs + "ms)");
//        }
//      }
//    } else if (parent != null)
//      // CHECK THE TIMER OF PARENT CONTEXT
//      return parent.checkTimeout();

    //TODO

    return true;
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

  public void setInputParameters(Map<String, Object> inputParameters) {
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

  public static int getLowerIndexOf(final String iText, final int iBeginOffset, final String... iToSearch) {
    int lowest = -1;
    for (String toSearch : iToSearch) {
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

  public static int getHigherIndexOf(final String iText, final int iBeginOffset, final String... iToSearch) {
    int lowest = -1;
    for (String toSearch : iToSearch) {
      int index = iText.indexOf(toSearch, iBeginOffset);
      if (index > -1 && (lowest == -1 || index > lowest))
        lowest = index;
    }
    return lowest;
  }

  @Override
  public void declareScriptVariable(String varName) {
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
