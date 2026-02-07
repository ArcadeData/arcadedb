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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BasicCommandContext} to improve coverage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BasicCommandContextTest {

  // --- setVariable / getVariable ---
  @Test
  void setAndGetVariable() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("myVar", 42);
    assertThat(ctx.getVariable("myVar")).isEqualTo(42);
  }

  @Test
  void setVariableWithDollarPrefix() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("$myVar", "hello");
    assertThat(ctx.getVariable("myVar")).isEqualTo("hello");
    assertThat(ctx.getVariable("$myVar")).isEqualTo("hello");
  }

  @Test
  void getVariableNull() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getVariable(null)).isNull();
    assertThat(ctx.getVariable(null, "default")).isEqualTo("default");
  }

  @Test
  void getVariableDefault() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getVariable("nonexistent", "fallback")).isEqualTo("fallback");
  }

  @Test
  void setVariableNullRemoves() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("key", "value");
    assertThat(ctx.getVariable("key")).isEqualTo("value");
    ctx.setVariable("key", null);
    assertThat(ctx.getVariable("key")).isNull();
  }

  @Test
  void setVariableNullNameReturnsNull() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.setVariable(null, "value")).isNull();
  }

  // --- getVariable with special names ---
  @Test
  void getVariableContext() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("test", "val");
    final Object result = ctx.getVariable("CONTEXT");
    assertThat(result).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) result).get("test")).isEqualTo("val");
  }

  @Test
  void getVariableParent() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(parent);
    assertThat(child.getVariable("PARENT")).isSameAs(parent);
  }

  @Test
  void getVariableRoot() {
    final BasicCommandContext root = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(root);
    assertThat(child.getVariable("ROOT")).isSameAs(root);
  }

  @Test
  void getVariableWithDotField() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final ResultInternal result = new ResultInternal();
    result.setProperty("name", "test");
    ctx.setVariable("myResult", result);
    assertThat(ctx.getVariable("myResult.name")).isEqualTo("test");
  }

  // --- getVariablePath ---
  @Test
  void getVariablePathNull() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getVariablePath(null)).isNull();
    assertThat(ctx.getVariablePath(null, "default")).isEqualTo("default");
  }

  @Test
  void getVariablePathContext() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("key", "value");
    final Object result = ctx.getVariablePath("CONTEXT");
    assertThat(result).isInstanceOf(Map.class);
  }

  @Test
  void getVariablePathParentWithSubPath() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setVariable("x", 42);
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(parent);
    assertThat(child.getVariablePath("PARENT.x")).isEqualTo(42);
  }

  @Test
  void getVariablePathParentWithoutSubPath() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(parent);
    assertThat(child.getVariablePath("PARENT")).isSameAs(parent);
  }

  @Test
  void getVariablePathRoot() {
    final BasicCommandContext root = new BasicCommandContext();
    root.setVariable("rootVar", "rootValue");
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(root);
    assertThat(child.getVariablePath("ROOT.rootVar")).isEqualTo("rootValue");
  }

  @Test
  void getVariablePathDollarPrefix() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("myVar", "hello");
    assertThat(ctx.getVariablePath("$myVar")).isEqualTo("hello");
  }

  @Test
  void getVariablePathWithMapNavigation() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final Map<String, Object> map = new HashMap<>();
    map.put("innerKey", "innerValue");
    ctx.setVariable("myMap", map);
    assertThat(ctx.getVariablePath("myMap.innerKey")).isEqualTo("innerValue");
  }

  @Test
  void getVariablePathWithDefault() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getVariablePath("nonexistent", "default")).isEqualTo("default");
  }

  @Test
  void getVariablePathFromChild() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    child.setVariable("childVar", "childValue");
    parent.setChild(child);
    assertThat(parent.getVariablePath("childVar")).isEqualTo("childValue");
  }

  @Test
  void getVariablePathFromParentHierarchy() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setVariable("parentVar", "parentValue");
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(parent);
    // When child doesn't have the variable, it should search parent hierarchy
    assertThat(child.getVariablePath("parentVar")).isEqualTo("parentValue");
  }

  // --- incrementVariable ---
  @Test
  void incrementVariableNew() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.incrementVariable("counter");
    assertThat(ctx.getVariable("counter")).isEqualTo(1);
  }

  @Test
  void incrementVariableExisting() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("counter", 5L);
    ctx.incrementVariable("counter");
    assertThat(ctx.getVariable("counter")).isEqualTo(6L);
  }

  @Test
  void incrementVariableWithDollar() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.incrementVariable("$count");
    assertThat(ctx.getVariable("count")).isEqualTo(1);
  }

  @Test
  void incrementVariableNonNumber() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("str", "hello");
    assertThatThrownBy(() -> ctx.incrementVariable("str"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void incrementVariableNull() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.incrementVariable(null);
    // Should not throw, just ignore
  }

  // --- updateMetric ---
  @Test
  void updateMetricWhenNotRecording() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.updateMetric("testMetric", 100)).isEqualTo(-1);
  }

  @Test
  void updateMetricWhenRecording() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.recordMetrics = true;
    assertThat(ctx.updateMetric("testMetric", 100)).isEqualTo(100);
    assertThat(ctx.updateMetric("testMetric", 50)).isEqualTo(150);
  }

  // --- getVariables ---
  @Test
  void getVariablesEmpty() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getVariables()).isEmpty();
  }

  @Test
  void getVariablesIncludesChild() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setVariable("parentKey", "parentVal");
    final BasicCommandContext child = new BasicCommandContext();
    child.setVariable("childKey", "childVal");
    parent.setChild(child);

    final Map<String, Object> all = parent.getVariables();
    assertThat(all).containsEntry("parentKey", "parentVal");
    assertThat(all).containsEntry("childKey", "childVal");
  }

  // --- setChild / getParent ---
  @Test
  void setChildNull() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    parent.setChild(child);
    assertThat(parent.getVariables()).isEmpty(); // child has no vars
    parent.setChild(null); // remove child
    assertThat(child.getParent()).isNull();
  }

  @Test
  void setChildSameChild() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    parent.setChild(child);
    parent.setChild(child); // should not cause issues
  }

  // --- setParent ---
  @Test
  void setParentSetsUpBidirectional() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    child.setParent(parent);
    assertThat(child.getParent()).isSameAs(parent);
  }

  @Test
  void setParentWithoutOverridingChild() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setParentWithoutOverridingChild(parent);
    assertThat(ctx.getParent()).isSameAs(parent);
  }

  // --- isProfiling / setProfiling ---
  @Test
  void profilingDefault() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.isProfiling()).isFalse();
  }

  @Test
  void profilingEnabled() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setProfiling(true);
    assertThat(ctx.isProfiling()).isTrue();
  }

  @Test
  void profilingInheritsFromParent() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setProfiling(true);
    final BasicCommandContext child = new BasicCommandContext();
    child.setParentWithoutOverridingChild(parent);
    assertThat(child.isProfiling()).isTrue();
  }

  // --- copy ---
  @Test
  void copyContext() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("a", 1);
    ctx.setVariable("b", "hello");
    ctx.setProfiling(true);
    final CommandContext copy = ctx.copy();
    assertThat(copy).isInstanceOf(BasicCommandContext.class);
    assertThat(copy.isProfiling()).isTrue();
  }

  @Test
  void copyEmptyContext() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final CommandContext copy = ctx.copy();
    assertThat(copy).isInstanceOf(BasicCommandContext.class);
  }

  // --- toString ---
  @Test
  void toStringEmpty() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final String result = ctx.toString();
    assertThat(result).isNotNull();
  }

  @Test
  void toStringWithVariables() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("key", "value");
    final String result = ctx.toString();
    assertThat(result).contains("VARIABLES:");
    assertThat(result).contains("key");
  }

  @Test
  void toStringWithInputParams() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setInputParameters(new Object[] { "arg1", 42 });
    final String result = ctx.toString();
    assertThat(result).contains("PARAMETERS:");
  }

  @Test
  void toStringWithChild() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setVariable("pKey", "pVal");
    final BasicCommandContext child = new BasicCommandContext();
    child.setVariable("cKey", "cVal");
    parent.setChild(child);
    final String result = parent.toString();
    assertThat(result).contains("CHILD:");
  }

  @Test
  void toStringWithNullValue() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("nullVar", null);
    // Just verify it doesn't throw
    ctx.toString();
  }

  @Test
  void toStringWithResultSet() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("rs", new InternalResultSet());
    final String result = ctx.toString();
    assertThat(result).contains("resultset");
  }

  // --- inputParameters ---
  @Test
  void setInputParametersMap() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final Map<String, Object> params = new HashMap<>();
    params.put("key1", "val1");
    ctx.setInputParameters(params);
    assertThat(ctx.getInputParameters()).containsEntry("key1", "val1");
  }

  @Test
  void setInputParametersArray() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setInputParameters(new Object[] { "a", "b", "c" });
    assertThat(ctx.getInputParameters()).containsEntry("0", "a");
    assertThat(ctx.getInputParameters()).containsEntry("1", "b");
    assertThat(ctx.getInputParameters()).containsEntry("2", "c");
  }

  @Test
  void setInputParametersNullArray() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setInputParameters((Object[]) null);
    assertThat(ctx.getInputParameters()).isEmpty();
  }

  @Test
  void getInputParametersFromParent() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setInputParameters(new Object[] { "x" });
    final BasicCommandContext child = new BasicCommandContext();
    child.setParentWithoutOverridingChild(parent);
    assertThat(child.getInputParameters()).containsEntry("0", "x");
  }

  @Test
  void getInputParametersNullNoParent() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getInputParameters()).isNull();
  }

  @Test
  void setInputParametersWithProfiling() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final Map<String, Object> params = new HashMap<>();
    params.put("$profileExecution", Boolean.TRUE);
    ctx.setInputParameters(params);
    assertThat(ctx.isProfiling()).isTrue();
  }

  // --- getDatabase ---
  @Test
  void getDatabaseNull() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getDatabase()).isNull();
  }

  @Test
  void getDatabaseFromParent() {
    final BasicCommandContext parent = new BasicCommandContext();
    final BasicCommandContext child = new BasicCommandContext();
    child.setParentWithoutOverridingChild(parent);
    // Both are null, result should be null
    assertThat(child.getDatabase()).isNull();
  }

  // --- declareScriptVariable / isScriptVariableDeclared ---
  @Test
  void declareAndCheckScriptVariable() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.declareScriptVariable("myVar");
    assertThat(ctx.isScriptVariableDeclared("myVar")).isTrue();
    assertThat(ctx.isScriptVariableDeclared("$myVar")).isTrue();
  }

  @Test
  void isScriptVariableDeclaredWithExistingVariable() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("existing", "value");
    assertThat(ctx.isScriptVariableDeclared("existing")).isTrue();
  }

  @Test
  void isScriptVariableDeclaredNullOrEmpty() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.isScriptVariableDeclared(null)).isFalse();
    assertThat(ctx.isScriptVariableDeclared("")).isFalse();
  }

  @Test
  void isScriptVariableDeclaredInParent() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.declareScriptVariable("parentVar");
    final BasicCommandContext child = new BasicCommandContext();
    child.setParentWithoutOverridingChild(parent);
    assertThat(child.isScriptVariableDeclared("parentVar")).isTrue();
  }

  @Test
  void isScriptVariableDeclaredNotFound() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.isScriptVariableDeclared("nonexistent")).isFalse();
  }

  // --- configuration ---
  @Test
  void configurationDefaultNotNull() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getConfiguration()).isNotNull();
  }

  @Test
  void setConfiguration() {
    final BasicCommandContext ctx = new BasicCommandContext();
    final ContextConfiguration config = new ContextConfiguration();
    ctx.setConfiguration(config);
    assertThat(ctx.getConfiguration()).isSameAs(config);
  }

  // --- getContextDeclaredVariable ---
  @Test
  void getContextDeclaredVariable() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setVariable("myVar", "value");
    assertThat(ctx.getContextDeclaredVariable("myVar")).isSameAs(ctx);
  }

  @Test
  void getContextDeclaredVariableFromParent() {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setVariable("parentVar", "pval");
    final BasicCommandContext child = new BasicCommandContext();
    child.setParentWithoutOverridingChild(parent);
    assertThat(child.getContextDeclaredVariable("parentVar")).isSameAs(parent);
  }

  @Test
  void getContextDeclaredVariableNotFound() {
    final BasicCommandContext ctx = new BasicCommandContext();
    assertThat(ctx.getContextDeclaredVariable("nonexistent")).isNull();
  }

  // --- toString with declaredScriptVariables ---
  @Test
  void toStringWithDeclaredScriptVariables() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.declareScriptVariable("scriptVar");
    final String result = ctx.toString();
    assertThat(result).contains("SCRIPT VARIABLES:");
  }
}
