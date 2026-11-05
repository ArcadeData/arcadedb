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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.SourceValue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9772: a tool argument is read through {@code MCPToolUtils}, whose accessors refuse a missing or wrongly shaped member
 * with an {@code MCPToolArgumentException}. A bare {@code args.getString("query")} raises a {@code JSONException} instead, which
 * production mode conceals and logs as a SEVERE server fault.
 * <p>
 * The census ranges over the authority (#11288): the arguments object is the {@code arguments} member of a {@code tools/call}
 * request, and it is followed through every call of the {@code mcp} module that hands it on, so every method it reaches, under any
 * name and in any package, is a method holding it ({@link CompiledClasses}, {@link CallOperands}). A {@code get...}/{@code opt...}
 * read of it outside {@code MCPToolUtils} is an offender. The source-text census it
 * replaces matched a read on a receiver named {@code args}, {@code arguments} or {@code params} in the tools package, so a
 * parameter named otherwise, or a tool written in another package, passed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPToolArgumentReadsCensusTest {
  private static final String MODULE      = "mcp";
  private static final String JSON_OBJECT = "com/arcadedb/serializer/json/JSONObject";
  private static final String UTILS       = "mcp/tools/MCPToolUtils";

  /** The member of a {@code tools/call} request that holds the arguments (MCP specification), and how it is read. */
  private static final String      ARGUMENTS       = "arguments";
  private static final Set<String> ARGUMENTS_READS = Set.of("getJSONObject", "optJSONObject");

  /** Reads of the arguments object that are not a tool reading an argument, each with what they are. */
  private static final Map<String, String> NOT_A_TOOL_ARGUMENT = new TreeMap<>(Map.of(//
      "mcp/MCPDispatcher#formatArgs", "renders every member for the audit log line, each by a key it just listed",//
      "mcp/MCPDispatcher#settingKey", "names the setting to redact in that log line, and catches what a malformed key raises",//
      "mcp/MCPDispatcher#textMember", "a JSON-type-checked read (opt + instanceof), raising nothing"));

  @Test
  void noToolReadsAnArgumentWithTheRawAccessor() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.byModule().get(MODULE);
    assertThat(classes).as("the census must read the " + MODULE + " module").isNotNull();

    final List<CallOperands.Site> calls = CallOperands.of(classes, (owner, name, desc) -> true);
    final Set<String> handedTheArguments = methodParametersHandedTheArguments(calls);
    assertThat(handedTheArguments).as("the census must follow the arguments object into the tools").anyMatch(
        p -> p.startsWith("com/arcadedb/mcp/tools/QueryTool.execute("));

    final Map<String, Set<String>> reads = new TreeMap<>();
    for (final CallOperands.Site site : calls)
      if (JSON_OBJECT.equals(site.call().owner) && (site.call().name.startsWith("get") || site.call().name.startsWith("opt"))
          && isTheArguments(site, site.operand(0), handedTheArguments))
        reads.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(site.call().name);
    assertThat(reads.keySet()).as("the census must reach the accessors every tool reads its arguments through").anyMatch(
        k -> k.startsWith(UTILS + "#"));
    reads.keySet().removeIf(k -> k.startsWith(UTILS + "#"));

    assertThat(CompiledClasses.unreviewed(reads, NOT_A_TOOL_ARGUMENT)).as(
        "a tool argument read with the raw JSONObject accessor raises a JSONException, concealed and logged as a server fault: read "
            + "it through MCPToolUtils (requireString, optionalString, intArgument, ...) (issue #9772)").isEmpty();
    assertThat(CompiledClasses.stale(reads, NOT_A_TOOL_ARGUMENT)).as("entries that no longer read the arguments: remove them")
        .isEmpty();
  }

  /**
   * Every parameter, as {@code owner.name(desc)#index}, that the arguments object reaches: the {@code arguments} member of a
   * {@code tools/call} request, then every parameter of a method of the module it is passed to, to a fixpoint.
   */
  private static Set<String> methodParametersHandedTheArguments(final List<CallOperands.Site> calls) {
    final Set<String> handed = new HashSet<>();
    boolean grew = true;
    while (grew) {
      grew = false;
      for (final CallOperands.Site site : calls) {
        final Type[] arguments = Type.getArgumentTypes(site.call().desc);
        for (int i = 0; i < arguments.length; i++)
          if (isTheArguments(site, site.argument(i), handed) && handed.add(
              site.call().owner + "." + site.call().name + site.call().desc + "#" + i))
            grew = true;
      }
    }
    return handed;
  }

  /** Whether {@code origins} is the arguments object: the {@code arguments} member read off a request, or a parameter handed it. */
  private static boolean isTheArguments(final CallOperands.Site site, final Set<AbstractInsnNode> origins, final Set<String> handed) {
    for (final AbstractInsnNode origin : origins) {
      if (origin instanceof MethodInsnNode read && JSON_OBJECT.equals(read.owner) && ARGUMENTS_READS.contains(read.name)
          && ARGUMENTS.equals(argumentMember(site, read)))
        return true;
      final int parameter = site.parameterOf(origin);
      if (parameter >= 0 && handed.contains(site.owner().name + "." + site.method().name + site.method().desc + "#" + parameter))
        return true;
    }
    return false;
  }

  /** The member name a {@code getJSONObject}/{@code optJSONObject} call reads, when it is a constant. */
  private static String argumentMember(final CallOperands.Site site, final MethodInsnNode read) {
    final Frame<SourceValue> frame = site.frames()[site.method().instructions.indexOf(read)];
    final int operands = Type.getArgumentTypes(read.desc).length;
    return CallOperands.stringConstant(site.origins(frame.getStack(frame.getStackSize() - operands)));
  }
}
