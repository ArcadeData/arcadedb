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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
 * request read by the {@code mcp} module, and it is followed through every call and every lambda that hands it on, so every
 * method it reaches, under any name, in any package and in any module, is a method holding it ({@link CompiledClasses},
 * {@link CallOperands}). Issue #11622 widened both ends of that walk:
 * <ul>
 *   <li>a member taken out of the arguments, an array or an object, raises the same concealed {@code JSONException} when one of
 *   its elements is read with a raw accessor ({@code array.getString(i)} on a number), so a value DERIVED from the arguments
 *   (a {@code get}/{@code opt} of a {@code JSONObject} or {@code JSONArray} off it, or an {@code MCPToolUtils} accessor's
 *   result) is followed like the arguments themselves, and a raw read of it is an offender too;</li>
 *   <li>the arguments handed to a method of another module (the engine's argument validators) are followed there, where a raw read
 *   is an offender unless it is reviewed below with what makes it safe.</li>
 * </ul>
 * A {@code get...}/{@code opt...} read of any of them outside {@code MCPToolUtils} is an offender.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPToolArgumentReadsCensusTest {
  private static final String MODULE      = "mcp";
  private static final String JSON_OBJECT = "com/arcadedb/serializer/json/JSONObject";
  private static final String JSON_ARRAY  = "com/arcadedb/serializer/json/JSONArray";
  private static final String UTILS       = "mcp/tools/MCPToolUtils";
  private static final String UTILS_CLASS = "com/arcadedb/" + UTILS;
  private static final String OBJECT      = "java/lang/Object";
  private static final String ARCADEDB_PACKAGE = "com/arcadedb/";

  /** The member of a {@code tools/call} request that holds the arguments (MCP specification), and how it is read. */
  private static final String      ARGUMENTS       = "arguments";
  private static final Set<String> ARGUMENTS_READS = Set.of("getJSONObject", "optJSONObject");

  /** How deep a derived value is followed back to the arguments inside one method: a member of a member of a member. */
  private static final int MAX_DERIVATION_DEPTH = 8;

  /**
   * The engine's search argument readers, shared by the HTTP, gRPC and MCP surfaces, which the MCP tools hand the arguments to.
   * Each tool runs the class's {@code validateArguments} first, through {@code MCPToolUtils.validateArguments}, which answers a
   * {@code JSONException} as an {@code MCPToolArgumentException}; the search itself runs the same validator again before it reads.
   */
  private static final String VALIDATOR = "a schema-free validator of the engine, run by the tool through MCPToolUtils.validateArguments, "
      + "which answers a JSONException as an MCPToolArgumentException";
  private static final String VALIDATED = "runs after the tool ran the validator of its class through MCPToolUtils.validateArguments, "
      + "and reads only members that validator checked the shape of (issue #11622 moved the full-text leg, the index address "
      + "and expand.edgeTypes into it)";
  private static final String TYPE_CHECKED = "a JSON-type-checked read (opt or get, then instanceof), raising nothing";

  /** Reads of the arguments object, or of a value derived from it, that are not a tool reading an argument raw. */
  private static final Map<String, String> NOT_A_TOOL_ARGUMENT = new TreeMap<>(Map.ofEntries(//
      Map.entry("mcp/MCPDispatcher#formatArgs", "renders every member for the audit log line, each by a key it just listed"),//
      Map.entry("mcp/MCPDispatcher#settingKey", "names the setting to redact in that log line, and catches what a malformed key raises"),//
      Map.entry("mcp/MCPDispatcher#textMember", TYPE_CHECKED),//
      Map.entry("query/search/FullTextQuery#validateArguments", VALIDATOR),//
      Map.entry("query/search/FullTextQuery#requireText", TYPE_CHECKED),//
      Map.entry("query/search/FullTextQuery#search", VALIDATED),//
      Map.entry("query/search/FullTextQuery#resolveIndexes", VALIDATED),//
      Map.entry("query/search/HybridSearch#validateArguments", VALIDATOR),//
      Map.entry("query/search/HybridSearch#validateExpand", VALIDATOR),//
      Map.entry("query/search/HybridSearch#validateWeights", VALIDATOR),//
      Map.entry("query/search/HybridSearch#strategyOf", VALIDATOR),//
      Map.entry("query/search/HybridSearch#requireExpandObject", TYPE_CHECKED),//
      Map.entry("query/search/HybridSearch#textMember", TYPE_CHECKED),//
      Map.entry("query/search/HybridSearch#edgeTypeNames", TYPE_CHECKED),//
      Map.entry("query/search/HybridSearch#search", VALIDATED),//
      Map.entry("query/search/HybridSearch#runExpansionLeg", VALIDATED),//
      Map.entry("query/search/HybridSearch#runFullTextLeg", VALIDATED),//
      Map.entry("query/search/HybridSearch#weightOf", VALIDATED),//
      Map.entry("query/search/VectorArgs#requireString", VALIDATOR),//
      Map.entry("query/search/VectorLeg#validateArguments", VALIDATOR),//
      Map.entry("query/search/VectorLeg#readFloatArray", VALIDATOR),//
      Map.entry("query/search/VectorLeg#buildSparseQuery", VALIDATOR),//
      Map.entry("query/search/VectorLeg#build", VALIDATED),//
      Map.entry("query/search/VectorSearch#validateArguments", VALIDATOR),//
      Map.entry("query/search/VectorSearch#search", VALIDATED)));

  @Test
  void noToolReadsAnArgumentWithTheRawAccessor() throws Exception {
    final Map<String, Map<String, byte[]>> modules = CompiledClasses.byModule();
    assertThat(modules.get(MODULE)).as("the census must read the " + MODULE + " module").isNotNull();

    final Scan scan = new Scan(modules.get(MODULE).keySet(), CompiledClasses.ofEveryModule());
    assertThat(scan.handed).as("the census must follow the arguments object into the tools").anyMatch(
        p -> p.startsWith("com/arcadedb/mcp/tools/QueryTool.execute("));
    assertThat(scan.handed).as("the census must follow the arguments object out of the mcp module, into the engine's validators")
        .anyMatch(p -> p.startsWith("com/arcadedb/query/") && p.contains(".validateArguments("));
    assertThat(scan.reads.keySet()).as("the census must reach the accessors every tool reads its arguments through").anyMatch(
        k -> k.startsWith(UTILS + "#"));
    assertThat(scan.reads.entrySet()).as("the census must reach the element reads of an array taken out of the arguments")
        .anyMatch(e -> e.getKey().startsWith(UTILS + "#") && e.getValue().contains(JSON_ARRAY + ".get"));

    final Map<String, Set<String>> reads = new TreeMap<>(scan.reads);
    reads.keySet().removeIf(k -> k.startsWith(UTILS + "#"));

    assertThat(CompiledClasses.unreviewed(reads, NOT_A_TOOL_ARGUMENT)).as(
        "a tool argument, or an element of an array or object taken out of the arguments, read with the raw JSONObject/JSONArray "
            + "accessor raises a JSONException, concealed and logged as a server fault: read it through MCPToolUtils (requireString, "
            + "optionalString, intArgument, optionalStringArray, ...) (issues #9772, #11622)").isEmpty();
    assertThat(CompiledClasses.stale(reads, NOT_A_TOOL_ARGUMENT)).as("entries that no longer read the arguments: remove them")
        .isEmpty();
  }

  @Test
  void theScanSeesADerivedArrayALambdaAndAHandOff() throws Exception {
    // control: the shapes the census relies on, compiled by the same javac
    final Map<String, byte[]> probes = CompiledClasses.ofTheProbes();
    final Scan scan = new Scan(Set.of("com/arcadedb/census/MCPToolArgumentReadsCensusTest$Probe.class"), probes);
    assertThat(scan.reads.keySet()).contains("census/MCPToolArgumentReadsCensusTest$Probe#tool",
        "census/MCPToolArgumentReadsCensusTest$Probe#elementRead", "census/MCPToolArgumentReadsCensusTest$Probe#captured",
        "census/MCPToolArgumentReadsCensusTest$Probe#memberOfAMember", "census/MCPToolArgumentReadsCensusTest$Probe#helperReturned",
        "census/MCPToolArgumentReadsCensusTest$Probe#consume", "census/MCPToolArgumentReadsCensusTest$Probe#returnedAsObject");
    assertThat(scan.reads.keySet()).doesNotContain("census/MCPToolArgumentReadsCensusTest$Probe#notTheArguments");
  }

  /** The arguments object and the values derived from it, followed from the roots through {@code scanned}, and their raw reads. */
  private static final class Scan {
    private final Set<String>              rootClasses;
    /** Every parameter, as {@code owner.name(desc)#index}, that the arguments object or a value derived from it reaches. */
    private final Set<String>              handed = new HashSet<>();
    /** The raw reads of them, by the method they are in. */
    private final Map<String, Set<String>> reads  = new TreeMap<>();
    private final Map<String, byte[]>      scanned;
    /** The analysed declaration of each method whose returned value is asked about, null when the scan does not declare it. */
    private final Map<String, CallOperands.Method> declarations = new HashMap<>();

    /**
     * A worklist over the methods the arguments reach, so only those are analysed: it starts at the methods of the root classes that
     * read the {@code arguments} member, and a method is (re)visited whenever one more of its parameters is handed them. Every
     * call and every lambda of a visited method is looked at, whatever the types it declares, so a hand-off through an
     * {@code Object} parameter ({@code consume(args)}, then a cast) is followed as well.
     */
    private Scan(final Set<String> rootClasses, final Map<String, byte[]> scanned) {
      this.rootClasses = rootClasses;
      this.scanned = scanned;

      final Map<String, byte[]> roots = new HashMap<>();
      for (final String rootClass : rootClasses)
        if (scanned.containsKey(rootClass))
          roots.put(rootClass, scanned.get(rootClass));

      final Deque<CallOperands.Method> pending = new ArrayDeque<>();
      final Set<CallOperands.Method> queued = new HashSet<>();
      for (final CallOperands.Site site : CallOperands.of(roots, (owner, name, desc) -> JSON_OBJECT.equals(owner) && ARGUMENTS_READS.contains(name)))
        if (queued.add(site.in()))
          pending.add(site.in());

      while (!pending.isEmpty()) {
        final CallOperands.Method method = pending.poll();
        queued.remove(method);
        for (final AbstractInsnNode insn : method.node().instructions) {
          if (insn instanceof MethodInsnNode call) {
            final CallOperands.Site site = new CallOperands.Site(method, call);
            if (isRawRead(call.owner, call.name)) {
              if (isTainted(method, site.operand(0), 0))
                reads.computeIfAbsent(method.key(), k -> new TreeSet<>()).add(call.owner + "." + call.name);
            } else if (!isJson(call.owner)) {
              final int arguments = Type.getArgumentTypes(call.desc).length;
              for (int i = 0; i < arguments; i++)
                if (isTainted(method, site.argument(i), 0) && handed.add(parameter(call.owner, call.name, call.desc, i)))
                  visit(declarationOf(call.owner, call.name, call.desc), pending, queued);
            }
          } else if (insn instanceof InvokeDynamicInsnNode indy) {
            final CallOperands.Capture capture = CallOperands.Capture.of(method, indy);
            if (capture == null)
              continue;
            final Handle implementation = capture.implementation();
            final int receiver = capture.implementedByInstanceMethod() ? 1 : 0;
            if (isRawRead(implementation.getOwner(), implementation.getName())) {
              // A METHOD REFERENCE OF A RAW ACCESSOR BOUND TO THEM (args::getString) READS THEM TOO
              if (receiver == 1 && isTainted(method, capture.captured(0), 0))
                reads.computeIfAbsent(method.key(), k -> new TreeSet<>())
                    .add(implementation.getOwner() + "." + implementation.getName());
            } else if (!isJson(implementation.getOwner()))
              for (int i = receiver; i < capture.capturedCount(); i++)
                if (isTainted(method, capture.captured(i), 0) && handed.add(parameter(implementation.getOwner(),
                    implementation.getName(), implementation.getDesc(), i - receiver)))
                  visit(declarationOf(implementation.getOwner(), implementation.getName(), implementation.getDesc()), pending,
                      queued);
          }
        }
      }
    }

    private static void visit(final CallOperands.Method method, final Deque<CallOperands.Method> pending,
        final Set<CallOperands.Method> queued) {
      if (method != null && queued.add(method))
        pending.add(method);
    }

    /**
     * Whether {@code origins} is the arguments object or a value derived from it: the {@code arguments} member read off a request
     * in a root class, a parameter handed one, a {@code get}/{@code opt} of a JSON object or array off one, or what an
     * {@code MCPToolUtils} accessor returns for one.
     */
    private boolean isTainted(final CallOperands.Method method, final Set<AbstractInsnNode> origins, final int depth) {
      if (depth > MAX_DERIVATION_DEPTH)
        return false;
      for (final AbstractInsnNode origin : origins) {
        final int parameter = method.parameterOf(origin);
        if (parameter >= 0 && handed.contains(parameter(method.owner().name, method.node().name, method.node().desc, parameter)))
          return true;
        if (!(origin instanceof MethodInsnNode call))
          continue;

        final int argumentCount = Type.getArgumentTypes(call.desc).length;
        if (JSON_OBJECT.equals(call.owner) && ARGUMENTS_READS.contains(call.name) && rootClasses.contains(
            method.owner().name + ".class") && ARGUMENTS.equals(
            CallOperands.stringConstant(method.stackBefore(call, argumentCount - 1))))
          return true;

        final Type returnType = Type.getReturnType(call.desc);
        if (returnType.getSort() != Type.OBJECT)
          continue;
        final String returned = returnType.getInternalName();
        // A MEMBER OF THEM: get/opt answer an Object (opt("expand") then cast) as well as a JSONObject or a JSONArray
        if (isRawRead(call.owner, call.name) && (isJson(returned) || OBJECT.equals(returned)) && isTainted(method,
            method.stackBefore(call, argumentCount), depth + 1))
          return true;
        if (!(isJson(returned) || OBJECT.equals(returned)) || isJson(call.owner))
          continue;
        // WHAT AN MCPToolUtils ACCESSOR ANSWERS FOR THEM
        if (UTILS_CLASS.equals(call.owner) && call.getOpcode() == Opcodes.INVOKESTATIC && isJson(returned))
          for (int i = 0; i < argumentCount; i++)
            if (isTainted(method, method.stackBefore(call, argumentCount - 1 - i), depth + 1))
              return true;
        // WHAT ANY OTHER METHOD HANDED THEM RETURNS OF THEM (requireExpandObject(args)), READ FROM ITS OWN BODY: a helper typed to
        // answer an Object (identity(args), then cast) included
        if (!call.owner.startsWith(ARCADEDB_PACKAGE))
          continue;
        final CallOperands.Method declaration = declarationOf(call.owner, call.name, call.desc);
        if (declaration != null && isTainted(declaration, declaration.returned(), depth + 1))
          return true;
      }
      return false;
    }

    /** The analysed method {@code owner} declares with that name and descriptor, null when the scan does not declare it. */
    private CallOperands.Method declarationOf(final String owner, final String name, final String desc) {
      final String key = owner + "." + name + desc;
      if (declarations.containsKey(key))
        return declarations.get(key);
      final CallOperands.Method declaration = owner.startsWith("[") ? null :
          CallOperands.declarationOf(scanned, new MethodInsnNode(Opcodes.INVOKESTATIC, owner, name, desc, false));
      declarations.put(key, declaration);
      return declaration;
    }

    private static boolean isJson(final String internalName) {
      return JSON_OBJECT.equals(internalName) || JSON_ARRAY.equals(internalName);
    }

    private static boolean isRawRead(final String owner, final String name) {
      return isJson(owner) && (name.startsWith("get") || name.startsWith("opt"));
    }

    private static String parameter(final String owner, final String name, final String desc, final int index) {
      return owner + "." + name + desc + "#" + index;
    }
  }

  /** The shapes the census must see, read by {@link #theScanSeesADerivedArrayALambdaAndAHandOff}. */
  @SuppressWarnings("unused")
  static final class Probe {
    static void dispatch(final JSONObject request) {
      final JSONObject args = request.getJSONObject(ARGUMENTS);
      tool(args);
      captured(args);
      memberOfAMember(args);
      helperReturned(args);
      handedAsObject(args);
      returnedAsObject(args);
      notTheArguments(request.getJSONObject("params"));
    }

    // A RAW READ, AND AN ARRAY TAKEN OUT OF THE ARGUMENTS HANDED TO ANOTHER METHOD
    static void tool(final JSONObject args) {
      args.getString("query");
      elementRead(args.getJSONArray("names", null));
    }

    static void elementRead(final JSONArray names) {
      names.getString(0);
    }

    // A LAMBDA CAPTURING THE ARGUMENTS
    static void captured(final JSONObject args) {
      final Runnable reader = () -> args.getInt("n");
      reader.run();
    }

    // THE ARGUMENTS HANDED TO A HELPER TYPED TO TAKE AN OBJECT, READ AFTER A CAST
    static void handedAsObject(final JSONObject args) {
      consume(args);
    }

    private static void consume(final Object value) {
      ((JSONObject) value).getInt("n");
    }

    // THE ARGUMENTS ANSWERED BY A HELPER TYPED TO RETURN AN OBJECT, READ BY ITS CALLER AFTER A CAST
    static void returnedAsObject(final JSONObject args) {
      ((JSONObject) identity(args)).getString("x");
    }

    private static Object identity(final JSONObject value) {
      return value;
    }

    // A MEMBER TAKEN OUT BY A HELPER, THEN READ BY ITS CALLER
    static void helperReturned(final JSONObject args) {
      memberOf(args).getString("x");
    }

    private static JSONObject memberOf(final JSONObject args) {
      return args.get("m") instanceof JSONObject member ? member : null;
    }

    static void memberOfAMember(final JSONObject args) {
      final JSONObject inner = args.getJSONObject("a", null);
      inner.getJSONArray("b").getLong(0);
    }

    // A JSON OBJECT THAT IS NOT THE ARGUMENTS
    static void notTheArguments(final JSONObject other) {
      other.getString("x");
    }
  }
}
