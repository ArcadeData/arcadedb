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
package com.arcadedb.function;

import com.arcadedb.utility.NumberUtils;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The census of the narrowing conversions on the path a function argument takes to become an int or a long: a fraction that
 * truncates ({@code 2.5} read as 2) and a value that wraps ({@code 3000000000} read as a negative int) are a wrong argument
 * answered as a plausible one.
 * <ul>
 * <li>Issue #10699: #10611 converted the integer arguments of the five vector functions it enumerated, and its test enumerated the
 * same five, so it could not find {@code vector.sparseCreate}, {@code randomInt} and {@code if}. An enumeration of the converted
 * sites cannot find the unconverted ones; a scan can.</li>
 * <li>Issue #10850: the first scan covered {@code function/sql/**} only, so the Cypher-style functions next to it ({@code left},
 * {@code substring}, {@code coll.*}, {@code duration(2.5, 'hours')}) kept reading their arguments through
 * {@code intValue()} / {@code longValue()}.</li>
 * <li>Issue #10931: a scan of source SPELLINGS ({@code .intValue()}, {@code Integer.parseInt(}) is passed by
 * {@code (int) ((Number) arg).longValue()}, a compound assignment, a method reference or a static import. The set of spellings is
 * open; the set of things the JVM can do to narrow a value is closed. So the scan reads the COMPILED classes and records every
 * narrowing instruction ({@code l2i}, {@code d2i}, {@code f2i}, {@code d2l}, {@code f2l}, {@code i2s}, {@code i2b},
 * {@code i2c}) and every call or method reference of the JDK methods that narrow, whatever the source looked like.</li>
 * <li>Issue #11022: the two scans ranged over two directory trees, and neither over the code that performs the check:
 * {@code NumberUtils.toLongWhole} / {@code toIntWhole}, the authority every strict helper delegates to. A narrowing put back
 * there left both green. Narrowing is a property of a data path, not of a directory, so the census now ranges over the path:
 * every function (SQL and Cypher, {@code function/**}), the Cypher function implementations kept in the executor, and
 * {@code NumberUtils}; and it asserts that every strict helper still reaches {@code NumberUtils}. The two scans were one
 * invariant over overlapping trees, with two reviewed lists to drift apart, and are now this one.</li>
 * </ul>
 * Every occurrence is keyed by its class, its method and the narrowing it performs, and is an offender unless {@link #REVIEWED}
 * lists that key with the number of times it occurs and the reason none is a narrowing read of an argument. A count, rather than a
 * key, cannot see one narrowing replaced by another in the same method ({@code Math.toIntExact} by {@code Number.intValue} in
 * {@code toIntWhole} is one occurrence either way), which is the shape of the #11022 mutation. What a census cannot see at all is a
 * check that is deleted around a narrowing it keeps (the whole-number test of {@code toLongWhole}): {@code NumberUtilsTest} pins
 * the behaviour of the authority itself.
 */
class Issue10850NoNarrowingReadsInFunctionsTest {
  private static final String FUNCTIONS        = "com/arcadedb/function";
  private static final String CYPHER_FUNCTIONS = "com/arcadedb/query/opencypher/executor";
  private static final String NUMBER_UTILS     = "com/arcadedb/utility/NumberUtils";

  private static final Set<String> NARROWING_INT_METHODS = Set.of("intValue", "shortValue", "byteValue");
  /** Unboxing loses nothing: {@code Integer.intValue()}, {@code Short.shortValue()}, {@code Byte.byteValue()}. */
  private static final Set<String> LOSSLESS_INT          = Set.of("java/lang/Integer.intValue", "java/lang/Short.shortValue",
      "java/lang/Byte.byteValue", "java/util/concurrent/atomic/AtomicInteger.intValue");
  /** The owners whose {@code longValue()} widens or unboxes: every other owner ({@code Number}, {@code Double}, ...) narrows. */
  private static final Set<String> LOSSLESS_LONG_OWNERS  = Set.of("java/lang/Long", "java/lang/Integer", "java/lang/Short",
      "java/lang/Byte", "java/util/concurrent/atomic/AtomicLong", "java/util/concurrent/atomic/AtomicInteger");
  private static final Set<String> PARSING_OWNERS        = Set.of("java/lang/Integer", "java/lang/Short", "java/lang/Byte");
  private static final Set<String> PARSING_METHODS       = Set.of("parseInt", "parseShort", "parseByte", "valueOf", "decode",
      "parseUnsignedInt");

  /**
   * The strict helpers an argument is read through, each of which must reach {@code NumberUtils.toLongWhole} or
   * {@code toIntWhole}: a helper that stops delegating stops being strict whatever its own instructions are.
   */
  private static final Map<String, List<String>> STRICT_HELPERS = Map.of(//
      "com/arcadedb/function/sql/SQLFunctionAbstract", List.of("requireWholeIntArgument", "requireWholeLongArgument"),
      "com/arcadedb/function/cypher/CypherFunctionHelper", List.of("requireWholeNumberArgument", "requireWholeIntArgument",
          "requireWholeNumberOrTextArgument", "requireWholeIntOrTextArgument"));

  /**
   * Relative to {@code com/arcadedb}, as {@code class#method: narrowing}, lambdas and nested classes counted for their outer class
   * and method: the reviewed occurrences, and why none of them is a narrowing read of an argument.
   */
  private static final Map<String, Integer> REVIEWED = new TreeMap<>(Map.ofEntries(//
      // the strict helper itself: parses a TEXT argument, and Integer.parseInt refuses what does not fit
      Map.entry("function/sql/SQLFunctionAbstract#requireWholeIntArgument: java/lang/Integer.parseInt", 1),
      // the vector family's strict helpers: parseInteger (#10611, same shape) and toInt8Code, a (byte) after a range check
      Map.entry("function/sql/vector/SQLFunctionVectorAbstract#parseInteger: java/lang/Integer.parseInt", 1),
      Map.entry("function/sql/vector/SQLFunctionVectorAbstract#toInt8Code: i2b", 1),
      Map.entry("function/sql/vector/SQLFunctionVectorAbstract#toInt8Code: l2i", 1),
      // the shared option-map reader: a text option parsed by Integer.parseInt, which refuses rather than truncates, and the sign check of a value getInt/getLong already read strictly
      Map.entry("function/sql/FunctionOptions#getInt: java/lang/Integer.parseInt", 1),
      Map.entry("function/sql/FunctionOptions#requireNonNegative: java/lang/Number.longValue", 1),
      // the Cypher strict helpers: (int) of a long tested to fit first; and a StringBuilder capacity capped by Math.min
      Map.entry("function/cypher/CypherFunctionHelper#replaceLiteral: l2i", 1),
      Map.entry("function/cypher/CypherFunctionHelper#requireWholeIntArgument: l2i", 2),
      Map.entry("function/cypher/CypherFunctionHelper#requireWholeIntOrTextArgument: l2i", 2),
      // the authority (issue #11022): every narrowing is behind a range or a whole-number check, or is the saturation the method promises; NumberUtilsTest pins what each one answers
      Map.entry("utility/NumberUtils#parsePositiveInteger: java/lang/Integer.parseInt", 1),
      Map.entry("utility/NumberUtils#saturateToInt: java/lang/Number.longValue", 1),
      Map.entry("utility/NumberUtils#saturateToInt: java/math/BigDecimal.intValue", 1),
      Map.entry("utility/NumberUtils#saturateToInt: java/math/BigInteger.intValue", 1),
      Map.entry("utility/NumberUtils#saturateToInt: l2i", 1),
      Map.entry("utility/NumberUtils#saturateToIntOrNull: java/lang/Integer.parseInt", 1),
      Map.entry("utility/NumberUtils#toIntExact: java/lang/Math.toIntExact", 1),
      Map.entry("utility/NumberUtils#toIntExact: java/lang/Number.longValue", 1),
      Map.entry("utility/NumberUtils#toIntExact: java/math/BigDecimal.intValue", 1),
      Map.entry("utility/NumberUtils#toIntExact: java/math/BigInteger.intValue", 1),
      Map.entry("utility/NumberUtils#toIntWhole: java/lang/Math.toIntExact", 1),
      Map.entry("utility/NumberUtils#toLongWhole: d2l", 1),
      Map.entry("utility/NumberUtils#toLongWhole: java/lang/Number.longValue", 1),
      // narrow a COMPUTED result back to the type of the input (abs(short) after absExact), not an argument
      Map.entry("function/sql/math/SQLFunctionAbsoluteValue#execute: i2b", 1),
      Map.entry("function/sql/math/SQLFunctionAbsoluteValue#execute: i2s", 1),
      Map.entry("function/sql/math/SQLFunctionAbsoluteValue#execute: l2i", 3),
      // the exact power of an integral base answered as an INTEGER once it is known to fit (issue #11047)
      Map.entry("function/sql/math/SQLFunctionPow#integralPow: java/lang/Math.toIntExact", 1),
      // sum(): an overflow check of the running long, and the INT result narrowed back once it is known to fit
      Map.entry("function/sql/math/NumericSum#add: java/lang/Number.longValue", 3),
      Map.entry("function/sql/math/NumericSum#add: l2i", 1),
      Map.entry("function/sql/math/NumericSum#get: l2i", 1),
      // round(value) IS the rounding the function is asked for
      Map.entry("function/math/RoundFunction#execute: java/lang/Math.round", 1),
      // a list index computed from the size of the collected values, never from an argument
      Map.entry("function/sql/math/SQLFunctionPercentile#evaluate: d2i", 2),
      Map.entry("function/sql/time/SQLFunctionTsPercentile#getResult: d2i", 2),
      Map.entry("function/sql/time/SQLFunctionLead#getResult: l2i", 1),
      Map.entry("function/agg/AggPercentiles#execute: d2i", 2),
      Map.entry("function/agg/PercentileContFunction#getAggregatedResult: d2i", 2),
      Map.entry("function/agg/AggStatistics#execute: l2i", 3),
      // a list index computed from the size of the collected values and clamped to it; and a value known to be Long/Integer
      Map.entry("function/agg/PercentileDiscFunction#getAggregatedResult: d2i", 1),
      Map.entry("function/agg/PercentileDiscFunction#getAggregatedResult: java/lang/Number.longValue", 1),
      // an index or a bound read strictly as a long, tested against or clamped to the size of the list before the cast
      Map.entry("function/agg/AggNth#execute: l2i", 1),
      Map.entry("function/agg/AggSlice#execute: l2i", 2),
      Map.entry("function/coll/CollInsert#execute: l2i", 1),
      Map.entry("function/coll/CollRemove#execute: l2i", 1),
      Map.entry("function/path/PathSlice#execute: l2i", 2),
      // the cardinality, refused above Integer.MAX_VALUE before the cast
      Map.entry("function/coll/RangeFunction#execute: l2i", 1),
      // a length or a start read strictly as a long, clamped to the int range a string fits in (past it is past the end)
      Map.entry("function/text/LeftFunction#execute: l2i", 1),
      Map.entry("function/text/RightFunction#execute: l2i", 1),
      Map.entry("function/text/SubstringFunction#startCodePoint: l2i", 1),
      Map.entry("query/opencypher/executor/CypherSubstringFunction#startCodePoint: l2i", 1),
      // a capacity or a bound already capped below Integer.MAX_VALUE by Math.min
      Map.entry("function/sql/time/SQLFunctionMovingAvg#execute: l2i", 1),
      Map.entry("function/sql/graph/SQLFunctionAstar#internalExecute: l2i", 1),
      Map.entry("function/sql/vector/SQLFunctionVectorDiscover#execute: l2i", 1),
      // a float score of the index turned into the int score of the cursor entry
      Map.entry("function/sql/text/SQLFunctionSearchFieldsMore#execute: f2i", 1),
      Map.entry("function/sql/text/SQLFunctionSearchIndexMore#execute: f2i", 1),
      // produce quantized codes from values the function computed itself (bits packed into bytes, round() in [0, 255])
      Map.entry("function/sql/vector/SQLFunctionVectorQuantizeBinary#execute: i2b", 1),
      Map.entry("function/sql/vector/SQLFunctionVectorQuantizeInt8#execute: i2b", 1),
      Map.entry("function/sql/vector/SQLFunctionVectorQuantizeInt8#execute: java/lang/Math.round", 1),
      Map.entry("function/sql/vector/SQLFunctionVectorQuantizeInt8#execute: l2i", 1),
      // a coordinate tested to be a whole number inside the range of its type before the cast
      Map.entry("function/vector/VectorCreateFunction#toBytes: i2b", 1),
      Map.entry("function/vector/VectorCreateFunction#toBytes: l2i", 1),
      Map.entry("function/vector/VectorCreateFunction#toInts: l2i", 1),
      Map.entry("function/vector/VectorCreateFunction#toShorts: i2s", 1),
      Map.entry("function/vector/VectorCreateFunction#toShorts: l2i", 1),
      Map.entry("function/vector/VectorCreateFunction#wholeCoordinate: d2l", 1),
      // the srid, tested to be a whole number in [0, Integer.MAX_VALUE] before the cast
      Map.entry("function/geo/CypherPointFunction#execute: d2i", 1),
      // hashCode() of a double pair
      Map.entry("function/sql/geo/LightweightPoint#hashCode: l2i", 2),
      // the bucket id unpacked from the high bits of an encoded id, not a read of a number
      Map.entry("function/graph/IdFunction#decodeLongToRid: l2i", 1),
      // the canonical key of a value: a double tested to be an exact integer, and a value known to be Long/Integer/Short/Byte
      Map.entry("function/DistinctNumericKey#canonicalDouble: d2l", 1),
      Map.entry("function/DistinctNumericKey#canonicalize: java/lang/Number.longValue", 1),
      // a value already known to be Long/Integer/Short/Byte: widening, nothing to lose
      Map.entry("function/math/AbsFunction#execute: java/lang/Number.longValue", 1),
      Map.entry("function/number/NumberFormat#execute: java/lang/Number.longValue", 1),
      // toInteger()/convert.toInteger(): truncating a FLOAT IS the conversion (toInteger(2.9) is 2 in Neo4j too)
      Map.entry("function/convert/ToIntegerFunction#execute: d2l", 1),
      Map.entry("function/convert/ToIntegerFunction#execute: java/lang/Number.longValue", 1),
      Map.entry("function/convert/ConvertToInteger#execute: d2l", 1),
      Map.entry("function/convert/ConvertToInteger#execute: java/lang/Number.longValue", 1),
      // toBoolean(): FLOAT and DOUBLE are refused before it, the integral value is tested against zero
      Map.entry("function/convert/ToBooleanFunction#execute: java/lang/Number.longValue", 1),
      // facet.searchValues(): the sign check of the limit only, which is then read through requireIntArgument
      Map.entry("function/sql/facet/SQLFunctionFacetSearchValues#execute: java/lang/Number.longValue", 1),
      // year/month/day of a temporal value the engine built: components a LocalDate already holds as ints
      Map.entry("function/temporal/DateConstructorFunction#execute: java/lang/Number.intValue", 3)));

  @Test
  void noFunctionReadsAnArgumentThroughANarrowingConversion() throws Exception {
    final Path classes = classesRoot();
    final List<Path> files = new ArrayList<>(classFiles(classes.resolve(FUNCTIONS)));
    // only the function implementations of the Cypher executor, not the whole executor
    files.addAll(classFiles(classes.resolve(CYPHER_FUNCTIONS)).stream()
        .filter(p -> outerClassOf(classes.relativize(p).toString()).matches(".*/Cypher\\w+Function")).toList());
    // the authority: the strict helpers are all built on it (issue #11022)
    files.addAll(classFiles(classes.resolve(NUMBER_UTILS).getParent()).stream()
        .filter(p -> outerClassOf(classes.relativize(p).toString()).equals(NUMBER_UTILS)).toList());

    final Map<String, Integer> found = new TreeMap<>();
    for (final Path file : files) {
      final String outerClass = outerClassOf(classes.relativize(file).toString()).substring("com/arcadedb/".length());
      for (final String occurrence : narrowingsIn(file))
        found.merge(outerClass + "#" + occurrence, 1, Integer::sum);
    }

    assertThat(files.size()).as("no compiled function found under " + classes.toAbsolutePath()).isGreaterThan(400);
    assertThat(files).as("the scan no longer reaches CypherSubstringFunction, what Cypher resolves substring() to")
        .anyMatch(p -> p.endsWith("CypherSubstringFunction.class"));
    assertThat(files).as("the scan no longer reaches NumberUtils, the authority of the strict helpers")
        .anyMatch(p -> p.endsWith("NumberUtils.class"));

    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, Integer> entry : found.entrySet())
      if (!REVIEWED.containsKey(entry.getKey()))
        offenders.add(entry.getKey() + " x" + entry.getValue());
    assertThat(offenders)
        .as("narrowing conversions that may read a function argument, which truncates 2.5 to 2 and wraps 3000000000 to a negative "
            + "int (issues #10699, #10850, #10931, #11022): read the argument through SQLFunctionAbstract.requireWholeIntArgument / "
            + "requireWholeLongArgument / requireIntArgument or CypherFunctionHelper.requireWholeNumberArgument / "
            + "requireWholeIntArgument instead, or review the occurrence and add it to REVIEWED")
        .isEmpty();

    final List<String> changed = new ArrayList<>();
    for (final Map.Entry<String, Integer> entry : REVIEWED.entrySet())
      if (!entry.getValue().equals(found.get(entry.getKey())))
        changed.add(entry.getKey() + " (reviewed " + entry.getValue() + ", found " + found.get(entry.getKey()) + ")");
    assertThat(changed)
        .as("the reviewed occurrences changed: a new narrowing in a reviewed method, or one removed (review it, then update REVIEWED)")
        .isEmpty();
  }

  /** Each strict helper reaches the authority, directly or through another method of its class (issue #11022). */
  @Test
  void everyStrictHelperDelegatesToNumberUtils() throws Exception {
    final Path classes = classesRoot();
    final String authority = NumberUtils.class.getName().replace('.', '/');
    for (final Map.Entry<String, List<String>> helper : STRICT_HELPERS.entrySet()) {
      final Map<String, Set<String>> calls = callsByMethod(classes.resolve(helper.getKey() + ".class"));
      for (final String method : helper.getValue()) {
        assertThat(calls).as(helper.getKey() + " no longer has " + method).containsKey(method);
        assertThat(reaches(calls, helper.getKey(), method, authority, new HashSet<>()))
            .as(helper.getKey() + "." + method + " no longer reaches NumberUtils.toLongWhole/toIntWhole: it reads its argument some "
                + "other way, which nothing proves strict")
            .isTrue();
      }
    }
  }

  private static boolean reaches(final Map<String, Set<String>> calls, final String owner, final String method, final String authority,
      final Set<String> visited) {
    if (!visited.add(method))
      return false;
    for (final String call : calls.getOrDefault(method, Set.of())) {
      if (call.equals(authority + ".toLongWhole") || call.equals(authority + ".toIntWhole"))
        return true;
      if (call.startsWith(owner + ".") && reaches(calls, owner, call.substring(owner.length() + 1), authority, visited))
        return true;
    }
    return false;
  }

  /** {@code a/b/C$1.class} -> {@code a/b/C}: lambdas and nested classes count for their file. */
  private static String outerClassOf(final String relative) {
    final String name = relative.replace('\\', '/');
    final String withoutExtension = name.endsWith(".class") ? name.substring(0, name.length() - ".class".length()) : name;
    final int nested = withoutExtension.indexOf('$');
    return nested < 0 ? withoutExtension : withoutExtension.substring(0, nested);
  }

  /** {@code lambda$execute$3} -> {@code execute}: a lambda counts for the method it is written in, whatever its number. */
  private static String methodOf(final String methodName) {
    if (methodName.startsWith("lambda$")) {
      final int end = methodName.lastIndexOf('$');
      return end > "lambda$".length() ? methodName.substring("lambda$".length(), end) : methodName;
    }
    return methodName;
  }

  private static Path classesRoot() throws URISyntaxException {
    return Path.of(NumberUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }

  private static List<Path> classFiles(final Path root) throws IOException {
    assertThat(root).as("compiled classes").isDirectory();
    try (final Stream<Path> files = Files.walk(root)) {
      return files.filter(p -> p.toString().endsWith(".class")).sorted().toList();
    }
  }

  /** Every narrowing instruction and narrowing JDK call of a class file, as {@code method: what}. */
  private static List<String> narrowingsIn(final Path classFile) throws IOException {
    final List<String> result = new ArrayList<>();
    try (final InputStream in = Files.newInputStream(classFile)) {
      new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final String method = methodOf(methodName);
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitInsn(final int opcode) {
              switch (opcode) {
              case Opcodes.L2I -> result.add(method + ": l2i");
              case Opcodes.D2I -> result.add(method + ": d2i");
              case Opcodes.F2I -> result.add(method + ": f2i");
              case Opcodes.D2L -> result.add(method + ": d2l");
              case Opcodes.F2L -> result.add(method + ": f2l");
              case Opcodes.I2S -> result.add(method + ": i2s");
              case Opcodes.I2B -> result.add(method + ": i2b");
              case Opcodes.I2C -> result.add(method + ": i2c");
              default -> {
              }
              }
            }

            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (isNarrowingCall(owner, name, desc))
                result.add(method + ": " + owner + "." + name);
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              // A METHOD REFERENCE (Number::intValue) IS A HANDLE PASSED TO THE LAMBDA BOOTSTRAP, NOT A CALL
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && isNarrowingCall(handle.getOwner(), handle.getName(), handle.getDesc()))
                  result.add(method + ": " + handle.getOwner() + "." + handle.getName());
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return result;
  }

  /** The calls of each method of a class file, as {@code owner.name}. */
  private static Map<String, Set<String>> callsByMethod(final Path classFile) throws IOException {
    final Map<String, Set<String>> result = new HashMap<>();
    try (final InputStream in = Files.newInputStream(classFile)) {
      new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final Set<String> calls = result.computeIfAbsent(methodName, k -> new HashSet<>());
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              calls.add(owner + "." + name);
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return result;
  }

  private static boolean isNarrowingCall(final String owner, final String name, final String desc) {
    if (NARROWING_INT_METHODS.contains(name) && desc.startsWith("()"))
      return !LOSSLESS_INT.contains(owner + "." + name);
    if ("longValue".equals(name) && "()J".equals(desc))
      return !LOSSLESS_LONG_OWNERS.contains(owner);
    if (PARSING_OWNERS.contains(owner) && PARSING_METHODS.contains(name) && desc.startsWith("(Ljava/lang/String;"))
      return true;
    return "java/lang/Math".equals(owner) && ("toIntExact".equals(name) || "round".equals(name));
  }
}
