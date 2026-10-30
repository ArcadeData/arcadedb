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
package com.arcadedb.function.sql;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10699: #10611 converted the integer arguments of the five vector functions it enumerated, and its test
 * enumerated the same five - so it could not find {@code vector.sparseCreate}, {@code randomInt} and {@code if}, which
 * still read their argument through {@code Number.intValue()} (a fraction truncated, a value past the int range
 * wrapped). An enumeration of the converted sites cannot find the unconverted ones; this scan can.
 * <p>
 * Issue #10931: the scan first matched two source spellings, {@code .intValue()} and {@code Integer.parseInt(}, so a
 * narrowing written any other way ({@code (int) ((Number) arg).longValue()}, a compound assignment, a method reference, a
 * static import) passed it. The set of source spellings is open; the set of things the JVM can do to narrow a value to an
 * int is closed. So the scan reads the COMPILED SQL functions and counts every narrowing instruction ({@code l2i},
 * {@code d2i}, {@code f2i}, {@code i2s}, {@code i2b}, {@code i2c}) and every call (or method reference) of the JDK
 * methods that narrow ({@code Number.intValue/shortValue/byteValue}, {@code Integer/Short/Byte} parsing,
 * {@code Math.toIntExact}, {@code Math.round(float)}), whatever the source looked like.
 * <p>
 * Every occurrence is an offender unless its class is listed below, with the number of occurrences and the reason none
 * of them is a narrowing read of an argument. A new occurrence in a listed class changes its count and fails the scan
 * too, so it gets reviewed. An argument that has to be an int is read through
 * {@link SQLFunctionAbstract#requireWholeIntArgument}, {@link SQLFunctionAbstract#requireIntArgument} (saturating, for a
 * configuration value) or the vector family's {@code parseInteger} / {@code toInt8Code}, all built on {@code NumberUtils}
 * or an explicit range check.
 */
class Issue10699NoNarrowingIntReadsInFunctionsTest {
  private static final String FUNCTIONS = "com/arcadedb/function/sql";

  private static final Set<String> NARROWING_VALUE_METHODS = Set.of("intValue", "shortValue", "byteValue");
  private static final Set<String> UNBOXING                = Set.of("java/lang/Integer.intValue", "java/lang/Short.shortValue",
      "java/lang/Byte.byteValue");
  private static final Set<String> PARSING_OWNERS          = Set.of("java/lang/Integer", "java/lang/Short", "java/lang/Byte");
  private static final Set<String> PARSING_METHODS         = Set.of("parseInt", "parseShort", "parseByte", "valueOf", "decode",
      "parseUnsignedInt");

  /** Relative to {@link #FUNCTIONS}, by outer class: the reviewed occurrences, and why none is a narrowing read of an argument. */
  private static final Map<String, Integer> REVIEWED = new TreeMap<>(Map.ofEntries(
      // the strict helper itself: parses a TEXT argument, and Integer.parseInt refuses what does not fit
      Map.entry("SQLFunctionAbstract", 1),
      // the vector family's strict helpers: parseInteger (#10611, same shape) and toInt8Code, a (byte) after a range check
      Map.entry("vector/SQLFunctionVectorAbstract", 3),
      // the shared option-map reader: a text option parsed by Integer.parseInt, which refuses rather than truncates
      Map.entry("FunctionOptions", 1),
      // narrow a COMPUTED result back to the type of the input (pow(int), sqrt(int), abs(short) after absExact), not an argument
      Map.entry("math/SQLFunctionPow", 2),//
      Map.entry("math/SQLFunctionSquareRoot", 2),//
      Map.entry("math/SQLFunctionAbsoluteValue", 5),
      // sum(): an overflow check of the running long, and the INT result narrowed back once it is known to fit
      Map.entry("math/NumericSum", 2),
      // a list index computed from the size of the collected values, never from an argument
      Map.entry("math/SQLFunctionPercentile", 2),//
      Map.entry("time/SQLFunctionTsPercentile", 2),//
      Map.entry("time/SQLFunctionLead", 1),
      // a capacity or a bound already capped below Integer.MAX_VALUE by Math.min
      Map.entry("time/SQLFunctionMovingAvg", 1),//
      Map.entry("graph/SQLFunctionAstar", 1),//
      Map.entry("vector/SQLFunctionVectorDiscover", 1),
      // a float score of the index turned into the int score of the cursor entry
      Map.entry("text/SQLFunctionSearchFieldsMore", 1),//
      Map.entry("text/SQLFunctionSearchIndexMore", 1),
      // produce quantized codes from values the function computed itself (bits packed into bytes, round() in [0, 255])
      Map.entry("vector/SQLFunctionVectorQuantizeBinary", 1),//
      Map.entry("vector/SQLFunctionVectorQuantizeInt8", 2),
      // hashCode() of a double pair
      Map.entry("geo/LightweightPoint", 2)));

  @Test
  void noSqlFunctionReadsAnIntegerArgumentThroughANarrowingConversion() throws Exception {
    final Path root = classesRoot().resolve(FUNCTIONS);
    assertThat(root).as("the compiled SQL functions").isDirectory();

    final Map<String, Integer> found = new TreeMap<>();
    final Map<String, List<String>> occurrences = new TreeMap<>();
    final List<String> offenders = new ArrayList<>();
    int scanned = 0;
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".class")).sorted().toList()) {
        scanned++;
        final String relative = root.relativize(file).toString().replace('\\', '/');
        final String outerClass = outerClassOf(relative);
        for (final String occurrence : narrowingsIn(file)) {
          found.merge(outerClass, 1, Integer::sum);
          occurrences.computeIfAbsent(outerClass, k -> new ArrayList<>()).add(relative + " " + occurrence);
          if (!REVIEWED.containsKey(outerClass))
            offenders.add(relative + " " + occurrence);
        }
      }
    }

    assertThat(scanned).as("no compiled SQL function found under " + root.toAbsolutePath()).isGreaterThan(100);
    assertThat(offenders)
        .as("SQL functions narrowing a value to an int, which truncates 2.5 to 2 and wraps 3000000000 to a negative int "
            + "(issues #10699, #10931): read the argument through requireWholeIntArgument / requireIntArgument / "
            + "parseInteger instead")
        .isEmpty();
    final List<String> changed = new ArrayList<>();
    for (final Map.Entry<String, Integer> entry : REVIEWED.entrySet())
      if (!entry.getValue().equals(found.get(entry.getKey())))
        changed.add(entry.getKey() + " (reviewed " + entry.getValue() + "): " + occurrences.getOrDefault(entry.getKey(), List.of()));
    assertThat(changed)
        .as("the reviewed occurrences changed: a new narrowing in a reviewed class, or one removed (review it, then update REVIEWED)")
        .isEmpty();
  }

  /** {@code math/SQLFunctionPow$1.class} -> {@code math/SQLFunctionPow}: lambdas and nested classes count for their file. */
  private static String outerClassOf(final String relative) {
    final String name = relative.substring(0, relative.length() - ".class".length());
    final int nested = name.indexOf('$');
    return nested < 0 ? name : name.substring(0, nested);
  }

  private static Path classesRoot() throws URISyntaxException {
    return Path.of(SQLFunctionAbstract.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }

  /** Every narrowing instruction and narrowing JDK call of a class file, as {@code method: what}. */
  private static List<String> narrowingsIn(final Path classFile) throws IOException {
    final List<String> result = new ArrayList<>();
    try (final InputStream in = Files.newInputStream(classFile)) {
      new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitInsn(final int opcode) {
              switch (opcode) {
              case Opcodes.L2I -> result.add(methodName + ": l2i");
              case Opcodes.D2I -> result.add(methodName + ": d2i");
              case Opcodes.F2I -> result.add(methodName + ": f2i");
              case Opcodes.I2S -> result.add(methodName + ": i2s");
              case Opcodes.I2B -> result.add(methodName + ": i2b");
              case Opcodes.I2C -> result.add(methodName + ": i2c");
              default -> {
              }
              }
            }

            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (isNarrowingCall(owner, name, desc))
                result.add(methodName + ": " + owner + "." + name + desc);
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              // A METHOD REFERENCE (Number::intValue) IS A HANDLE PASSED TO THE LAMBDA BOOTSTRAP, NOT A CALL
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && isNarrowingCall(handle.getOwner(), handle.getName(), handle.getDesc()))
                  result.add(methodName + ": " + handle.getOwner() + "::" + handle.getName());
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return result;
  }

  private static boolean isNarrowingCall(final String owner, final String name, final String desc) {
    if (NARROWING_VALUE_METHODS.contains(name) && desc.startsWith("()"))
      // UNBOXING (Integer.intValue(), Short.shortValue(), Byte.byteValue()) LOSES NOTHING: EVERY OTHER OWNER NARROWS
      return !UNBOXING.contains(owner + "." + name);
    if (PARSING_OWNERS.contains(owner) && PARSING_METHODS.contains(name) && desc.startsWith("(Ljava/lang/String;"))
      return true;
    return "java/lang/Math".equals(owner) && ("toIntExact".equals(name) || ("round".equals(name) && desc.equals("(F)I")));
  }
}
