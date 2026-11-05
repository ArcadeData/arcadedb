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

import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.arcadedb.census.SchemaPresentationReadableWalkCensusTest.classRootsOfEveryModule;
import static com.arcadedb.census.SchemaPresentationReadableWalkCensusTest.methodOf;
import static com.arcadedb.census.SchemaPresentationReadableWalkCensusTest.outerClassOf;
import static com.arcadedb.census.SchemaPresentationReadableWalkCensusTest.readClasses;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every refusal raised as a {@link ValidationException} names the constraint it violates, or is listed here with why it names
 * none (issue #11314). A protocol answers the kind with its own code - the Postgres wire {@code 23502} for a NOT NULL violation,
 * {@code 22001} for a value over its maximum length, Bolt {@code ConstraintValidationFailed}, MongoDB
 * {@code DocumentValidationFailure} - and a refusal raised without one falls back to the generic invalid value of its
 * category, which is how every constraint answered {@code 22023} before.
 * <p>
 * The census ranges over the authority, the constructors of {@link ValidationException}: it reads the COMPILED classes of every
 * module and records each construction (a {@code new}, or a constructor reference) that does not pass a
 * {@link ValidationException.Kind}, keyed by the class and the method it is written in. A construction cannot hide behind a
 * helper method, a static import or a line break, wherever its file sits.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ValidationExceptionKindCensusTest {
  private static final String AUTHORITY = "com/arcadedb/exception/ValidationException";
  private static final String WITH_KIND = "(Lcom/arcadedb/exception/ValidationException$Kind;Ljava/lang/String;)V";

  /** The reviewed constructions with no kind, as {@code class#method} relative to {@code com/arcadedb}, each with why. */
  private static final Map<String, String> ALLOWED = new TreeMap<>(Map.ofEntries(//
      Map.entry("exception/RecordVetoedException#<init>", "a listener's veto of the whole record, not a declared constraint on a value"),
      Map.entry("engine/timeseries/TimeSeriesStorableRows#refused",
          "the refusal of a TIMESERIES batch: one message for every row and column constraint the batch broke"),
      Map.entry("query/sql/executor/SaveElementStep#refuseViolation",
          "a TIMESERIES column constraint: TimeSeriesColumnConstraints answers its reason as one sentence for every constraint"),
      Map.entry("remote/RemoteExceptionRebuilder#<clinit>",
          "the table of names rebuilt from a message alone; both drivers rebuild a ValidationException with its kind ahead of it")));

  @Test
  void everyValidationExceptionNamesItsConstraintOrIsListed() throws Exception {
    final Map<String, byte[]> classes = readClasses(classRootsOfEveryModule());
    assertThat(classes).as("the census must read the classes of every module").hasSizeGreaterThan(3000);

    final Map<String, Set<String>> withKind = new TreeMap<>();
    final Map<String, Set<String>> withoutKind = new TreeMap<>();
    constructionsOf(classes, withKind, withoutKind);

    // the scan still reaches the authority: the property validator, which raises every declared constraint through one helper
    assertThat(withKind).as("the census no longer reaches the constructions of " + AUTHORITY + " that pass a kind")
        .containsKey("database/DocumentValidator#throwValidationException");

    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, Set<String>> site : withoutKind.entrySet())
      if (!ALLOWED.containsKey(site.getKey()))
        offenders.add(site.getKey() + " " + site.getValue());
    assertThat(offenders).as("pass the ValidationException.Kind of the constraint the value violates, or list the site in ALLOWED "
        + "with the reason it names none").isEmpty();

    final List<String> stale = new ArrayList<>();
    for (final String allowed : ALLOWED.keySet())
      if (!withoutKind.containsKey(allowed))
        stale.add(allowed);
    assertThat(stale).as("ALLOWED entries that no longer construct a ValidationException without a kind: remove them").isEmpty();
  }

  /** Every construction of the authority, split by whether it passes a kind, keyed by {@code class#method}. */
  private static void constructionsOf(final Map<String, byte[]> classes, final Map<String, Set<String>> withKind,
      final Map<String, Set<String>> withoutKind) {
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final String outerClass = outerClassOf(entry.getKey()).substring("com/arcadedb/".length());
      // the constructors delegating to each other inside the authority construct nothing of their own
      if (("com/arcadedb/" + outerClass).equals(AUTHORITY))
        continue;
      new ClassReader(entry.getValue()).accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final String key = outerClass + "#" + methodOf(methodName);
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (opcode == Opcodes.INVOKESPECIAL && AUTHORITY.equals(owner) && "<init>".equals(name))
                record(key, desc);
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              // A CONSTRUCTOR REFERENCE (ValidationException::new) IS A HANDLE PASSED TO THE LAMBDA BOOTSTRAP, NOT A CALL
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && AUTHORITY.equals(handle.getOwner()) && "<init>".equals(handle.getName()))
                  record(key, handle.getDesc());
            }

            private void record(final String site, final String constructor) {
              (WITH_KIND.equals(constructor) ? withKind : withoutKind).computeIfAbsent(site, k -> new TreeSet<>()).add(constructor);
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
  }
}
