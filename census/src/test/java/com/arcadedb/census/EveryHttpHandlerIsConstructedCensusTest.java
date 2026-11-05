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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every concrete HTTP handler is constructed by some production code (issue #7829).
 * <p>
 * {@code DeleteDropUserHandler} outlived the route that used to construct it. It still compiled and still called
 * {@code ServerSecurity.dropUserClusterWide}, so an audit of the security entry points counted it as a live door - twice - and
 * reached a wrong conclusion about the blast radius of a bug (issue #7559). A handler that no route builds is not an endpoint;
 * it is a false lead for the next reader. This test fails when one appears, so the route and its handler are removed together.
 * <p>
 * The census ranges over the authority (#11288): a handler is a non-abstract class whose super types reach
 * {@code AbstractServerHttpHandler}, in the compiled classes of every module ({@link CompiledClasses}), and it is constructed
 * when another class instantiates it ({@code new}, or a {@code ::new} reference) or extends it (a constructed subclass runs its
 * constructor; an orphaned subclass is itself flagged, so an orphan chain is caught one level at a time). The source-text census
 * it replaces took a handler to be a {@code *Handler.java} file of the server's handler package, so a handler named otherwise or
 * written in a plugin's package was out of range.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EveryHttpHandlerIsConstructedCensusTest {
  private static final String AUTHORITY = "com/arcadedb/server/http/handler/AbstractServerHttpHandler";

  /** The server alone holds far more handlers than this; below it the census has silently lost its range. */
  private static final int EXPECTED_MINIMUM_HANDLERS = 30;

  @Test
  void everyConcreteHandlerIsConstructedByProductionCode() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();

    final Set<String> handlers = concreteHandlersOf(classes);
    assertThat(handlers).as("the census must find the handlers").hasSizeGreaterThan(EXPECTED_MINIMUM_HANDLERS);
    final Set<String> constructed = constructedBy(classes);
    // Sanity: a handler known to be routed must be seen as constructed, or the check below proves nothing.
    assertThat(constructed).as("the census must see HttpServer's routes").contains("com/arcadedb/server/http/handler/DeleteUserHandler");

    final Set<String> unconstructed = new TreeSet<>(handlers);
    unconstructed.removeAll(constructed);
    assertThat(unconstructed)
        .as("a handler no route constructs is dead code that reads as a live endpoint: register it in HttpServer or delete it")
        .isEmpty();
  }

  /** The non-abstract classes whose super types reach the handler base. */
  private static Set<String> concreteHandlersOf(final Map<String, byte[]> classes) {
    final Set<String> hierarchy = CompiledClasses.implementationsOf(AUTHORITY, classes);
    final Set<String> handlers = new TreeSet<>();
    for (final byte[] bytes : classes.values()) {
      final ClassReader reader = new ClassReader(bytes);
      if (hierarchy.contains(reader.getClassName()) && (reader.getAccess() & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) == 0)
        handlers.add(reader.getClassName());
    }
    return handlers;
  }

  /** Every class another class instantiates, references as a constructor, or extends. */
  private static Set<String> constructedBy(final Map<String, byte[]> classes) {
    final Set<String> constructed = new HashSet<>();
    for (final byte[] bytes : classes.values()) {
      final ClassReader reader = new ClassReader(bytes);
      final String self = reader.getClassName();
      if (reader.getSuperName() != null)
        constructed.add(reader.getSuperName());
      reader.accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String name, final String descriptor, final String signature,
            final String[] exceptions) {
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitTypeInsn(final int opcode, final String type) {
              if (opcode == Opcodes.NEW && !type.equals(self))
                constructed.add(type);
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && handle.getTag() == Opcodes.H_NEWINVOKESPECIAL && !handle.getOwner().equals(self))
                  constructed.add(handle.getOwner());
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return constructed;
  }
}
