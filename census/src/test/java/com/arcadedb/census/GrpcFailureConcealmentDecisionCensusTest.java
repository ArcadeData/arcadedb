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
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7472: every gRPC failure mapped through {@code GrpcErrorMapper} must state a concealment decision, and a new call site
 * that does not must fail the build rather than leak quietly.
 * <p>
 * The concealment-less overloads of {@code GrpcErrorMapper.toStatusRuntimeException} still exist - the tests use them, and they
 * are the honest spelling for a caller that has already decided - and they default {@code conceal} to false. That is exactly what
 * makes a missed call site dangerous: it COMPILES, it PASSES every behavioural test (it answers the right status with the right
 * trailers), and the only thing wrong with it is that it puts the exception's own message on the wire in production. No
 * behavioural test can see that, because the test server is not in production mode.
 * <p>
 * The census ranges over the authority (#11288): every call of a mapper entry point ({@code toStatusRuntimeException},
 * {@code toStatusException}, {@code concealableDescription}), or method reference to one, in the compiled classes of every module
 * ({@link CompiledClasses}). A call states a decision when it calls an overload whose last parameter is the {@code conceal}
 * flag and passes it a value, not a literal: a literal {@code false} is the leak written out, and a literal {@code true} is a
 * decision nobody made. The source-text census it replaces read only this module's {@code com/arcadedb/server/grpc} directory,
 * and judged the decision by the spelling of the last argument ({@code concealErrors()}, {@code conceal}). The other three
 * scans of {@code Issue7472EveryGrpcFailureIsConcealableTest} stay in the grpcw module, which says why.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcFailureConcealmentDecisionCensusTest {
  private static final String      MAPPER       = "com/arcadedb/server/grpc/GrpcErrorMapper";
  private static final Set<String> ENTRY_POINTS = Set.of("toStatusRuntimeException", "toStatusException", "concealableDescription");

  @Test
  void noProductionCallSiteMapsAFailureWithoutAConcealmentDecision() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    assertThat(classes).as("the census must reach the mapper").containsKey(MAPPER + ".class");

    assertThat(callsWithoutADecision(classes)).as("every gRPC failure must be mapped through a call that states whether to "
        + "conceal - use this service's mapError(...) helper, which passes concealErrors(), or thread a 'conceal' parameter from "
        + "it").isEmpty();
  }

  /**
   * The control: a call without the flag, one passing a literal and a method reference are offenders, one passing a value is
   * not. The probe is written as bytecode here because this module carries no gRPC classes to compile a source probe against.
   */
  @Test
  void theCensusTellsADecisionFromALiteral() {
    assertThat(callsWithoutADecision(Map.of(PROBE + ".class", probe()))).contains(
        "server/grpc/ConcealmentProbe#withoutTheFlag (GrpcErrorMapper.toStatusRuntimeException)",
        "server/grpc/ConcealmentProbe#withALiteralFalse (GrpcErrorMapper.concealableDescription)",
        "server/grpc/ConcealmentProbe#withAMethodReference (GrpcErrorMapper::toStatusRuntimeException)").noneMatch(
        offender -> offender.startsWith("server/grpc/ConcealmentProbe#threadingTheDecision"));
  }

  /** The call sites, outside the mapper itself, that call an entry point without passing the decision as a value. */
  private static Set<String> callsWithoutADecision(final Map<String, byte[]> classes) {
    final Set<String> offenders = new TreeSet<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final String outerClass = CompiledClasses.keyOf(entry.getKey());
      final ClassReader reader = new ClassReader(entry.getValue());
      if (reader.getClassName().startsWith(MAPPER))
        continue;
      reader.accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final String key = outerClass + "#" + CompiledClasses.methodOf(methodName);
          return new MethodVisitor(Opcodes.ASM9) {
            private boolean lastPushedALiteral;

            @Override
            public void visitInsn(final int opcode) {
              lastPushedALiteral = opcode == Opcodes.ICONST_0 || opcode == Opcodes.ICONST_1;
            }

            @Override
            public void visitIntInsn(final int opcode, final int operand) {
              lastPushedALiteral = false;
            }

            @Override
            public void visitVarInsn(final int opcode, final int varIndex) {
              lastPushedALiteral = false;
            }

            @Override
            public void visitTypeInsn(final int opcode, final String type) {
              lastPushedALiteral = false;
            }

            @Override
            public void visitFieldInsn(final int opcode, final String owner, final String name, final String desc) {
              lastPushedALiteral = false;
            }

            @Override
            public void visitLdcInsn(final Object value) {
              lastPushedALiteral = false;
            }

            @Override
            public void visitJumpInsn(final int opcode, final Label label) {
              lastPushedALiteral = false;
            }

            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (MAPPER.equals(owner) && ENTRY_POINTS.contains(name) && (!takesTheDecision(desc) || lastPushedALiteral))
                offenders.add(key + " (GrpcErrorMapper." + name + ")");
              lastPushedALiteral = false;
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              // A METHOD REFERENCE TO AN ENTRY POINT DECIDES NOTHING AT THE SITE: THE FLAG, IF ANY, COMES FROM WHOEVER CALLS IT
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && MAPPER.equals(handle.getOwner())
                    && ENTRY_POINTS.contains(handle.getName()))
                  offenders.add(key + " (GrpcErrorMapper::" + handle.getName() + ")");
              lastPushedALiteral = false;
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return offenders;
  }

  private static final String PROBE        = "com/arcadedb/server/grpc/ConcealmentProbe";
  private static final String WITHOUT_FLAG = "(Ljava/lang/Throwable;Ljava/lang/String;)Lio/grpc/StatusRuntimeException;";
  private static final String DESCRIPTION  = "(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/Throwable;Z)Ljava/lang/String;";

  /** A class with one method per shape of a call to the mapper. Never loaded: only read. */
  private static byte[] probe() {
    final ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    writer.visit(Opcodes.V21, Opcodes.ACC_FINAL, PROBE, null, "java/lang/Object", null);

    MethodVisitor method = writer.visitMethod(Opcodes.ACC_STATIC, "withoutTheFlag", WITHOUT_FLAG, null, null);
    method.visitCode();
    method.visitVarInsn(Opcodes.ALOAD, 0);
    method.visitVarInsn(Opcodes.ALOAD, 1);
    method.visitMethodInsn(Opcodes.INVOKESTATIC, MAPPER, "toStatusRuntimeException", WITHOUT_FLAG, false);
    method.visitInsn(Opcodes.ARETURN);
    method.visitMaxs(0, 0);
    method.visitEnd();

    method = writer.visitMethod(Opcodes.ACC_STATIC, "withALiteralFalse", DESCRIPTION, null, null);
    method.visitCode();
    method.visitVarInsn(Opcodes.ALOAD, 0);
    method.visitVarInsn(Opcodes.ALOAD, 1);
    method.visitVarInsn(Opcodes.ALOAD, 2);
    method.visitInsn(Opcodes.ICONST_0);
    method.visitMethodInsn(Opcodes.INVOKESTATIC, MAPPER, "concealableDescription", DESCRIPTION, false);
    method.visitInsn(Opcodes.ARETURN);
    method.visitMaxs(0, 0);
    method.visitEnd();

    method = writer.visitMethod(Opcodes.ACC_STATIC, "threadingTheDecision", DESCRIPTION, null, null);
    method.visitCode();
    method.visitVarInsn(Opcodes.ALOAD, 0);
    method.visitVarInsn(Opcodes.ALOAD, 1);
    method.visitVarInsn(Opcodes.ALOAD, 2);
    method.visitVarInsn(Opcodes.ILOAD, 3);
    method.visitMethodInsn(Opcodes.INVOKESTATIC, MAPPER, "concealableDescription", DESCRIPTION, false);
    method.visitInsn(Opcodes.ARETURN);
    method.visitMaxs(0, 0);
    method.visitEnd();

    method = writer.visitMethod(Opcodes.ACC_STATIC, "withAMethodReference", "()Ljava/util/function/BiFunction;", null, null);
    method.visitCode();
    method.visitInvokeDynamicInsn("apply", "()Ljava/util/function/BiFunction;",
        new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/LambdaMetafactory", "metafactory",
            "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodType;"
                + "Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;", false),
        Type.getType("(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"),
        new Handle(Opcodes.H_INVOKESTATIC, MAPPER, "toStatusRuntimeException", WITHOUT_FLAG, false), Type.getType(WITHOUT_FLAG));
    method.visitInsn(Opcodes.ARETURN);
    method.visitMaxs(0, 0);
    method.visitEnd();

    writer.visitEnd();
    return writer.toByteArray();
  }

  /** Whether the overload's last parameter is the {@code conceal} flag. */
  private static boolean takesTheDecision(final String descriptor) {
    final Type[] parameters = Type.getArgumentTypes(descriptor);
    return parameters.length > 0 && parameters[parameters.length - 1] == Type.BOOLEAN_TYPE;
  }
}
