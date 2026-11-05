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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.SourceInterpreter;
import org.objectweb.asm.tree.analysis.SourceValue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The calls of {@link CompiledClasses} with what produced each of their operands, for the censuses whose offender is a call with
 * a particular argument: a format constant against the length of the varargs array that follows it, a value handed on from a
 * request, a {@code Locale} that is not there. The operands come from the data flow of the method ({@link SourceInterpreter}),
 * followed back through a local variable, a {@code DUP} and a cast to the instruction that made the value, so a value held in a
 * local first, or a call split over several lines, is the same call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CallOperands {
  /** A method with the data flow of its body: which instructions produced each value of each frame. */
  record Method(String key, ClassNode owner, MethodNode node, Frame<SourceValue>[] frames) {
    /** The instructions that produced the value {@code depth} slots below the top of the stack just before {@code insn}. */
    Set<AbstractInsnNode> stackBefore(final AbstractInsnNode insn, final int depth) {
      final Frame<SourceValue> frame = frames[node.instructions.indexOf(insn)];
      return origins(frame.getStack(frame.getStackSize() - 1 - depth));
    }

    /** The instructions that produced {@code value}, followed back through locals, {@code DUP}s and casts. */
    Set<AbstractInsnNode> origins(final SourceValue value) {
      final Set<AbstractInsnNode> origins = new LinkedHashSet<>();
      final Deque<AbstractInsnNode> pending = new ArrayDeque<>(value.insns);
      final Set<AbstractInsnNode> seen = new HashSet<>();
      while (!pending.isEmpty()) {
        final AbstractInsnNode insn = pending.pop();
        if (!seen.add(insn))
          continue;
        final Frame<SourceValue> frame = frames[node.instructions.indexOf(insn)];
        if (frame == null) {
          origins.add(insn);
          continue;
        }
        switch (insn.getOpcode()) {
        case Opcodes.ALOAD, Opcodes.ILOAD, Opcodes.LLOAD, Opcodes.FLOAD, Opcodes.DLOAD -> {
          final SourceValue stored = frame.getLocal(((VarInsnNode) insn).var);
          if (stored.insns.isEmpty())
            origins.add(insn); // a parameter of the method, or this
          else
            pending.addAll(stored.insns);
        }
        case Opcodes.ASTORE, Opcodes.ISTORE, Opcodes.LSTORE, Opcodes.FSTORE, Opcodes.DSTORE, Opcodes.CHECKCAST, Opcodes.DUP ->
            pending.addAll(frame.getStack(frame.getStackSize() - 1).insns);
        default -> origins.add(insn);
        }
      }
      return origins;
    }

    /** The parameter of the method a load of {@code origins} reads, or -1 when the value is not one of its parameters. */
    int parameterOf(final Set<AbstractInsnNode> origins) {
      return origins.size() == 1 ? parameterOf(origins.iterator().next()) : -1;
    }

    /** The parameter of the method {@code origin} loads, or -1 when it is not the load of a parameter. */
    int parameterOf(final AbstractInsnNode origin) {
      if (!(origin instanceof VarInsnNode load) || load.getOpcode() < Opcodes.ILOAD || load.getOpcode() > Opcodes.ALOAD)
        return -1;
      int slot = (node.access & Opcodes.ACC_STATIC) != 0 ? 0 : 1;
      final Type[] parameters = Type.getArgumentTypes(node.desc);
      for (int i = 0; i < parameters.length; i++) {
        if (slot == load.var)
          return i;
        slot += parameters[i].getSize();
      }
      return -1;
    }

    /** The instructions that produced each value the method returns. */
    Set<AbstractInsnNode> returned() {
      final Set<AbstractInsnNode> returned = new LinkedHashSet<>();
      for (final AbstractInsnNode insn : node.instructions)
        if (insn.getOpcode() == Opcodes.ARETURN && frames[node.instructions.indexOf(insn)] != null)
          returned.addAll(stackBefore(insn, 0));
      return returned;
    }

    /** The instructions that produced each operand of an {@code invokedynamic} (the parts of a string concatenation). */
    Set<AbstractInsnNode> operandsOf(final InvokeDynamicInsnNode insn) {
      final Set<AbstractInsnNode> operands = new LinkedHashSet<>();
      for (int i = 0; i < Type.getArgumentTypes(insn.desc).length; i++)
        operands.addAll(stackBefore(insn, i));
      return operands;
    }
  }

  /** One call: the analysed method it is in, and what it calls. */
  record Site(Method in, MethodInsnNode call) {
    String key() {
      return in.key();
    }

    ClassNode owner() {
      return in.owner();
    }

    MethodNode method() {
      return in.node();
    }

    Frame<SourceValue>[] frames() {
      return in.frames();
    }

    /** The instructions that produced operand {@code index} of the call ({@code 0} is the receiver of an instance call). */
    Set<AbstractInsnNode> operand(final int index) {
      final int operands = Type.getArgumentTypes(call.desc).length + (call.getOpcode() == Opcodes.INVOKESTATIC ? 0 : 1);
      return in.stackBefore(call, operands - 1 - index);
    }

    /** The instructions that produced argument {@code index} of the call, whatever the call's kind. */
    Set<AbstractInsnNode> argument(final int index) {
      return operand(call.getOpcode() == Opcodes.INVOKESTATIC ? index : index + 1);
    }

    Set<AbstractInsnNode> origins(final SourceValue value) {
      return in.origins(value);
    }

    int parameterOf(final Set<AbstractInsnNode> origins) {
      return in.parameterOf(origins);
    }

    int parameterOf(final AbstractInsnNode origin) {
      return in.parameterOf(origin);
    }
  }

  private CallOperands() {
  }

  /** Every call {@code matcher} accepts in {@code classes}, with its analysed method. */
  static List<Site> of(final Map<String, byte[]> classes, final CompiledClasses.CallMatcher matcher) {
    final List<Site> sites = new ArrayList<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final ClassNode owner = read(entry.getValue());
      for (final MethodNode node : owner.methods) {
        Method method = null;
        for (final AbstractInsnNode insn : node.instructions) {
          if (!(insn instanceof MethodInsnNode call) || !matcher.matches(call.owner, call.name, call.desc))
            continue;
          if (method == null)
            method = analyse(entry.getKey(), owner, node);
          sites.add(new Site(method, call));
        }
      }
    }
    return sites;
  }

  /** The method {@code call} names, analysed, when {@code classes} declares it in the class it names; null otherwise. */
  static Method declarationOf(final Map<String, byte[]> classes, final MethodInsnNode call) {
    final String classFile = call.owner + ".class";
    final byte[] bytes = classes.get(classFile);
    if (bytes == null)
      return null;
    final ClassNode owner = read(bytes);
    for (final MethodNode node : owner.methods)
      if (node.name.equals(call.name) && node.desc.equals(call.desc) && node.instructions.size() > 0)
        return analyse(classFile, owner, node);
    return null;
  }

  /** The string constant {@code origins} is, or null when it is anything else. */
  static String stringConstant(final Set<AbstractInsnNode> origins) {
    return origins.size() == 1 && origins.iterator().next() instanceof LdcInsnNode ldc && ldc.cst instanceof String s ? s : null;
  }

  /** The int constant {@code origins} is, or null when it is anything else. */
  static Integer intConstant(final Set<AbstractInsnNode> origins) {
    if (origins.size() != 1)
      return null;
    final AbstractInsnNode insn = origins.iterator().next();
    final int opcode = insn.getOpcode();
    if (opcode >= Opcodes.ICONST_M1 && opcode <= Opcodes.ICONST_5)
      return opcode - Opcodes.ICONST_0;
    if (insn instanceof IntInsnNode push && (opcode == Opcodes.BIPUSH || opcode == Opcodes.SIPUSH))
      return push.operand;
    if (insn instanceof LdcInsnNode ldc && ldc.cst instanceof Integer i)
      return i;
    return null;
  }

  /**
   * The length of the array {@code origins} is, when it is a new array of constant length ({@code new Object[] { a, b }}, a
   * varargs call), or null when it is anything else.
   */
  static Integer arrayLength(final Site site, final Set<AbstractInsnNode> origins) {
    if (origins.size() != 1 || !(origins.iterator().next() instanceof TypeInsnNode array) || array.getOpcode() != Opcodes.ANEWARRAY)
      return null;
    return intConstant(site.in().stackBefore(array, 0));
  }

  private static ClassNode read(final byte[] bytes) {
    final ClassNode owner = new ClassNode();
    new ClassReader(bytes).accept(owner, ClassReader.SKIP_FRAMES);
    return owner;
  }

  @SuppressWarnings("unchecked")
  private static Method analyse(final String classFile, final ClassNode owner, final MethodNode node) {
    try {
      return new Method(CompiledClasses.keyOf(classFile) + "#" + CompiledClasses.methodOf(node.name), owner, node,
          new Analyzer<>(new SourceInterpreter()).analyze(owner.name, node));
    } catch (final AnalyzerException e) {
      throw new AssertionError("cannot analyse " + owner.name + "." + node.name + node.desc, e);
    }
  }
}
