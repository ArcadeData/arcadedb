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
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.SourceValue;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10375: {@code Iterator.remove()} on an adjacency iterator took the entry out of the segment by position and wrote the
 * segment without excluding its page from the edge-append merge. When the same transaction had also appended to that segment, a
 * concurrent commit on the page made the merge rebase it - committed image plus this transaction's tracked appends - and the
 * removal vanished, while the edge record it named was deleted for good. A segment write that is not an in-chunk append must go
 * through {@code EdgeLinkedList.updateRewrittenSegment}, which excludes the page from the merge; the behaviour is pinned by
 * {@code Issue10375IteratorRemoveEdgeAppendMergeTest}.
 * <p>
 * The census ranges over the authority (#11288): every call of {@code updateRecord} whose argument is an {@code EdgeSegment} - by
 * the static type of the value handed to it: a local's declared type, a cast, a call's return type, a field's - in the compiled
 * classes of every module ({@link CompiledClasses}, {@link CallOperands}), counted per method, so a new direct write fails it,
 * in a method listed or not. The source-text census it replaces counted {@code .updateRecord(name)} per argument NAME, line by
 * line, in the graph sources that mention {@code EdgeSegment}, so a write in another package, through a renamed local or split
 * over two lines passed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeSegmentRewriteCensusTest {
  private static final String EDGE_SEGMENT  = "com/arcadedb/graph/EdgeSegment";
  private static final String UPDATE_RECORD = "updateRecord";

  /** Every direct segment write, with how many there are in the method, and which of the two it is. */
  private static final Map<String, Integer> DIRECT_UPDATES = new TreeMap<>(Map.of(//
      // THE TRACKED IN-CHUNK APPEND, WHICH THE EDGE-APPEND MERGE REPLAYS
      "graph/EdgeLinkedList#add", 1,//
      "graph/StripedEdgeList#append", 1,//
      // THE ONE DOOR FOR EVERY OTHER SEGMENT WRITE: IT EXCLUDES THE PAGE FROM THE MERGE
      "graph/EdgeLinkedList#updateRewrittenSegment", 1,//
      // GraphBatch'S OWN TRANSACTIONS, WHICH TRACK NO APPEND: SEE THE NOTE ON THE EDGE-APPEND MERGE IN THE CLASS
      "graph/GraphBatch#addEdgesToSegmentBulkLazy", 3,//
      "graph/GraphBatch#addIncomingEdgesToSegmentBulkLazy", 3,//
      "graph/GraphBatch#connectIncomingEdgesRangeLocal", 1,//
      "graph/GraphBatch#connectIncomingEdgesSequential", 1,//
      "graph/GraphBatch#connectOutEdgesRangeLocal", 1,//
      "graph/GraphBatch#connectOutgoingEdgesSorted", 1));

  @Test
  void everySegmentRewriteGoesThroughTheDoorThatExcludesItsPage() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> segments = CompiledClasses.implementationsOf(EDGE_SEGMENT, classes);

    final Map<String, Integer> writes = new TreeMap<>();
    for (final CallOperands.Site site : CallOperands.of(classes, (owner, name, desc) -> UPDATE_RECORD.equals(name)
        && Type.getArgumentTypes(desc).length == 1))
      if (segments.contains(staticTypeOf(site)))
        writes.merge(site.key(), 1, Integer::sum);

    assertThat(writes).as("the census must reach the one door every other segment write goes through").containsKey(
        "graph/EdgeLinkedList#updateRewrittenSegment");
    // THE ITERATORS IN PARTICULAR: THEIR remove() IS THE WRITE #10375 FOUND UNEXCLUDED
    assertThat(writes.keySet()).as("an iterator writing a segment itself").noneMatch(k -> k.startsWith("graph/ResettableIteratorBase"));
    assertThat(writes).as("methods handing updateRecord an edge segment directly, and how many times. A segment rewrite must use "
        + "EdgeLinkedList.updateRewrittenSegment; a call that is a tracked append is added to DIRECT_UPDATES with which it is "
        + "(issue #10375)").isEqualTo(DIRECT_UPDATES);
  }

  /** The static type of the argument handed to the call, from the instruction that produced it. */
  private static String staticTypeOf(final CallOperands.Site site) {
    final MethodNode method = site.method();
    final Frame<SourceValue> frame = site.frames()[method.instructions.indexOf(site.call())];
    final Set<AbstractInsnNode> producers = frame.getStack(frame.getStackSize() - 1).insns;
    if (producers.size() != 1)
      return null;
    final AbstractInsnNode producer = producers.iterator().next();
    final String descriptor = switch (producer) {
      case VarInsnNode load -> localType(method, load);
      case TypeInsnNode cast -> "L" + cast.desc + ";";
      case MethodInsnNode call -> Type.getReturnType(call.desc).getDescriptor();
      case FieldInsnNode field -> field.desc;
      default -> null;
    };
    return descriptor != null && descriptor.startsWith("L") ? Type.getType(descriptor).getInternalName() : null;
  }

  /** The declared type of the local {@code load} reads, from the local variable table. */
  private static String localType(final MethodNode method, final VarInsnNode load) {
    final int at = method.instructions.indexOf(load);
    if (method.localVariables != null)
      for (final LocalVariableNode local : method.localVariables)
        if (local.index == load.var && method.instructions.indexOf(local.start) <= at + 1 && at < method.instructions.indexOf(local.end))
          return local.desc;
    return null;
  }
}
