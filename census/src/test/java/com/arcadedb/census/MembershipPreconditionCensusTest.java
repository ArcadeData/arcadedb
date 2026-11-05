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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10769: a membership REMOVAL (and the leave that leads to one) built its compare-and-set precondition from
 * {@code RaftHAServer.getLivePeers()}, which substitutes the declared server list while the division cannot be read, so the
 * leader compared against a configuration it never had. Every membership change now receives a
 * {@code RaftClusterManager.CommittedConfiguration}, which only {@code readCommittedConfiguration} builds, and the behaviour is
 * pinned by {@code Issue10769MembershipChangeReadsCommittedConfigurationTest}.
 * <p>
 * The census ranges over the authority (#11288): every call of the Ratis {@code setServersInCurrentConf}, the precondition
 * itself, in the compiled classes of every module ({@link CompiledClasses}), must sit in a method handed the committed
 * configuration. The source-text census it replaces read the nearest method header above the spelling
 * {@code .setServersInCurrentConf(} in the {@code ha-raft} package, so a lambda or a nested class between them, or a builder in
 * another package, decided against the wrong header or escaped it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MembershipPreconditionCensusTest {
  private static final String RATIS        = "org/apache/ratis/";
  private static final String PRECONDITION = "setServersInCurrentConf";
  private static final String COMMITTED    = "Lcom/arcadedb/server/ha/raft/RaftClusterManager$CommittedConfiguration;";

  @Test
  void everyPreconditionIsBuiltFromTheCommittedConfiguration() throws Exception {
    final List<CallOperands.Site> sites = CallOperands.of(CompiledClasses.ofEveryModule(),
        (owner, name, desc) -> owner.startsWith(RATIS) && PRECONDITION.equals(name));

    final Set<String> builders = new TreeSet<>();
    final Set<String> unhanded = new TreeSet<>();
    for (final CallOperands.Site site : sites) {
      builders.add(site.key());
      if (Arrays.stream(Type.getArgumentTypes(site.method().desc)).noneMatch(t -> COMMITTED.equals(t.getDescriptor())))
        unhanded.add(site.key() + site.method().desc);
    }

    assertThat(builders).as("the census must reach the precondition of the add and of the removal").contains(
        "server/ha/raft/RaftClusterManager#buildAddArgs", "server/ha/raft/RaftClusterManager#buildRemoveArgs");
    assertThat(unhanded).as("methods building a compare-and-set precondition without being handed the committed configuration: "
        + "the leader compares it against the configuration it has, so it must be read from there, never from getLivePeers() "
        + "(issue #10769)").isEmpty();
  }
}
