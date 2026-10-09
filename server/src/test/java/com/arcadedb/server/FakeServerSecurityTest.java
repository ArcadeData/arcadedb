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
package com.arcadedb.server;

import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FakeServerSecurityTest {

  @Test
  void anUnansweredMethodRunsTheRealCodeOverAnEmptyDirectory() {
    final FakeServerSecurity security = FakeServerSecurity.create();

    assertThat(security.getUsers()).isEmpty();
    assertThat(security.getUser("anyone")).isNull();
    assertThat(security.unconvergedClusterSecurityDocuments()).as("nothing was ever installed from the cluster")
        .containsExactly("users", "groups", "API tokens");
    assertThat(security.calls("getUser")).containsExactly(List.of("anyone"));
  }

  @Test
  void unansweredAuthenticationRunsTheRealChecksAgainstAnEmptyUserList() {
    final FakeServerSecurity security = FakeServerSecurity.create();

    assertThatThrownBy(() -> security.authenticate("nobody", "pw", null)).isInstanceOf(ServerSecurityException.class);
    assertThatThrownBy(() -> security.revalidate(TestServerHelper.securityUser("nobody")))
        .isInstanceOf(ServerSecurityException.class);
  }

  @Test
  void unansweredReplicationAndSeedingRunTheRealCode() {
    final FakeServerSecurity security = FakeServerSecurity.create();

    security.applyReplicatedUsers("[]");
    assertThatThrownBy(() -> security.applyReplicatedGroups("{}")).as("the real validation refuses an unusable document")
        .isInstanceOf(ServerSecurityException.class).hasMessageContaining("databases");
    security.applyReplicatedApiTokens("{\"tokens\":[]}");
    assertThat(security.getUsers()).isEmpty();
    assertThat(security.seedSecurityStateClusterWide()).as("bound to no server, there is no cluster to seed").isEmpty();
    assertThat(security.calls("seedSecurityStateClusterWide")).as("the no-argument form delegates").hasSize(1);
  }

  @Test
  void overloadsShareOneNameAndAreToldApartByArity() {
    final FakeServerSecurity security = FakeServerSecurity.create()
        .on("applyReplicatedUsers", args -> args.length == 2 && "expected".equals(args[1]));

    security.applyReplicatedUsers("[]");
    assertThat(security.applyReplicatedUsers("[]", "expected")).isTrue();
    assertThat(security.applyReplicatedUsers("[]", "stale")).isFalse();

    assertThat(security.calls("applyReplicatedUsers")).containsExactly(List.of("[]"), List.of("[]", "expected"),
        List.of("[]", "stale"));
  }

  @Test
  void aUserCanBeAnsweredAndAFailureInjected() {
    final ServerSecurityUser alice = TestServerHelper.securityUser("alice");
    final FakeServerSecurity security = FakeServerSecurity.create()
        .on("revalidate", args -> args[0])
        .fails("authenticate", new ServerSecurityException("User/Password not valid"));

    assertThat(security.revalidate(alice)).isSameAs(alice);
    assertThatThrownBy(() -> security.authenticate("alice", "wrong", null)).isInstanceOf(ServerSecurityException.class)
        .hasMessage("User/Password not valid");
    assertThat(security.calls("authenticate")).containsExactly(Arrays.asList("alice", "wrong", null));
  }

  @Test
  void aNonBooleanAnswerForABooleanOverloadIsRefusedByName() {
    final FakeServerSecurity security = FakeServerSecurity.create().returns("applyReplicatedGroups", null);

    assertThatThrownBy(() -> security.applyReplicatedGroups("{}", "fp")).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("applyReplicatedGroups");
  }
}
