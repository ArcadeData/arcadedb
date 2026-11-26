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
package com.arcadedb.bench.workload.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresProtocolClientTest {

  @Test
  void parametersBecomePlaceholdersInNameOrder() {
    final PostgresProtocolClient.Translated t = PostgresProtocolClient.translate(
        "MATCH (a:P {id: $from}), (b:P {id: $to}) WHERE a.k = $from RETURN a.id AS a, b.id AS b");
    assertThat(t.sql()).isEqualTo("{cypher}MATCH (a:P {id: ?}), (b:P {id: ?}) WHERE a.k = ? RETURN a.id AS a, b.id AS b");
    assertThat(t.names()).containsExactly("from", "to", "from");
  }

  @Test
  void quotedTextAndCommentsKeepTheirDollar() {
    final PostgresProtocolClient.Translated t = PostgresProtocolClient.translate(
        "RETURN '$a' AS x, \"$b\" AS `$c`, 'it\\'s $d' AS y, $e AS z // $f\n/* $g */ ");
    assertThat(t.sql()).isEqualTo("{cypher}RETURN '$a' AS x, \"$b\" AS `$c`, 'it\\'s $d' AS y, ? AS z // $f\n/* $g */ ");
    assertThat(t.names()).containsExactly("e");
  }

  @Test
  void aNameThatPrefixesAnotherIsItsOwnParameter() {
    final PostgresProtocolClient.Translated t = PostgresProtocolClient.translate(
        "MATCH (n) WHERE n.id = $id OR n.id IN $ids RETURN $id_2 AS x");
    assertThat(t.sql()).isEqualTo("{cypher}MATCH (n) WHERE n.id = ? OR n.id IN ? RETURN ? AS x");
    assertThat(t.names()).containsExactly("id", "ids", "id_2");
  }

  @Test
  void noParametersNoPlaceholders() {
    final PostgresProtocolClient.Translated t = PostgresProtocolClient.translate("MATCH (n) RETURN count(n) AS n");
    assertThat(t.sql()).isEqualTo("{cypher}MATCH (n) RETURN count(n) AS n");
    assertThat(t.names()).isEmpty();
  }

  @Test
  void scalarsAndListsAreBindableAMapIsNot() {
    PostgresProtocolClient.checkBindable("s", "x");
    PostgresProtocolClient.checkBindable("l", 1L);
    PostgresProtocolClient.checkBindable("d", 0.5d);
    PostgresProtocolClient.checkBindable("rows", List.of(Map.of("id", 1)));
    PostgresProtocolClient.checkBindable("vectors", List.of(new float[] { 0.5f }));
    assertThatThrownBy(() -> PostgresProtocolClient.checkBindable("m", Map.of("k", 1)))
        .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("'m'").hasMessageContaining("map");
    assertThatThrownBy(() -> PostgresProtocolClient.checkBindable("o", new Object()))
        .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("'o'");
  }

  @Test
  void protocolCaseNames() {
    assertThat(Protocol.POSTGRES.caseName()).isEqualTo("pg");
    assertThat(Protocol.ofCaseName("BOLT")).isEqualTo(Protocol.BOLT);
    assertThatThrownBy(() -> Protocol.ofCaseName("mongo")).isInstanceOf(IllegalArgumentException.class);
  }
}
