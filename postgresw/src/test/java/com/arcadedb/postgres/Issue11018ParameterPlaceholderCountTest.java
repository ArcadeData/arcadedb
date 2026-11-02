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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import static com.arcadedb.postgres.PostgresNetworkExecutor.highestParameterPlaceholder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The count of {@code $N} placeholders a Parse derives from its text (issue #11018). Since a Bind with the wrong number of values
 * is refused, a placeholder counted where the SQL lexer sees none would refuse a correct Bind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11018ParameterPlaceholderCountTest {

  @Test
  void countsTheHighestPlaceholderNotHowManyThereAre() {
    assertThat(highestParameterPlaceholder("SELECT 1")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT $3, $1, $3")).isEqualTo(3);
    assertThat(highestParameterPlaceholder("SELECT * FROM t WHERE a=$1 AND b IN ($2,$12)")).isEqualTo(12);
  }

  @Test
  void ignoresPlaceholdersInsideQuotedTextAndComments() {
    assertThat(highestParameterPlaceholder("SELECT '$5' FROM t WHERE a = $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT \"$5\" FROM t WHERE a = $2")).isEqualTo(2);
    assertThat(highestParameterPlaceholder("SELECT `$5` FROM t WHERE a = $2")).isEqualTo(2);
    assertThat(highestParameterPlaceholder("SELECT 'it\\'s $9' FROM t WHERE a = $1")).as("an escaped quote does not end the string")
        .isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT 'a''$9' FROM t WHERE a = $1")).as("a doubled quote does not end it either")
        .isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT a /* $9 */ FROM t WHERE a = $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT a -- $9\nFROM t WHERE a = $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT a /* $9 unterminated")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT '$9 unterminated")).isEqualTo(0);
  }

  @Test
  void aDollarNameIsNotAPlaceholder() {
    assertThat(highestParameterPlaceholder("SELECT $parent, $current FROM t")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $1abc FROM t")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $1_x FROM t")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT 5 - $2")).isEqualTo(2);
  }

  @Test
  void aPositionPastAnyBindIsReportedNotOverflowed() {
    assertThat(highestParameterPlaceholder("SELECT $70000")).isGreaterThan(PostgresNetworkExecutor.MAX_STATEMENT_PARAMETERS);
    assertThat(highestParameterPlaceholder("SELECT $99999999999999999999")).isEqualTo(Integer.MAX_VALUE);
  }
}
