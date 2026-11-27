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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The depth limit and the conflict search of the paths of an update.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoUpdatePathsTest {

  @Test
  void aPathOfTheMaximumDepthIsAcceptedAndOneMoreFieldIsRefused() {
    assertThatCode(() -> MongoUpdatePaths.checkDepth("a")).doesNotThrowAnyException();
    assertThatCode(() -> MongoUpdatePaths.checkDepth(path(MongoUpdatePaths.MAX_DEPTH))).doesNotThrowAnyException();

    assertThatThrownBy(() -> MongoUpdatePaths.checkDepth(path(MongoUpdatePaths.MAX_DEPTH + 1))).isInstanceOfSatisfying(
        MongoServerError.class, e -> {
          assertThat(e.getCode()).isEqualTo(ErrorCode.BadValue.getValue());
          // the text names the limit, never the path
          assertThat(e.getMessageWithoutErrorCode()).contains(String.valueOf(MongoUpdatePaths.MAX_DEPTH)).hasSizeLessThan(100);
        });
    assertThatThrownBy(() -> MongoUpdatePaths.checkDepth(path(200_000))).isInstanceOf(MongoServerError.class);
  }

  @Test
  void aPathIsOrderedFieldByFieldWithTheDotFirst() {
    assertThat(MongoUpdatePaths.compare("a", "a.b")).isNegative();
    assertThat(MongoUpdatePaths.compare("a.b", "a")).isPositive();
    assertThat(MongoUpdatePaths.compare("a.b", "a-b")).isNegative();
    assertThat(MongoUpdatePaths.compare("a.b", "a0")).isNegative();
    assertThat(MongoUpdatePaths.compare("a-b", "a0")).isNegative();
    assertThat(MongoUpdatePaths.compare("a.b", "a.b")).isZero();
    assertThat(MongoUpdatePaths.compare("", "a")).isNegative();
  }

  @Test
  void pathsWithoutAConflictAreAccepted() {
    assertThat(MongoUpdatePaths.findConflict(List.of())).isNull();
    assertThat(MongoUpdatePaths.findConflict(List.of("a.b.c"))).isNull();
    assertThat(MongoUpdatePaths.findConflict(List.of("a.b", "a.c", "a-b", "a0", "ab", "b.a", "a b"))).isNull();
    assertThat(MongoUpdatePaths.findConflict(List.of("a.bc", "a.b", "a.b0"))).isNull();
  }

  @Test
  void aConflictIsReportedAsMongodDoesWhicheverOrderThePathsAreNamedIn() {
    // the path met last names the path it conflicts with
    assertThat(MongoUpdatePaths.findConflict(List.of("a", "a.b"))).containsExactly("a.b", "a");
    assertThat(MongoUpdatePaths.findConflict(List.of("a.b", "a"))).containsExactly("a", "a");
    assertThat(MongoUpdatePaths.findConflict(List.of("a.b.c", "x", "a.b"))).containsExactly("a.b", "a.b");
    // the same path twice
    assertThat(MongoUpdatePaths.findConflict(List.of("a.b", "x", "a.b"))).containsExactly("a.b", "a.b");
    // what sorts between a path and the ones below it by character does not hide them
    assertThat(MongoUpdatePaths.findConflict(List.of("a", "a-b", "a.b", "a0"))).containsExactly("a.b", "a");
    assertThat(MongoUpdatePaths.findConflict(List.of("a.b", "a b", "a!", "a"))).containsExactly("a", "a");
    // an empty field is a field
    assertThat(MongoUpdatePaths.findConflict(List.of("a", "a."))).containsExactly("a.", "a");
  }

  @Test
  void manyPathsAndLongPathsAreComparedWithoutCopyingPrefixes() {
    final List<String> paths = new ArrayList<>();
    for (int i = 0; i < 50_000; i++)
      paths.add("k" + i + ".v");
    assertThat(MongoUpdatePaths.findConflict(paths)).isNull();
    paths.add("k49999");
    assertThat(MongoUpdatePaths.findConflict(paths)).containsExactly("k49999", "k49999");

    // the fields are long, the paths a few megabytes: only the paths themselves are held
    final String field = "f".repeat(50_000);
    final String deep = String.join(".", Collections.nCopies(MongoUpdatePaths.MAX_DEPTH, field));
    final String ancestor = String.join(".", Collections.nCopies(MongoUpdatePaths.MAX_DEPTH - 1, field));
    assertThat(MongoUpdatePaths.findConflict(List.of(ancestor, deep))).containsExactly(deep, ancestor);
    assertThat(MongoUpdatePaths.findConflict(List.of(deep, ancestor))).containsExactly(ancestor, ancestor);
    assertThat(MongoUpdatePaths.findConflict(List.of(deep, deep + "x"))).isNull();
  }

  private static String path(final int fields) {
    return String.join(".", Collections.nCopies(fields, "f"));
  }
}
