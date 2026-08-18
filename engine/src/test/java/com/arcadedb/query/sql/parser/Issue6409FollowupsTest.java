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
package com.arcadedb.query.sql.parser;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6409, items 1, 2 and 4: three follow-ups from #6401 that were not that issue's to fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6409FollowupsTest extends AbstractParserTest {

  /**
   * Item 1: {@code IMPORT}, {@code EXPORT} and {@code BACKUP DATABASE} used to park a setting NAME in the node's
   * inherited {@code value} slot rather than building an identifier expression out of it, the way {@code REBUILD
   * INDEX} and {@code REBUILD TYPE} already did. All five now build the key the same way, so all five read it back
   * the same way too: {@code toString()}, not the raw {@code value} slot.
   */
  @Test
  void allFiveWithSettingsStatementsBuildTheSameKeyShape() throws Exception {
    final ImportDatabaseStatement imp = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
        "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true");
    final ExportDatabaseStatement exp = (ExportDatabaseStatement) new SQLAntlrParser(null).parse(
        "EXPORT DATABASE file://foo.bar WITH format = 'graphml'");
    final BackupDatabaseStatement bak = (BackupDatabaseStatement) new SQLAntlrParser(null).parse(
        "BACKUP DATABASE file://foo.bar WITH compressionLevel = 5");
    final RebuildIndexStatement idx = (RebuildIndexStatement) new SQLAntlrParser(null).parse(
        "REBUILD INDEX Foo WITH batchSize = 1000");
    final RebuildTypeStatement typ = (RebuildTypeStatement) new SQLAntlrParser(null).parse(
        "REBUILD TYPE Foo WITH batchSize = 1000");

    assertThat(imp.settings.keySet()).as("IMPORT").allMatch(k -> k.isBaseIdentifier());
    assertThat(exp.settings.keySet()).as("EXPORT").allMatch(k -> k.isBaseIdentifier());
    assertThat(bak.settings.keySet()).as("BACKUP").allMatch(k -> k.isBaseIdentifier());
    assertThat(idx.settings.keySet()).as("REBUILD INDEX").allMatch(k -> k.isBaseIdentifier());
    assertThat(typ.settings.keySet()).as("REBUILD TYPE").allMatch(k -> k.isBaseIdentifier());

    assertThat(imp.settings.keySet().iterator().next().value).as("no raw value slot any more").isNull();
    assertThat(imp.settings.keySet().stream().map(Expression::toString)).containsExactly("forceDatabaseCreate");
    assertThat(exp.settings.keySet().stream().map(Expression::toString)).containsExactly("format");
    assertThat(bak.settings.keySet().stream().map(Expression::toString)).containsExactly("compressionLevel");
  }

  /**
   * The historical defect the old shape produced (issue #6080 / #6359, item 2): a backup asked to be encrypted was
   * silently written in clear because the setting name could not be read back. Guards against a regression on the
   * executable path, not just the parsed shape.
   */
  @Test
  void importExportBackupSettingsRoundTripThroughToString() {
    checkRightSyntax("IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");
    checkRightSyntax("EXPORT DATABASE file://foo.bar WITH format = 'graphml'");
    checkRightSyntax("BACKUP DATABASE file://foo.bar WITH compressionLevel = 5, encryptionKey = 'secret'");
  }

  /**
   * Item 2: before this, only {@code REBUILD TYPE} refused a repeated setting; the other four silently accepted it,
   * last one wins. All five now agree, and the message names the actual statement.
   */
  @Test
  void allFiveStatementsRefuseADuplicateSetting() {
    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("REBUILD INDEX Foo WITH batchSize = 1, batchSize = 2"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("REBUILD INDEX").hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("REBUILD TYPE Foo WITH batchSize = 1, batchSize = 2"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("REBUILD TYPE").hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("IMPORT DATABASE http://foo.bar WITH batchSize = 1, batchSize = 2"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("IMPORT DATABASE").hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("EXPORT DATABASE file://foo.bar WITH batchSize = 1, batchSize = 2"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("EXPORT DATABASE").hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("BACKUP DATABASE file://foo.bar WITH batchSize = 1, batchSize = 2"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("BACKUP DATABASE").hasMessageContaining("duplicate setting");
  }

  /**
   * The dedup key is the LOWERCASED name - matching the {@code equalsIgnoreCase} every reader already uses to
   * recognise a setting - not {@link Expression} identity, which is case-sensitive since #6401. A case-only repeat
   * ({@code batchSize} / {@code BATCHSIZE}) has to be caught too, in every one of the five statements.
   */
  @Test
  void aCaseOnlyRepeatIsStillADuplicateInEveryStatement() {
    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("REBUILD INDEX Foo WITH batchSize = 1, BATCHSIZE = 2"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("IMPORT DATABASE http://foo.bar WITH batchSize = 1, BATCHSIZE = 2"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("EXPORT DATABASE file://foo.bar WITH batchSize = 1, BATCHSIZE = 2"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("BACKUP DATABASE file://foo.bar WITH batchSize = 1, BATCHSIZE = 2"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");
  }

  /** Two DIFFERENT settings, or the same setting spelled once, must keep working - only an actual repeat is refused. */
  @Test
  void distinctSettingsAreNotFlaggedAsDuplicates() throws Exception {
    final RebuildIndexStatement idx = (RebuildIndexStatement) new SQLAntlrParser(null).parse(
        "REBUILD INDEX Foo WITH batchSize = 1000, maxAttempts = 5, statsOnly = true");
    assertThat(idx.settings).hasSize(3);

    final ImportDatabaseStatement imp = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
        "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");
    assertThat(imp.settings).hasSize(2);
  }

  /**
   * Found while addressing code review on this very PR, in the same sweep as item 3 but on a statement node rather
   * than an expression node: {@code ImportDatabaseStatement#equals()} only compared {@code url}, and
   * {@code ExportDatabaseStatement#getIdentityElements()} only listed {@code url} - so two statements with the same
   * URL but DIFFERENT {@code WITH} settings compared equal. For {@code IMPORT DATABASE} this was the sharper of the
   * two: it has no URL at all for a CSV import, so the settings ARE the statement.
   * {@link BackupDatabaseStatement} already included {@code settings} in its identity; the other two now match it.
   */
  @Test
  void importAndExportStatementsWithDifferentSettingsAreNotEqual() throws Exception {
    final ImportDatabaseStatement importOne = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
        "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true");
    final ImportDatabaseStatement importTwo = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
        "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = false");
    assertThat(importOne).as("same URL, different settings").isNotEqualTo(importTwo);
    assertThat((ImportDatabaseStatement) new SQLAntlrParser(null).parse("IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true"))
        .as("but the same statement is still the same").isEqualTo(importOne);

    final ExportDatabaseStatement exportOne = (ExportDatabaseStatement) new SQLAntlrParser(null).parse(
        "EXPORT DATABASE file://foo.bar WITH format = 'graphml'");
    final ExportDatabaseStatement exportTwo = (ExportDatabaseStatement) new SQLAntlrParser(null).parse(
        "EXPORT DATABASE file://foo.bar WITH format = 'jsonl'");
    assertThat(exportOne).as("same URL, different settings").isNotEqualTo(exportTwo);
    assertThat(exportOne.hashCode()).isNotEqualTo(exportTwo.hashCode());
  }

  /**
   * Item 4: {@link SimpleNode#equals(Object)} indexed the OTHER node's identity array by THIS node's length, with no
   * guard. Safe today because {@code getClass() == other.getClass()} means both arrays come from the same override,
   * and every override returns a fixed-length array literal - but nothing enforces that invariant. This subclass
   * builds its array conditionally, the shape the issue calls out as the one that would otherwise throw
   * {@link ArrayIndexOutOfBoundsException} out of {@code equals()}.
   */
  private static final class VariableArityNode extends SimpleNode {
    private final Object[] elements;

    private VariableArityNode(final Object... elements) {
      this.elements = elements;
    }

    @Override
    protected Object[] getIdentityElements() {
      return elements;
    }
  }

  @Test
  void equalsGuardsAgainstAVariableLengthIdentityArray() {
    final VariableArityNode shortOne = new VariableArityNode("a");
    final VariableArityNode longOne = new VariableArityNode("a", "b");

    assertThat(shortOne).as("different arity is never equal, and must not throw").isNotEqualTo(longOne);
    assertThat(longOne).isNotEqualTo(shortOne);
    assertThat(shortOne.hashCode()).as("hashCode still has to be computable").isEqualTo(Objects.hash("a"));
  }
}
