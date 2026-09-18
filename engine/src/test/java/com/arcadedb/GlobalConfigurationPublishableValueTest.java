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
package com.arcadedb;

import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GlobalConfiguration#publishableValue(Object)} is the single rule for what a settings report may show.
 * It replaced three divergent copies - {@code GetServerHandler.convertValue}, the MCP
 * {@code GetServerSettingsTool.normalize} and {@code FetchFromSchemaDatabaseStep.convertValue} - of which only
 * the first redacted the credentials embedded in {@code arcadedb.server.defaultDatabases}, so the MCP tool
 * handed them to the model (found reviewing the issue #7784 fix, which is what made the overlay's real value -
 * the one that actually carries credentials - reach those reports).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GlobalConfigurationPublishableValueTest {

  @Test
  void aHiddenSettingIsMaskedWhole() {
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN.publishableValue("s3cr3t")).isEqualTo("*****");
    assertThat(GlobalConfiguration.SERVER_ROOT_PASSWORD.publishableValue("s3cr3t")).isEqualTo("*****");
  }

  @Test
  void defaultDatabasesKeepsItsNamesAndLosesItsPasswords() {
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2]"))
        .isEqualTo("mydb[jay:*****]");
  }

  @Test
  void everyDatabaseAndEveryCredentialInTheValueIsCovered() {
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES
        .publishableValue("one[jay:hunter2,kim:letmein];two[sam:opensesame]"))
        .as("a second credential, and a second database, are not a place a password may survive")
        .isEqualTo("one[jay:*****,kim:*****];two[sam:*****]");
  }

  @Test
  void aDatabaseWithNoCredentialsSurvivesIntactAndKeepsItsSeparator() {
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("plain;mydb[jay:hunter2];other"))
        .as("the old copy dropped the ';' after an entry that carried no credentials, gluing the names together")
        .isEqualTo("plain;mydb[jay:*****];other");
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("")).isEqualTo("");
  }

  @Test
  void theGroupGrantedToAUserSurvivesTheRedaction() {
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2:admins]"))
        .as("who may reach a database with which role is what this report is read for; only the password cannot be shown")
        .isEqualTo("mydb[jay:*****:admins]");
  }

  @Test
  void onlyTheFieldTheServerAuthenticatesWithIsMasked() {
    // A ':' inside a password is not expressible in this format: ArcadeDBServer.parseCredentials authenticates
    // with field 1 and reads field 2 as the group, so "word" below is a GROUP NAME to the server, not a secret.
    // Masking it too would hide a role an operator reads this report for; what the server calls the password is
    // masked, whatever the field count.
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:pass:word]"))
        .isEqualTo("mydb[jay:*****:word]");
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:pass:word:extra]"))
        .as("a field the server ignores entirely is not a secret it could ever have used")
        .isEqualTo("mydb[jay:*****:word:extra]");
  }

  @Test
  void aTrailingStartupCommandIsLeftAloneEvenWhenItContainsABracket() {
    // The credential block closes on the FIRST ']', the way loadDefaultDatabases closes it. Closing on the last
    // one would swallow the {commands} segment into the credentials whenever a restore: path contains a ']'.
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES
        .publishableValue("mydb[jay:hunter2]{restore:/backups/a]b.tar}"))
        .isEqualTo("mydb[jay:*****]{restore:/backups/a]b.tar}");
  }

  @Test
  void aMissingClosingBracketDoesNotLeakTheEntry() {
    // A '[' that is never closed is a MALFORMED credential block, not an absent one. It cannot be split into
    // fields with any confidence about which is the password, so the whole remainder goes. Reachable live:
    // SET SERVER SETTING writes the overlay and only the next startup parses it, and since #7784 these
    // reports read the overlay.
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2"))
        .as("a value the server has not parsed yet is still a value with a password in it")
        .isEqualTo("mydb[*****");
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2;other"))
        .as("entries are independent, so a later one must not lend its bracket to a broken one")
        .isEqualTo("mydb[*****;other");
  }

  @Test
  void anEmptyPasswordIsNotASecretToHide() {
    // The server reads 'jay:' as a reference to an existing user (split(":") drops the trailing empty field,
    // so parseCredentials sees one field), and there is no password in it to replace.
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:]")).isEqualTo("mydb[jay:]");
  }

  @Test
  void aTrailingSeparatorIsNotQuietlyNormalisedAway() {
    // The server tolerates these because its own splits discard them, so they are values an operator really
    // has configured - and a report that drops them is reporting a value that was not set. Replacing one span
    // and copying the rest is what makes that impossible rather than merely unlikely.
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2];"))
        .isEqualTo("mydb[jay:*****];");
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2,]"))
        .isEqualTo("mydb[jay:*****,]");
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay:hunter2:]"))
        .isEqualTo("mydb[jay:*****:]");
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue(";mydb[jay:hunter2]"))
        .isEqualTo(";mydb[jay:*****]");
  }

  @Test
  void anEmptyPasswordBeforeAGroupIsNotReportedAsAConfiguredOne() {
    // The sibling of anEmptyPasswordIsNotASecretToHide, with a group after it: 'jay::admins' has no password
    // either, and "*****" there would tell an operator one is configured where none is.
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay::admins]"))
        .isEqualTo("mydb[jay::admins]");
  }

  @Test
  void aCredentialWithNoPasswordIsLeftAsWritten() {
    assertThat(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue("mydb[jay]")).isEqualTo("mydb[jay]");
  }

  @Test
  void aClassValuedSettingIsRenderedByName() {
    assertThat(GlobalConfiguration.DATE_IMPLEMENTATION.publishableValue(Date.class))
        .isEqualTo("java.util.Date");
  }

  @Test
  void anOrdinarySettingIsPublishedAsItIs() {
    assertThat(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.publishableValue(4242)).isEqualTo(4242);
    assertThat(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.publishableValue(null)).isNull();
  }
}
