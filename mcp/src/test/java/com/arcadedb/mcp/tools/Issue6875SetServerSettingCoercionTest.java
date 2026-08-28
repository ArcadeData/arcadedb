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
package com.arcadedb.mcp.tools;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6875, a follow-up to #6837. #6874 stopped {@code set_server_setting} accepting a missing or empty
 * {@code value}; it left a NON-empty value that is not parseable as the setting's declared type alone, so
 * {@code {"key":"arcadedb.asyncWorkerThreads","value":"abc"}} still answered {@code isError: false} and deferred
 * the {@code NumberFormatException} to whichever component read the setting next.
 * <p>
 * The tool now coerces through {@link GlobalConfiguration#coerce(Object)} - the same parse
 * {@link GlobalConfiguration#setValue(Object)} uses - so what it stores is a typed value rather than the caller's
 * text, and what it refuses is what the global setter refuses.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6875SetServerSettingCoercionTest extends BaseGraphServerTest {
  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowAdmin(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void rejectsAValueThatIsNotParseableAsTheSettingType() {
    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", "arcadedb.asyncWorkerThreads").put("value", "abc"), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.asyncWorkerThreads")
        .hasMessageContaining("Integer");

    // the rejected call did not become the one that breaks a later read, on either accessor
    assertThatCode(() -> getServer(0).getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
        .doesNotThrowAnyException();
    assertThat(getServer(0).getConfiguration().hasValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey())).isFalse();
  }

  @Test
  void rejectsAFloatValueThatIsNotANumber() {
    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", GlobalConfiguration.SERVER_METRICS_TRACING_SAMPLING_RATE.getKey())
            .put("value", "half"), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Float");
  }

  /** What the tool stores is the setting's type, so neither accessor has to re-parse a string to read it back. */
  @Test
  void storesACoercedTypedValue() {
    final GlobalConfiguration setting = GlobalConfiguration.SQL_STATEMENT_CACHE;
    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", setting.getKey()).put("value", "500"), config);

      assertThat(result.getString("newValue")).isEqualTo("500");
      assertThat((Object) getServer(0).getConfiguration().getValue(setting)).isInstanceOf(Integer.class).isEqualTo(500);
      assertThat(getServer(0).getConfiguration().getValueAsInteger(setting)).isEqualTo(500);
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }

  /**
   * A size-suffixed value is what {@link GlobalConfiguration#getValueAsLong()} has always read back, so the writer
   * accepts it too, and stores the number it means rather than the text.
   */
  @Test
  void acceptsASizeSuffixedValueForAnIntegralSetting() {
    final GlobalConfiguration setting = GlobalConfiguration.COMMIT_LOCK_TIMEOUT;
    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", setting.getKey()).put("value", "1KB"), config);

      assertThat(getServer(0).getConfiguration().getValueAsLong(setting)).isEqualTo(1024L);
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }

  /** A secret is masked on the way out of the setter, not only out of the getter. */
  @Test
  void masksTheNewValueOfAHiddenSetting() {
    final GlobalConfiguration setting = GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD;
    assertThat(setting.isHidden()).isTrue();

    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", setting.getKey()).put("value", "s3cr3t-not-in-the-response"), config);

      assertThat(result.getString("newValue")).isEqualTo("*****");
      assertThat(result.getString("previousValue")).isEqualTo("*****");
      assertThat(result.toString()).doesNotContain("s3cr3t-not-in-the-response");

      // masked in the response, but still actually stored
      assertThat(getServer(0).getConfiguration().getValueAsString(setting)).isEqualTo("s3cr3t-not-in-the-response");
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }

  /** A String setting takes any text, including one that would not parse as a number. */
  @Test
  void stillAcceptsAnyTextForAStringSetting() {
    final GlobalConfiguration setting = GlobalConfiguration.DATE_TIME_FORMAT;
    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", setting.getKey()).put("value", "yyyy-MM-dd"), config);

      assertThat(result.getString("newValue")).isEqualTo("yyyy-MM-dd");
      assertThat(getServer(0).getConfiguration().getValueAsString(setting)).isEqualTo("yyyy-MM-dd");
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }
}
