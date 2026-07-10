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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.*;

/**
 * Regression test for the SSRF (CWE-918) and arbitrary local file read (CWE-22) hardening of {@code IMPORT DATABASE}.
 */
class ImportSecurityValidatorTest {

  @AfterEach
  void reset() {
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.reset();
    GlobalConfiguration.SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS.reset();
  }

  @Test
  void blocksAwsMetadataEndpoint() {
    // 169.254.169.254 is link-local: the canonical cloud-metadata SSRF target
    assertThatThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://169.254.169.254/latest/meta-data/"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("169.254.169.254");
  }

  @Test
  void blocksLoopbackAndPrivateRanges() {
    assertThatThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://127.0.0.1:8080/x"))
        .isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> ImportSecurityValidator.validateRemoteURL("https://10.0.0.5/internal"))
        .isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://192.168.1.10/x"))
        .isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://172.16.0.1/x"))
        .isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://0.0.0.0/x"))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void allowsPublicAddress() {
    // A public literal IP does not require DNS resolution and must be allowed
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://8.8.8.8/data.json"));
  }

  @Test
  void allowsAnyRemoteWhenProtectionDisabled() {
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://127.0.0.1/x"));
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateRemoteURL("http://169.254.169.254/x"));
  }

  @Test
  void localPathUnrestrictedByDefault() throws Exception {
    assertThat(GlobalConfiguration.SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS.getValueAsString()).isEmpty();
    // No allow-list configured: backward-compatible, anything is permitted
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateLocalURL("file:///etc/passwd"));
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateLocalURL("/etc/passwd"));
  }

  @Test
  void localPathBlockedOutsideAllowList() {
    GlobalConfiguration.SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS.setValue("./target/imports");
    assertThatThrownBy(() -> ImportSecurityValidator.validateLocalURL("file:///etc/passwd"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
    // Path traversal escaping the allowed dir must also be rejected
    assertThatThrownBy(() -> ImportSecurityValidator.validateLocalURL("file://./target/imports/../../../etc/passwd"))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void localPathAllowedInsideAllowList() throws Exception {
    final File allowedDir = new File("./target/imports");
    allowedDir.mkdirs();
    final File data = new File(allowedDir, "data.csv");
    data.createNewFile();

    GlobalConfiguration.SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS.setValue(allowedDir.getPath());
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateLocalURL("file://" + data.getCanonicalPath()));
  }

  @Test
  void classpathResourcesAlwaysAllowed() {
    GlobalConfiguration.SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS.setValue("./target/imports");
    assertThatNoException().isThrownBy(() -> ImportSecurityValidator.validateLocalURL("classpath://orientdb-export-small.gz"));
  }
}
