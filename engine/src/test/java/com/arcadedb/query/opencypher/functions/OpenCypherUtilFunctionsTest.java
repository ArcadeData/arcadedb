/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.query.opencypher.functions.util.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for OpenCypher utility functions.
 */
class OpenCypherUtilFunctionsTest {

  // ============ UtilMd5 tests ============

  @Test
  void utilMd5Basic() {
    final UtilMd5 fn = new UtilMd5();
    assertThat(fn.getName()).isEqualTo("util.md5");

    // Known MD5 hash of "hello"
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("5d41402abc4b2a76b9719d911017c592");
  }

  @Test
  void utilMd5EmptyString() {
    final UtilMd5 fn = new UtilMd5();

    // Known MD5 hash of empty string
    final String result = (String) fn.execute(new Object[]{""}, null);
    assertThat(result).isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
  }

  @Test
  void utilMd5NullHandling() {
    final UtilMd5 fn = new UtilMd5();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ UtilSha1 tests ============

  @Test
  void utilSha1Basic() {
    final UtilSha1 fn = new UtilSha1();
    assertThat(fn.getName()).isEqualTo("util.sha1");

    // Known SHA1 hash of "hello"
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
  }

  @Test
  void utilSha1NullHandling() {
    final UtilSha1 fn = new UtilSha1();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ UtilSha256 tests ============

  @Test
  void utilSha256Basic() {
    final UtilSha256 fn = new UtilSha256();
    assertThat(fn.getName()).isEqualTo("util.sha256");

    // Known SHA256 hash of "hello"
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
  }

  @Test
  void utilSha256NullHandling() {
    final UtilSha256 fn = new UtilSha256();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ UtilSha512 tests ============

  @Test
  void utilSha512Basic() {
    final UtilSha512 fn = new UtilSha512();
    assertThat(fn.getName()).isEqualTo("util.sha512");

    // Known SHA512 hash of "hello"
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043");
  }

  @Test
  void utilSha512NullHandling() {
    final UtilSha512 fn = new UtilSha512();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ Hash consistency tests ============

  @Test
  void hashFunctionsAreConsistent() {
    final UtilMd5 md5 = new UtilMd5();
    final UtilSha1 sha1 = new UtilSha1();
    final UtilSha256 sha256 = new UtilSha256();

    final String input = "test input";

    // Same input should always produce same output
    assertThat(md5.execute(new Object[]{input}, null))
        .isEqualTo(md5.execute(new Object[]{input}, null));
    assertThat(sha1.execute(new Object[]{input}, null))
        .isEqualTo(sha1.execute(new Object[]{input}, null));
    assertThat(sha256.execute(new Object[]{input}, null))
        .isEqualTo(sha256.execute(new Object[]{input}, null));
  }

  @Test
  void hashFunctionsProduceDifferentOutputs() {
    final UtilMd5 md5 = new UtilMd5();
    final UtilSha1 sha1 = new UtilSha1();
    final UtilSha256 sha256 = new UtilSha256();

    final String input = "hello";

    final String md5Result = (String) md5.execute(new Object[]{input}, null);
    final String sha1Result = (String) sha1.execute(new Object[]{input}, null);
    final String sha256Result = (String) sha256.execute(new Object[]{input}, null);

    // Different algorithms should produce different hashes
    assertThat(md5Result).isNotEqualTo(sha1Result);
    assertThat(sha1Result).isNotEqualTo(sha256Result);
    assertThat(md5Result).isNotEqualTo(sha256Result);
  }

  @Test
  void hashLengths() {
    final UtilMd5 md5 = new UtilMd5();
    final UtilSha1 sha1 = new UtilSha1();
    final UtilSha256 sha256 = new UtilSha256();
    final UtilSha512 sha512 = new UtilSha512();

    final String input = "test";

    // Check expected hash lengths (in hex characters)
    assertThat(((String) md5.execute(new Object[]{input}, null)).length()).isEqualTo(32);    // 128 bits
    assertThat(((String) sha1.execute(new Object[]{input}, null)).length()).isEqualTo(40);   // 160 bits
    assertThat(((String) sha256.execute(new Object[]{input}, null)).length()).isEqualTo(64); // 256 bits
    assertThat(((String) sha512.execute(new Object[]{input}, null)).length()).isEqualTo(128); // 512 bits
  }

  // ============ Metadata tests ============

  @Test
  void utilFunctionsMetadata() {
    final UtilMd5 md5 = new UtilMd5();
    assertThat(md5.getMinArgs()).isEqualTo(1);
    assertThat(md5.getMaxArgs()).isEqualTo(1);
    assertThat(md5.getDescription()).contains("MD5");

    final UtilSha256 sha256 = new UtilSha256();
    assertThat(sha256.getMinArgs()).isEqualTo(1);
    assertThat(sha256.getMaxArgs()).isEqualTo(1);
    assertThat(sha256.getDescription()).contains("SHA-256");
  }

  // ============ Non-string input tests ============

  @Test
  void hashFunctionsConvertNonStringsToString() {
    final UtilMd5 md5 = new UtilMd5();

    // Numbers should be converted to string before hashing
    final String hashOfInt = (String) md5.execute(new Object[]{42}, null);
    final String hashOfString = (String) md5.execute(new Object[]{"42"}, null);

    assertThat(hashOfInt).isEqualTo(hashOfString);
  }
}
