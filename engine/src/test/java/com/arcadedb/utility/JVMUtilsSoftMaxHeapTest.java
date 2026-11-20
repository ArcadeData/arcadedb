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
package com.arcadedb.utility;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * {@link JVMUtils#applySoftMaxHeapPercent}: the soft heap limit the server asks the collector for at start
 * ({@code arcadedb.server.softMaxHeapPercent}).
 */
class JVMUtilsSoftMaxHeapTest {
  @Test
  void setsTheSoftLimitAsAShareOfTheCeilingOnlyWhenNoneWasGiven() {
    final HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    final VMOption before = bean.getVMOption("SoftMaxHeapSize");
    assumeThat(before.getOrigin()).as("this JVM was started without a soft limit and none was set since")
        .isIn(VMOption.Origin.DEFAULT, VMOption.Origin.ERGONOMIC);
    final long ceiling = Long.parseLong(bean.getVMOption("MaxHeapSize").getValue());

    try {
      assertThat(JVMUtils.applySoftMaxHeapPercent(0)).isEqualTo(-1);
      assertThat(JVMUtils.applySoftMaxHeapPercent(100)).isEqualTo(-1);
      assertThat(bean.getVMOption("SoftMaxHeapSize").getValue()).isEqualTo(before.getValue());

      final long softMax = JVMUtils.applySoftMaxHeapPercent(60);
      assertThat(softMax).isEqualTo(ceiling / 100 * 60);
      assertThat(bean.getVMOption("SoftMaxHeapSize").getValue()).isEqualTo(Long.toString(softMax));

      // A limit already set, by the operator or an earlier start, is left alone
      assertThat(JVMUtils.applySoftMaxHeapPercent(30)).isEqualTo(-1);
      assertThat(bean.getVMOption("SoftMaxHeapSize").getValue()).isEqualTo(Long.toString(softMax));
    } finally {
      bean.setVMOption("SoftMaxHeapSize", before.getValue());
    }
  }
}
