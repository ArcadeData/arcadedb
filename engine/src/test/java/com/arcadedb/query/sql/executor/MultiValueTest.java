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
package com.arcadedb.query.sql.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.utility.MultiIterator;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

class MultiValueTest {


    @Test
    void testMultivaluesClasses() {

        assertThat(MultiValue.isMultiValue(Map.class)).isTrue();
        assertThat(MultiValue.isMultiValue(List.class)).isTrue();
        assertThat(MultiValue.isMultiValue(Set.class)).isTrue();
        assertThat(MultiValue.isMultiValue(Collection.class)).isTrue();
        assertThat(MultiValue.isMultiValue(new Object[]{}.getClass())).isTrue();
        assertThat(MultiValue.isMultiValue(Iterable.class)).isTrue();
        assertThat(MultiValue.isMultiValue(MultiIterator.class)).isTrue();
        assertThat(MultiValue.isMultiValue(ResultSet.class)).isTrue();
    }

    @Test
    void testMultivaluesObjects() {

        assertThat(MultiValue.isMultiValue(Map.of())).isTrue();
        assertThat(MultiValue.isMultiValue(List.of())).isTrue();
        assertThat(MultiValue.isMultiValue(Set.of())).isTrue();
        assertThat(MultiValue.isMultiValue(new Object[]{})).isTrue();
        //iterable
        assertThat(MultiValue.isMultiValue(new EmptyIndexCursor())).isTrue();
        assertThat(MultiValue.isMultiValue(new MultiIterator())).isTrue();
        assertThat(MultiValue.isMultiValue(new InternalResultSet())).isTrue();
    }
    @Test
    void testMultivaluesSize() {

        assertThat(MultiValue.getSize(null)).isEqualTo(0);
        assertThat(MultiValue.getSize("single")).isEqualTo(0);
        assertThat(MultiValue.getSize(Map.of("key","value"))).isEqualTo(1);
        assertThat(MultiValue.getSize(List.of("one"))).isEqualTo(1);
        assertThat(MultiValue.getSize(Set.of("one","two"))).isEqualTo(2);
        assertThat(MultiValue.getSize(new Object[]{})).isEqualTo(0);
        //iterable
        assertThat(MultiValue.getSize(new EmptyIndexCursor())).isEqualTo(0);
        assertThat(MultiValue.getSize(new MultiIterator())).isEqualTo(0);
        assertThat(MultiValue.getSize(new InternalResultSet())).isEqualTo(0);
    }
}
