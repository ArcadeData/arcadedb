package org.apache.tinkerpop.gremlin.util;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class GremlinValueComparatorTest {
  @Test
  void orderabilityOffsetDateTimeAndString() {
        OffsetDateTime date = OffsetDateTime.parse("2026-04-15T18:59:01.493937Z");
        String str = "bar";
        List<Object> list = Arrays.asList(str, date);
        // Orderability priority: Date (3) < String (4)
        Collections.sort(list, GremlinValueComparator.ORDERABILITY);
    assertThat(list.get(0)).isEqualTo(date);
    assertThat(list.get(1)).isEqualTo(str);
    }

  @Test
  void orderabilityMixedDateTypes() {
        OffsetDateTime date1 = OffsetDateTime.parse("2026-04-15T18:59:01Z");
        Date date2 = new Date(date1.toInstant().toEpochMilli() + 1000); // 1 second later
        List<Object> list = Arrays.asList(date2, date1);
        Collections.sort(list, GremlinValueComparator.ORDERABILITY);
    assertThat(list.get(0)).isEqualTo(date1);
    assertThat(list.get(1)).isEqualTo(date2);
    }
}
