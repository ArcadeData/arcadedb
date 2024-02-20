package com.arcadedb.database;

import java.util.List;

public class Utils {
  public static final String CREATED_BY = "@createdBy";
  public static final String CREATED_DATE = "@createdDate";
  public static final String LAST_MODIFIED_BY = "@lastModifiedBy";
  public static final String LAST_MODIFIED_DATE = "@lastModifiedDate";

  public static final List<String> EXTRA_METADATA_PROPS = List.of(CREATED_BY, CREATED_DATE, LAST_MODIFIED_BY, LAST_MODIFIED_DATE);

  public static boolean isExtraMetadataProp(String property) {
    return EXTRA_METADATA_PROPS.contains(property);
  }
}
