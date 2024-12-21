package com.arcadedb.utility;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
/**
 * Exclude an interface from Jacoco, which is not able to measure it in the total code coverage.
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public @interface ExcludeFromJacocoGeneratedReport {
}
