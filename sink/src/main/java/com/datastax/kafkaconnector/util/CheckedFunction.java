/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import java.io.IOException;

/**
 * Standard java.util.Function does not allow to throw checked exception from the apply call To make
 * it explicit we need a CheckedFunction interface that declares that it throws IOException We are
 * specifying IOException instead of Exception to give caller of that function more information
 * about inner behaviour of apply(). If in the future someone wish to make it more generic by
 * throwing Exception will need to change callers to catch Exception instead of IOException.
 */
@FunctionalInterface
public interface CheckedFunction<T, R> {
  R apply(T t) throws IOException;
}
