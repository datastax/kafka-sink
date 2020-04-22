/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Defines metadata applicable to a record, in particular which field types it contains. */
public interface RecordMetadata {

  /**
   * Returns the type of the given field.
   *
   * @param field the field name.
   * @param cqlType the CQL type associated with the given field.
   * @return the type of the given field, or {@code null} if the field isn't defined in this schema.
   */
  GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType);
}
