/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string.dse;

import com.datastax.dsbulk.engine.internal.codecs.string.StringConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.dse.driver.api.core.codec.DseTypeCodecs;
import com.datastax.dse.driver.api.core.type.geometry.Point;
import java.util.List;

public class StringToPointCodec extends StringConvertingCodec<Point> {

  public StringToPointCodec(List<String> nullStrings) {
    super(DseTypeCodecs.POINT, nullStrings);
  }

  @Override
  public Point externalToInternal(String s) {
    if (isNull(s)) {
      return null;
    }
    return CodecUtils.parsePoint(s);
  }

  @Override
  public String internalToExternal(Point value) {
    if (value == null) {
      return nullString();
    }
    return value.asWellKnownText();
  }
}
