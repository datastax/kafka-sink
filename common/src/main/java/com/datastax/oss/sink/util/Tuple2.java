/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.util;

import java.util.Objects;

public class Tuple2<_1, _2> {
  public final _1 _1;
  public final _2 _2;

  public Tuple2(_1 _1, _2 _2) {
    this._1 = _1;
    this._2 = _2;
  }

  public static <_1, _2> Tuple2<_1, _2> of(_1 _1, _2 _2) {
    return new Tuple2<>(_1, _2);
  }

  public _1 _1() {
    return _1;
  }

  public _2 _2() {
    return _2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
    return Objects.equals(_1, tuple2._1) && Objects.equals(_2, tuple2._2);
  }

  @Override
  public int hashCode() {

    return Objects.hash(_1, _2);
  }
}
