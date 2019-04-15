package com.datastax.kafkaconnector.util;

/**
 * There is no construct available in the java.util.function package that allows caller to specify 3
 * input parameters. The closes one is BiFunction but it is able to apply two arguments. TriFunction
 * is created to overcome that limitation.
 *
 * @param <T> the type of the first argument to the function
 * @param <U> the type of the second argument to the function
 * @param <V> the type of the third argument to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
interface TriFunction<T, U, V, R> {

  /**
   * Applies this function to the given arguments.
   *
   * @param t the first function argument
   * @param u the second function argument
   * @param v the third function argument
   * @return the function result
   */
  R apply(T t, U u, V v);
}
