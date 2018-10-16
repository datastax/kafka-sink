/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.TaskStateManager.TaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Duration;
import org.junit.jupiter.api.Test;

class TaskStateManagerTest {

  @Test
  void shouldStartTaskAndEndInWaitState() {
    // given
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    taskStateManager.waitRunTransitionLogic(() -> {
    });

    // then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.WAIT);
  }

  @Test
  void shouldStopProperlyEvenIfTheRunLogicFinishedInTheMeantime() throws ExecutionException, InterruptedException {
    // given
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    CountDownLatch stopLatch = new CountDownLatch(1);
    CountDownLatch putLatch = new CountDownLatch(1);
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    Future<?> putFuture = executorService.submit(
        () ->
            taskStateManager.waitRunTransitionLogic(
                () -> {
                  try {
                    stopLatch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }));
    Future<?> stopFuture = executorService.submit(
        () ->
            taskStateManager.toStopTransitionLogic(
                () -> {
                  try {
                    stopLatch.countDown();
                    putLatch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                },
                () -> {
                }));
    putFuture.get();
    putLatch.countDown();
    executorService.submit(() -> taskStateManager.waitRunTransitionLogic(() -> {
    })).get();
    stopFuture.get();

    // then
    executorService.shutdown();
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.STOP);
  }
}
