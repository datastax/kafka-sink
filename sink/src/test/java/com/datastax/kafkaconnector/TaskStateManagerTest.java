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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.awaitility.Duration;
import org.junit.jupiter.api.Test;

class TaskStateManagerTest {

  private static final Runnable NO_OP_RUNNABLE = () -> { };

  @Test
  void shouldStartTaskAndEndInWaitState() {
    // given
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    taskStateManager.waitRunTransitionLogic(NO_OP_RUNNABLE);

    // then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.WAIT);
  }

  @Test()
  void shouldStopProperlyEvenIfTheRunLogicFinishedInTheMeantime() {
    // given
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    CountDownLatch stopLatch = new CountDownLatch(1);
    CountDownLatch runLatch = new CountDownLatch(1);
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    executorService.submit(
        () ->
            taskStateManager.waitRunTransitionLogic(
                () -> {
                  try {
                    stopLatch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }));

    executorService.submit(
        () ->
            taskStateManager.toStopTransitionLogic(
                () -> {
                  try {
                    stopLatch.countDown();
                    runLatch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                },
                NO_OP_RUNNABLE));
    runLatch.countDown();
    executorService.submit(() -> taskStateManager.waitRunTransitionLogic(NO_OP_RUNNABLE));

    // then
    executorService.shutdownNow();
    await()
        .atMost(Duration.FIVE_SECONDS)
        .until(() -> taskStateManager.state.get() == TaskState.STOP);
  }

  @Test
  void shouldTransitionToStopIfRunOperationSucceeded() {
    // given
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    taskStateManager.waitRunTransitionLogic(NO_OP_RUNNABLE);
    taskStateManager.toStopTransitionLogic(NO_OP_RUNNABLE, NO_OP_RUNNABLE);

    // then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.STOP);
  }

  @Test
  void shouldStopProperlyEvenIfTheStopOccurredDuringRunState() {
    // given
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    CountDownLatch stopLatch = new CountDownLatch(1);
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    executorService.submit(
        () ->
            taskStateManager.waitRunTransitionLogic(
                () -> {
                  try {
                    stopLatch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }));

    executorService.submit(
        () -> taskStateManager.toStopTransitionLogic(stopLatch::countDown, NO_OP_RUNNABLE));

    // then
    executorService.shutdownNow();
    await()
        .atMost(Duration.FIVE_SECONDS)
        .until(() -> taskStateManager.state.get() == TaskState.STOP);
  }

  @Test
  void shouldInvokeStopCallbackWhenStopTransitionLogicMethodFinished() {
    // given
    Runnable stopCallbackMock = mock(Runnable.class);
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    taskStateManager.waitRunTransitionLogic(NO_OP_RUNNABLE);
    taskStateManager.toStopTransitionLogic(NO_OP_RUNNABLE, stopCallbackMock);

    // then
    verify(stopCallbackMock, times(1)).run();
  }
}
