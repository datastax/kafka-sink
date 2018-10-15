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
import org.awaitility.Duration;
import org.junit.jupiter.api.Test;

class TaskStateManagerTest {

  @Test
  void shouldStartTaskAndEndInWaitState() {
    // given
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    taskStateManager.waitRunTransitionLogic(() -> {});

    // then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.WAIT);
  }

  @Test
  void shouldStopProperlyEvenIfTheRunLogicFinishedInTheMeantime() {
    // given
    CountDownLatch countDownLatch = new CountDownLatch(1);
    TaskStateManager taskStateManager = new TaskStateManager();

    // when
    Thread putOperation =
        new Thread(
            () ->
                taskStateManager.waitRunTransitionLogic(
                    () -> {
                      try {
                        countDownLatch.await();
                        Thread.sleep(100);
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    }));
    Thread stopOperation =
        new Thread(
            () ->
                taskStateManager.toStopTransitionLogic(
                    () -> {
                      try {
                        countDownLatch.await();
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    },
                    () -> {}));
    putOperation.start();
    stopOperation.start();
    countDownLatch.countDown();

    // then
    await()
        .atMost(Duration.FIVE_SECONDS)
        .until(() -> taskStateManager.state.get() == TaskState.STOP);
  }
}
