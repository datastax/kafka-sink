package com.datastax.kafkaconnector;

import org.awaitility.Duration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static com.datastax.kafkaconnector.TaskStateManager.TaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

class TaskStateManagerTest {

  @Test
  void shouldStartTaskAndEndInWaitState() {
    //given
    TaskStateManager taskStateManager = new TaskStateManager();

    //when
    taskStateManager.waitToRunTransitionLogic(() -> {
    });

    //then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.WAIT);
  }

  @Test
  void shouldStopProperlyEvenIfTheRunLogicFinishedInTheMeantime() {
    //given
    CountDownLatch countDownLatch = new CountDownLatch(1);
    TaskStateManager taskStateManager = new TaskStateManager();
    DseSinkTask dseSinkTaskMock = mock(DseSinkTask.class);
    taskStateManager.setDseSinkTask(dseSinkTaskMock);

    //when
    Thread putOperation = new Thread(() ->
        taskStateManager.waitToRunTransitionLogic(() -> {
          try {
            countDownLatch.await();
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }));
    Thread stopOperation = new Thread(() ->
        taskStateManager.toStopTransitionLogic(() -> {
          try {
            countDownLatch.await();
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }));
    putOperation.start();
    stopOperation.start();
    countDownLatch.countDown();


    //then
    await()
        .atMost(Duration.FIVE_SECONDS)
        .until(() -> taskStateManager.state.get() == TaskState.STOP);
  }

}