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
package com.datastax.oss.sink;

import static com.datastax.oss.sink.TaskStateManager.TaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskStateManagerTest {

  private static final Runnable NO_OP_RUNNABLE = () -> {};
  private ExecutorService executorService;
  private TaskStateManager taskStateManager;

  @BeforeEach
  void setUp() {
    executorService = Executors.newFixedThreadPool(2);
    taskStateManager = new TaskStateManager();
  }

  @AfterEach
  void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  void shouldRunTaskAndEndInWaitState() {
    // given
    //

    // when
    taskStateManager.waitRunTransitionLogic(NO_OP_RUNNABLE);

    // then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.WAIT);
  }

  @Test
  void shouldEnterRunStateWhenRunning()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Verify that when the task state manager is running an action, its state is RUN.

    // given

    // Use two latches: one to indicate that an action is running, another to stall the action
    // from completing until our main thread is ready.
    CountDownLatch inRunActionLatch = new CountDownLatch(1);
    CountDownLatch endRunActionLatch = new CountDownLatch(1);

    // when
    Future<?> runFuture =
        executorService.submit(
            () ->
                taskStateManager.waitRunTransitionLogic(
                    () -> {
                      inRunActionLatch.countDown();
                      try {
                        endRunActionLatch.await();
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    }));

    // then
    inRunActionLatch.await();
    TaskState state = taskStateManager.state.get();
    endRunActionLatch.countDown();
    waitFuture(runFuture);
    TaskState stateAfter = taskStateManager.state.get();

    assertThat(state).isEqualTo(TaskState.RUN);
    assertThat(stateAfter).isEqualTo(TaskState.WAIT);
  }

  @Test
  void shouldStopProperlyEvenIfTheRunLogicFinishedInTheMeantime()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Verify that the following set of events result in a final stop state:
    // 1. Begin running a task.
    // 2. Begin stop action.
    // 3. Allow run-task to complete, resulting in a WAIT state.
    // 4. Allow stop action to complete (transitioning from WAIT to STOP).

    // given
    CountDownLatch stopLatch = new CountDownLatch(1);
    CountDownLatch inRunActionLatch = new CountDownLatch(1);
    CountDownLatch runLatch = new CountDownLatch(1);

    // when
    Future<?> runFuture =
        executorService.submit(
            () ->
                taskStateManager.waitRunTransitionLogic(
                    () -> {
                      try {
                        inRunActionLatch.countDown();
                        stopLatch.await();
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    }));
    inRunActionLatch.await();
    Future<?> stopFuture =
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

    // then

    // Wait for runFuture to complete, guaranteeing that the task state manager enters the WAIT
    // state.
    waitFuture(runFuture);
    runLatch.countDown();
    waitFuture(stopFuture);
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.STOP);
  }

  @Test
  void shouldTransitionFromWaitToStopWithoutActionWithCallback() {
    // Verify that when the task state manager is in the WAIT state, telling
    // it to stop does *not* run the action, but *does* run the callback.
    //
    // NOTE: We have a test that shows that running an action ultimately leads to a WAIT
    // state, so this covers the case where actions have run before as well as when no
    // action has run before.

    // given
    Runnable actionMock = mock(Runnable.class);
    Runnable callbackMock = mock(Runnable.class);

    // when
    taskStateManager.toStopTransitionLogic(actionMock, callbackMock);

    // then
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.STOP);
    verify(actionMock, never()).run();
    verify(callbackMock, times(1)).run();
  }

  @Test
  void shouldTransitionFromRunToStopWithActionWithCallback()
      throws InterruptedException, TimeoutException, ExecutionException {
    // given

    // Use two latches: one to indicate that an action is running, another to stall the action
    // from completing until our main thread is ready.
    CountDownLatch inRunActionLatch = new CountDownLatch(1);
    CountDownLatch stopLatch = new CountDownLatch(1);
    Runnable stopCallbackMock = mock(Runnable.class);

    // when
    Future<?> runFuture =
        executorService.submit(
            () ->
                taskStateManager.waitRunTransitionLogic(
                    () -> {
                      inRunActionLatch.countDown();
                      try {
                        stopLatch.await();
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    }));

    // Wait for the run-action to be active before submitting the stop-transition
    inRunActionLatch.await();

    Future<?> stopFuture =
        executorService.submit(
            () -> taskStateManager.toStopTransitionLogic(stopLatch::countDown, stopCallbackMock));

    // then
    waitFuture(runFuture);
    waitFuture(stopFuture);
    assertThat(taskStateManager.state.get()).isEqualTo(TaskState.STOP);
    verify(stopCallbackMock, times(1)).run();
  }

  private static void waitFuture(Future<?> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    future.get(3, TimeUnit.SECONDS);
  }
}
