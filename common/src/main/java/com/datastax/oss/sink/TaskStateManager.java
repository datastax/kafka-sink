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

import static com.datastax.oss.sink.TaskStateManager.TaskState.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TaskStateManager {
  private static final Logger log = LoggerFactory.getLogger(TaskStateManager.class);
  final AtomicReference<TaskState> state;
  private final CountDownLatch stopLatch;

  TaskStateManager() {
    state = new AtomicReference<>(WAIT);
    stopLatch = new CountDownLatch(1);
  }

  void waitRunTransitionLogic(Runnable action) {
    state.compareAndSet(TaskState.WAIT, TaskState.RUN);
    try {
      action.run();
    } finally {
      state.compareAndSet(TaskState.RUN, TaskState.WAIT);
      if (state.get() == STOP) {
        // Task is stopping. Notify the caller of stop() that we're done working.
        stopLatch.countDown();
      }
    }
  }

  void toStopTransitionLogic(Runnable action, Runnable stopCallback) {
    // Stopping has a few scenarios:
    // 1. We're not currently processing records (e.g. we are in the WAIT state).
    //    Just transition to the STOP state and return. Signal stopLatch
    //    since we are effectively the entity declaring that this task is stopped.
    // 2. We're currently processing records (e.g. we are in the RUN state).
    //    Transition to the STOP state and wait for the thread processing records
    //    (e.g. running put()) to signal stopLatch.
    // 3. We're currently in the STOP state. This could mean that no work is occurring
    //    (because a previous call to stop occurred when we were in the WAIT state or
    //    a previous call to put completed and signaled the latch) or that a thread
    //    is running put and hasn't completed yet. Either way, this thread waits on the
    //    latch. If the latch has been opened already, there's nothing to wait for
    //    and we immediately return.
    // 4. The put() method can finish, making transition from RUN to wait after
    //    state.compareAndSet(WAIT, STOP) check
    //    To prevent that we need to double check for CAS WAIT -> STOP
    try {
      if (state.compareAndSet(WAIT, STOP)) {
        // Clean stop; nothing running/in-progress.
        stopLatch.countDown();
        return;
      }
      action.run();
      state.compareAndSet(RUN, STOP);
      if (state.compareAndSet(WAIT, STOP)) {
        // Clean stop; nothing running/in-progress.
        stopLatch.countDown();
        return;
      }
      stopLatch.await();
    } catch (InterruptedException e) {
      // "put" is likely also interrupted, so we're effectively stopped.
      Thread.currentThread().interrupt();
    } finally {
      log.info("Task is stopped.");
      stopCallback.run();
    }
  }

  /**
   * The CassandraSinkTask can be in one of three states, and shutdown behavior varies depending on
   * the current state.
   */
  enum TaskState {
    // Task is waiting for records from the infrastructure
    WAIT,
    // Task is processing a collection of SinkRecords
    RUN,
    // Task is in the process of stopping or is stopped
    STOP
  }
}
