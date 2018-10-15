package com.datastax.kafkaconnector;

import com.datastax.kafkaconnector.state.LifeCycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.kafkaconnector.TaskStateManager.TaskState.*;

class TaskStateManager {
  private static final Logger log = LoggerFactory.getLogger(TaskStateManager.class);
  AtomicReference<TaskState> state;
  private CountDownLatch stopLatch;
  private DseSinkTask dseSinkTask;

  TaskStateManager() {
    state = new AtomicReference<>(WAIT);
    stopLatch = new CountDownLatch(1);
  }

  void waitToRunTransitionLogic(Runnable action) {
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

  void toStopTransitionLogic(Runnable action) {
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
    try {
      if (state != null) {
        if (state.compareAndSet(WAIT, STOP)) {
          // Clean stop; nothing running/in-progress.
          stopLatch.countDown();
          return;
        }
        action.run();
        state.compareAndSet(RUN, STOP);
        state.compareAndSet(WAIT, STOP);
        stopLatch.await();
      } else if (stopLatch != null) {//todo is it possible? stopLatch is init AFTER state
        // There is no state, so we didn't get far in starting up the task. If by some chance
        // there is a stopLatch initialized, decrement it to indicate to any callers that
        // we're done and they need not wait on us.
        stopLatch.countDown();
      }
    } catch (InterruptedException e) {
      // "put" is likely also interrupted, so we're effectively stopped.
      Thread.currentThread().interrupt();
    } finally {
      log.info("Task is stopped.");
      LifeCycleManager.stopTask(dseSinkTask.getInstanceState(), dseSinkTask);
    }
  }

  void setDseSinkTask(DseSinkTask dseSinkTask) {
    this.dseSinkTask = dseSinkTask;
  }


  /**
   * The DseSinkTask can be in one of three states, and shutdown behavior varies depending on the
   * current state.
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
