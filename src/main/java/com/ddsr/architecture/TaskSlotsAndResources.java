package com.ddsr.architecture;

/**
 * Each worker (TaskManager) is a <strong>JVM process</strong>, and may execute one or more subtasks in separate
 * <strong>threads</strong>. To control how many tasks a TaskManager accepts, it has so called task slots (at least one).
 *
 * <p>Each task slot represents a fixed subset of resources of the TaskManager. A TaskManager with three slots,
 * for example, will dedicate 1/3 of its managed memory to each slot. Slotting the resources means that a subtask
 * will not compete with subtasks from other jobs for managed memory, but instead has a certain amount of reserved
 * managed memory. <u>Note that no CPU isolation happens here; currently slots only separate the managed memory
 * of tasks</u>.</p>
 *
 * <p>By adjusting the number of task slots, users can define how subtasks are isolated from each other. Having one slot
 * per TaskManager means that each task group runs in a separate JVM (which can be started in a separate container, for example).
 * Having multiple slots means more subtasks share the same JVM. Tasks in the same JVM share TCP connections (via multiplexing)
 * and heartbeat messages. They may also share data sets and data structures, thus reducing the per-task overhead.</p>
 */
public class TaskSlotsAndResources {


}
