package com.ddsr.window;

/**
 * A demo of window-coGroup function
 * <p><strong>join() method</strong>
 * <p>
 * The join() method is used to perform an inner join between two data streams based on a common key. It produces pairs
 * of elements from both streams that share the same key and fall within a specified time window.
 * <p>
 * Key Characteristics:
 * Inner Join: Only elements with matching keys in both streams are included in the output.
 * <p>
 * Windowed Operation: Requires a time window (e.g., event time, processing time) to define the range of elements to
 * join.
 * <p>
 * Pair Output: The result is a stream of pairs, where each pair contains one element from each input stream that
 * matched the join condition.
 *
 * <p><strong>coGroup() method</strong></p>
 * The coGroup() method is a more general operation that groups elements from two data streams by a common key and
 * allows you to define custom logic for processing the grouped elements. Unlike join(), it does not require a strict
 * one-to-one pairing and can handle cases where elements in one stream do not have a corresponding match in the other
 * stream.
 * <p>
 * Key Characteristics:
 * Grouping Operation: Groups elements from both streams by key but does not enforce a strict pairing.
 * <p>
 * Custom Logic: Allows you to define how grouped elements are processed, making it more flexible than join().
 * <p>
 * Non-Windowed or Windowed: Can be used with or without a time window, depending on the use case.
 *
 * <table border="1">
 *     <caption>Comparison of join() and coGroup()</caption>
 *     <tr>
 *         <th>Feature</th>
 *         <th>join()</th>
 *         <th>coGroup()</th>
 *     </tr>
 *     <tr>
 *         <td>Output</td>
 *         <td>Pairs of matching elements</td>
 *         <td>Custom output based on grouped data</td>
 *     </tr>
 *     <tr>
 *         <td>Join Type</td>
 *         <td>Inner join</td>
 *         <td>Flexible grouping</td>
 *     </tr>
 *     <tr>
 *         <td>Window Requirement</td>
 *         <td>Requires a time window</td>
 *         <td>Can be used with or without a window</td>
 *     </tr>
 *     <tr>
 *         <td>Use Case</td>
 *         <td>Strict one-to-one matching</td>
 *         <td>Custom logic for grouped elements</td>
 *     </tr>
 *     <tr>
 *         <td>Flexibility</td>
 *         <td>Less flexible</td>
 *         <td>More flexible</td>
 *     </tr>
 * </table>
 */
public class WindowCoGroupDemo {
}
