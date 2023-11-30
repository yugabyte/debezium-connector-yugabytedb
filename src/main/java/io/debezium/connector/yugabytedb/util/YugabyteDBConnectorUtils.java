package io.debezium.connector.yugabytedb.util;

import io.debezium.connector.yugabytedb.ObjectUtil;
import io.debezium.connector.yugabytedb.connection.HashPartition;
import io.debezium.connector.yugabytedb.connection.YBTablet;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility functions to assist across various stages of flow in the connector.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBConnectorUtils {
	public static <T> void groupPartitions(List<T> elements, int numGroups, List<List<T>> result) {
		if (numGroups <= 0)
			throw new IllegalArgumentException("Number of groups must be positive.");

		List<List<T>> res = new ArrayList<>(numGroups);

		// Each group has either n+1 or n raw partitions
		int perGroup = elements.size() / numGroups;
		int leftover = elements.size() - (numGroups * perGroup);

		int assigned = 0;
		for (int group = 0; group < numGroups; group++) {
			if (assigned == elements.size()) {
				// We need not assign empty groups if we have exhausted the total number of elements.
				break;
			}
			int numThisGroup = group < leftover ? perGroup + 1 : perGroup;
			List<T> groupList = new ArrayList<>(numThisGroup);
			for (int i = 0; i < numThisGroup; i++) {
				groupList.add(elements.get(assigned));
				assigned++;
			}
			res.add(groupList);
		}

		result.addAll(res);
	}

	/**
	 * This grouping function ensures that we group the tablets in a way that each task contains
	 * all the tables of just one colocated tablet. For non-colocated tables, the division of tablets
	 * will be done the regular way.
	 * @param elements a list of pairs where key is tableId and value is tabletId
	 * @param numGroups the total number of groups we should be dividing the tasks to.
	 */
	public static List<List<YBTablet>> groupPartitionsSmartly(
		List<YBTablet> elements, int numGroups) {
		if (elements.size() == 0) {
			throw new IllegalStateException("Elements to be grouped must be positive");
		}

		if (numGroups <= 0) {
			throw new IllegalArgumentException("Number of groups must be positive");
		}

		List<List<YBTablet>> result = new ArrayList<>(numGroups);

		// Filter out groups having the same tabletId as value
		// The map will have tabletId -> table1,table2,table3 map
		Map<String, List<YBTablet>> groupedData = elements.stream()
			.collect(Collectors.groupingBy(YBTablet::getTabletId));

		// If there are same number of tablets in the grouped reverse map then use the older function
		// to group rather than going to the complicated logic of grouping colocated and non-colocated
		// tablets differently.
		// Note: The keySet of the reverse map will only contain tablets.
		if (groupedData.keySet().size() == elements.size()) {
			groupPartitions(elements, numGroups, result);
			return result;
		}

		// Divide tablets into tasks and then form groups based on that.
		List<List<String>> groupedTablets = new ArrayList<>();
		groupPartitions(new ArrayList<>(groupedData.keySet()), numGroups, groupedTablets);

		// Iterate over grouped tablets now.
		// The assumption here is that at this stage, the division of tablets across tasks would be
		// something similar to:
		// 1. Task 1 -
		//    a. tablet_1
		//    b. tablet_2
		//    b. tablet_3
		// 2. Task 2 -
		//    a. tablet_4
		//    b. tablet_5
		// After this, we can simply iterate over the reversed map and just put proper table-tablet
		// pairs to the task list.
		for (List<String> tablets : groupedTablets) {
			List<YBTablet> groupList = new ArrayList<>();
			for (String tablet : tablets) {
				groupList.addAll(groupedData.get(tablet));
			}

			result.add(groupList);
		}

		return result;
	}

	/**
	 * Populate the partition ranges with the {@link HashPartition} objects. Internally, the serialized
	 * string is deserialized to a list object with each element being a {@link Pair} where the key is
	 * a {@link Pair} of <tableId, tabletId> and value is another {@link Pair} of <partitionKeyStart, partitionKeyEnd>
	 * @param serializedString
	 * @param partitionRanges a {@link List} of {@link HashPartition}
	 * @throws IOException if unable to deserialize the string
	 * @throws ClassNotFoundException if unable to deserialize the string
	 */
	public static void populatePartitionRanges(String serializedString, List<HashPartition> partitionRanges)
			throws IOException, ClassNotFoundException {
		List<YBTablet> tableToTabletRanges =
				(List<YBTablet>) ObjectUtil.deserializeObjectFromString(serializedString);

		for (YBTablet tablet : tableToTabletRanges) {
			partitionRanges.add(tablet.toHashPartition());
		}
	}
}
