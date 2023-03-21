package io.debezium.connector.yugabytedb.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YugabyteDBConnectorUtil {
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
	public static List<List<Pair<String, String>>> groupPartitionsSmartly(List<Pair<String, String>> elements, int numGroups) {
		/*
		  TODO Vaibhav: Discussion needed-
		  1. The number of tasks can never be 1 in case we are planning to stream multiple colocated tablets.
		  2. If a colocated tablet contains only one table, it will be considered as non-colocated. (Fix known: to add the YBTable.isColocated check as well)
		 */
		if (elements.size() == 0) {
			throw new IllegalStateException("Elements to be grouped should not be equal to 0");
		}

		if (numGroups <= 0) {
			throw new IllegalArgumentException("Number of groups must be positive");
		}

		List<List<Pair<String, String>>> result = new ArrayList<>();

		// Filter out groups having the same tabletId as value
		// The map will have tabletId -> table1,table2,table3 map
		Map<String, ArrayList<String>> reverseMap = new HashMap<>(
			elements.stream().collect(Collectors.groupingBy(Pair::getValue)).values().stream()
				.collect(Collectors.toMap(
					item -> item.get(0).getValue(),
					item -> new ArrayList<>(
						item.stream()
							.map(Map.Entry::getKey)
							.collect(Collectors.toList())
					))
				));

		// If there are same number of tablets in the grouped reverse map then use the older function
		// to group rather than going to the complicated logic of grouping colocated and non-colocated
		// tablets differently.
		// Note: The keySet of the reverse map will only contain tablets.
		if (reverseMap.keySet().size() == elements.size()) {
			groupPartitions(elements, numGroups, result);
			return result;
		}

		List<Pair<String, String>> nonColocatedPairs = new ArrayList<>();
		for (Map.Entry<String, ArrayList<String>> entry : reverseMap.entrySet()) {
			List<Pair<String, String>> groupList = new ArrayList<>();
			// TODO Vaibhav: This block will also treat colocated tablets having just one table as a non-colocated tablet.
			if (entry.getValue().size() == 1) {
				// This would mean that the tablet is not a colocated tablet and has only one table mapped.
				// Here, we can simply access the first element of the list assuming that there is only one
				// element.
				nonColocatedPairs.add(new ImmutablePair<>(entry.getValue().get(0), entry.getKey() /* tablet ID */));
			} else {
				for (String tableId : entry.getValue()) {
					// entry.getKey will return tablet ID.
					groupList.add(new ImmutablePair<>(tableId, entry.getKey() /* tablet ID */));
				}

				result.add(groupList);
			}
		}

		if (!nonColocatedPairs.isEmpty()) {
			// Process the non colocated pair the regular way of processing.
			if (numGroups - result.size() <= 0) {
				throw new IllegalArgumentException("The number of tasks specified should be at least " +
																					 "one greater than the number of colocated tablets");
			}

			groupPartitions(nonColocatedPairs, numGroups - result.size(), result);
		}

		return result;
	}
}
