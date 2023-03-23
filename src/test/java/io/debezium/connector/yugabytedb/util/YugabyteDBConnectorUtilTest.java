package io.debezium.connector.yugabytedb.util;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests to verify the behaviour of various APIs the connector is supposed to use
 * in order to make sure those APIs are working fine as an individual unit. This test class will
 * always remain a work in progress.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBConnectorUtilTest extends YugabyteDBContainerTestBase {
	@Test
	public void allColocatedTablesBelongToSameTablet() throws Exception {
		Pair<String, String> pair1 = new ImmutablePair<>("table1", "same_tablet");
		Pair<String, String> pair2 = new ImmutablePair<>("table2", "same_tablet");
		Pair<String, String> pair3 = new ImmutablePair<>("table3", "same_tablet");

		List<Pair<String, String>> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);

		// A random number of groups.
		final int numberGroups = 2;

		List<List<Pair<String, String>>> groupedTablets =
				YugabyteDBConnectorUtil.groupPartitionsSmartly(pairList, numberGroups);

		// Since all the tablets are the same, we should be getting only 1 batch i.e. the size of
		// grouped tablets would be 1.
		assertEquals(1, groupedTablets.size());
	}

	@Test
	public void someTablesBelongToDifferentTablet() {
		Pair<String, String> pair1 = new ImmutablePair<>("table1", "same_tablet");
		Pair<String, String> pair2 = new ImmutablePair<>("table2", "same_tablet");
		Pair<String, String> pair3 = new ImmutablePair<>("table3", "different_tablet");

		List<Pair<String, String>> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);

		// A random number of groups.
		final int numberGroups = 5;

		List<List<Pair<String, String>>> groupedTablets =
			YugabyteDBConnectorUtil.groupPartitionsSmartly(pairList, numberGroups);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(2, groupedTablets.size());
	}

	@Test
	public void multipleColocatedTabletsPresent() {
		Pair<String, String> pair1 = new ImmutablePair<>("table1", "same_tablet");
		Pair<String, String> pair2 = new ImmutablePair<>("table2", "same_tablet");
		Pair<String, String> pair3 = new ImmutablePair<>("table3", "different_tablet");
		Pair<String, String> pair4 = new ImmutablePair<>("table4", "different_tablet");
		Pair<String, String> pair5 = new ImmutablePair<>("table5", "different_tablet");

		List<Pair<String, String>> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);
		pairList.add(pair4);
		pairList.add(pair5);

		// A random number of groups.
		final int numberGroups = 5;

		List<List<Pair<String, String>>> groupedTablets =
			YugabyteDBConnectorUtil.groupPartitionsSmartly(pairList, numberGroups);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(2, groupedTablets.size());
	}

	@Test
	public void throwExceptionIfIncorrectGroupSizeSpecified() {
		Pair<String, String> pair1 = new ImmutablePair<>("table1", "same_tablet");
		Pair<String, String> pair2 = new ImmutablePair<>("table2", "same_tablet");
		Pair<String, String> pair3 = new ImmutablePair<>("table3", "different_tablet");

		List<Pair<String, String>> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);

		// Number of groups is equal to the number of colocated tablets.
		final int numberGroups = 1;

		try {
			List<List<Pair<String, String>>> groupedTablets =
				YugabyteDBConnectorUtil.groupPartitionsSmartly(pairList, numberGroups);
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
			assertTrue(e.getMessage().contains("The number of tasks specified should be at least " +
																				 "one greater than the number of colocated tablets"));
		}
	}

	@ParameterizedTest(name = "{0} tasks")
	@ValueSource(ints = {1, 2, 3, 4, 5})
	public void allNonColocatedTablets(int maxTasks) {
		Pair<String, String> pair1 = new ImmutablePair<>("table1", "tablet1");
		Pair<String, String> pair2 = new ImmutablePair<>("table2", "tablet2");
		Pair<String, String> pair3 = new ImmutablePair<>("table3", "tablet3");
		Pair<String, String> pair4 = new ImmutablePair<>("table4", "tablet4");
		Pair<String, String> pair5 = new ImmutablePair<>("table5", "tablet5");

		List<Pair<String, String>> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);
		pairList.add(pair4);
		pairList.add(pair5);

		List<List<Pair<String, String>>> groupedTablets =
			YugabyteDBConnectorUtil.groupPartitionsSmartly(pairList, maxTasks);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(maxTasks, groupedTablets.size());
	}
}
