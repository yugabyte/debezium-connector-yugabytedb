package io.debezium.connector.yugabytedb.util;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.connection.HashPartition;
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
public class YugabyteDBgRPCConnectorUtilsTest extends YugabyteDBContainerTestBase {
	// We can use an empty pair here to signify the tablet ranges since those values will not even be
	// used. This test class is just to verify the grouping.
	private final Pair<String, String> emptyPair = new ImmutablePair<>("", "");
	private final byte[] emptyByteArray = new byte[0];

	@Test
	public void allColocatedTablesBelongToSameTablet() throws Exception {
		HashPartition pair1 = new HashPartition("table1", "same_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair2 = new HashPartition("table2", "same_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair3 = new HashPartition("table3", "same_tablet", emptyByteArray, emptyByteArray);

		List<HashPartition> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);

		// A random number of groups.
		final int numberGroups = 2;

		List<List<HashPartition>> groupedTablets =
			YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, numberGroups);

		// Since all the tablets are the same, we should be getting only 1 batch i.e. the size of
		// grouped tablets would be 1.
		assertEquals(1, groupedTablets.size());
	}

	@ParameterizedTest(name = "Equal tablets as groups: {0}")
	@ValueSource(booleans = {true, false})
	public void someTablesBelongToDifferentTablet(boolean equalTabletsAsGroups) {
		HashPartition pair1 = new HashPartition("table1", "same_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair2 = new HashPartition("table2", "same_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair3 = new HashPartition("table3", "different_tablet", emptyByteArray, emptyByteArray);

		List<HashPartition> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);

		final int numGroups = equalTabletsAsGroups ? 2 : 1;

		List<List<HashPartition>> groupedTablets =
			YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, numGroups);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(numGroups, groupedTablets.size());
	}

	@ParameterizedTest(name = "All tablets to one group: {0}")
	@ValueSource(booleans = {true, false})
	public void higherTabletsLowerGroups(boolean allTabletsToOneGroup) {
		HashPartition pair1 = new HashPartition("table1", "tablet_1", emptyByteArray, emptyByteArray);
		HashPartition pair2 = new HashPartition("table2", "tablet_1", emptyByteArray, emptyByteArray);
		HashPartition pair3 = new HashPartition("table3", "tablet_2", emptyByteArray, emptyByteArray);
		HashPartition pair4 = new HashPartition("table4", "tablet_2", emptyByteArray, emptyByteArray);
		HashPartition pair5 = new HashPartition("table5", "tablet_3", emptyByteArray, emptyByteArray);
		HashPartition pair6 = new HashPartition("table6", "tablet_3", emptyByteArray, emptyByteArray);
		HashPartition pair7 = new HashPartition("table7", "tablet_4", emptyByteArray, emptyByteArray);

		List<HashPartition> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);
		pairList.add(pair4);
		pairList.add(pair5);
		pairList.add(pair6);
		pairList.add(pair7);

		final int numGroups = allTabletsToOneGroup ? 1 : 2;
		List<List<HashPartition>> groupedTablets =
			YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, numGroups);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(numGroups, groupedTablets.size());
	}

	@Test
	public void multipleColocatedTabletsPresent() {
		HashPartition pair1 = new HashPartition("table1", "same_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair2 = new HashPartition("table2", "same_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair3 = new HashPartition("table3", "different_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair4 = new HashPartition("table4", "different_tablet", emptyByteArray, emptyByteArray);
		HashPartition pair5 = new HashPartition("table5", "different_tablet", emptyByteArray, emptyByteArray);

		List<HashPartition> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);
		pairList.add(pair4);
		pairList.add(pair5);

		// A random number of groups.
		final int numberGroups = 5;

		List<List<HashPartition>> groupedTablets =
			YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, numberGroups);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(2, groupedTablets.size());
	}

	@Test
	public void throwExceptionOnInvalidGroupSize() {
		HashPartition pair1 = new HashPartition("table1", "same_tablet", emptyByteArray, emptyByteArray);

		List<HashPartition> pairList = new ArrayList<>();
		pairList.add(pair1);

		// 0 is an invalid group size.
		final int numberGroups = 0;

		try {
			List<List<HashPartition>> groupedTablets =
				YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, numberGroups);
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
			assertTrue(e.getMessage().contains("Number of groups must be positive"));
		}
	}

	@Test
	public void throwExceptionOnEmptyList() {
		List<HashPartition> pairList = new ArrayList<>();

		// 0 is an invalid group size.
		final int numberGroups = 1;

		try {
			List<List<HashPartition>> groupedTablets =
				YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, numberGroups);
		} catch (Exception e) {
			assertTrue(e instanceof IllegalStateException);
			assertTrue(e.getMessage().contains("Elements to be grouped must be positive"));
		}
	}

	@ParameterizedTest(name = "{0} tasks")
	@ValueSource(ints = {1, 2, 3, 4, 5})
	public void allNonColocatedTablets(int maxTasks) {
		HashPartition pair1 = new HashPartition("table1", "tablet1", emptyByteArray, emptyByteArray);
		HashPartition pair2 = new HashPartition("table2", "tablet2", emptyByteArray, emptyByteArray);
		HashPartition pair3 = new HashPartition("table3", "tablet3", emptyByteArray, emptyByteArray);
		HashPartition pair4 = new HashPartition("table4", "tablet4", emptyByteArray, emptyByteArray);
		HashPartition pair5 = new HashPartition("table5", "tablet5", emptyByteArray, emptyByteArray);

		List<HashPartition> pairList = new ArrayList<>();
		pairList.add(pair1);
		pairList.add(pair2);
		pairList.add(pair3);
		pairList.add(pair4);
		pairList.add(pair5);

		List<List<HashPartition>> groupedTablets =
			YugabyteDBConnectorUtils.groupPartitionsSmartly(pairList, maxTasks);

		// Since all the tablets are NOT the same, we should be getting only 2 batches
		// i.e. the size of grouped tablets would be 2.
		assertEquals(maxTasks, groupedTablets.size());
	}
}
