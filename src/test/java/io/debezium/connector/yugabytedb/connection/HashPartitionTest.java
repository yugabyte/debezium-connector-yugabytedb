package io.debezium.connector.yugabytedb.connection;

import io.debezium.DebeziumException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Basic unit tests to verify functioning of the class {@link HashPartition}
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class HashPartitionTest {
	/*
		If we try to visualise hash ranges on a number line, the following would be the representation:
		start --- one --- two --- three --- end
	 */
	private static final String start = "[]";
	private static final String one = "[55, -6]";
	private static final String two = "[113, -49]";
	private static final String three = "[-51, 113]";
	private static final String end = "[]";

	// Array representation of the partition key for "one"
	private static final byte[] oneArrayRepresentation = {55, -6};

	// Dummy values to be used as table and tablet IDs
	private static final String tableId1 = "3fe122ffe3f24ad39c2cf8a57fa124b3";
	private static final String tableId2 = "ddc122ffe3f24ad39c2cf8a57fa124b3";
	private static final String tabletId1 = "111111ffe3f24ad39c2cf8a57fa124b3";
	private static final String tabletId2 = "222222ffe3f24ad39c2cf8a57fa124b3";

	@ParameterizedTest
	@MethodSource("parameterSourceForChildRanges")
	public void parentShouldContainAllRanges(String childStartKeyStr, String childEndKeyStr) {
		HashPartition parent = HashPartition.from(tableId1, tabletId1, start, end);
		HashPartition child = HashPartition.from(tableId1, tabletId2, childStartKeyStr, childEndKeyStr);

		assertTrue(parent.containsPartition(child));
	}

	@ParameterizedTest
	@MethodSource("parameterSourceForChildRangesWithoutFullRange")
	public void childShouldNotContainParent(String childStartKeyStr, String childEndKeyStr) {
		HashPartition parent = HashPartition.from(tableId1, tabletId1, start, end);
		HashPartition child = HashPartition.from(tableId1, tabletId2, childStartKeyStr, childEndKeyStr);

		assertFalse(child.containsPartition(parent));
	}

	@Test
	public void verifyPartitionsMadeFromArrayAndStringRepresentationHaveEqualKeys() {
		HashPartition a = HashPartition.from(tableId1, tabletId1, start, one);
		HashPartition b = new HashPartition(tableId1, tabletId1, new byte[]{}, oneArrayRepresentation);

		assertEquals(0, HashPartition.compareKey(a.getPartitionKeyStart(), b.getPartitionKeyStart()));
		assertEquals(0, HashPartition.compareKey(a.getPartitionKeyEnd(), b.getPartitionKeyEnd()));
	}

	@Test
	public void verifyPartitionsUnequalForDifferentTables() {
		HashPartition a = HashPartition.from(tableId1, tableId1, start, one);
		HashPartition b = HashPartition.from(tableId2, tabletId2, start, one);

		assertFalse(a.equals(b));
	}

	@Test
	public void verifyPartitionsUnequalForDifferentTablets() {
		HashPartition a = HashPartition.from(tableId1, tabletId1, start, one);

		// The start and end key doesn't matter here as the equality check will fail at the tablet ID itself.
		HashPartition b = HashPartition.from(tableId1, tabletId2, start, one);

		assertFalse(a.equals(b));
	}

	@Test
	public void verifySorting() {
		List<HashPartition> partitionsToBeSorted = new ArrayList<>();

		// Using the same tabletId in all of them will not affect anything as we are not validating
		// the tabletIds.
		HashPartition a = HashPartition.from(tableId1, tabletId1, start, one);
		HashPartition b = HashPartition.from(tableId1, tabletId1, one, two);
		HashPartition c = HashPartition.from(tableId1, tabletId1, two, three);
		HashPartition d = HashPartition.from(tableId1, tabletId1, three, end);

		// Add to the list in a non-ordered fashion.
		partitionsToBeSorted.add(c);
		partitionsToBeSorted.add(b);
		partitionsToBeSorted.add(d);
		partitionsToBeSorted.add(a);

		List<HashPartition> sortedPartitions = List.of(a, b, c, d);

		Collections.sort(partitionsToBeSorted);

		assertEquals(sortedPartitions, partitionsToBeSorted);
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	public void throwAssertionErrorIfAnyPartitionIsMissing(boolean skipEndPartition) {
		List<HashPartition> partitions = new ArrayList<>();

		// Using the same tabletId in all of them will not affect anything as we are not validating
		// the tabletIds.
		HashPartition a = HashPartition.from(tableId1, tabletId1, start, one);
		HashPartition b = HashPartition.from(tableId1, tabletId1, one, two);
		HashPartition c = HashPartition.from(tableId1, tabletId1, two, three);
		HashPartition d = HashPartition.from(tableId1, tabletId1, three, end);

		// Skip end partition and keep the rest of them present OR skip one of the middle partitions
		// depending on the parameter.
		partitions.add(c);
		partitions.add(a);

		if (skipEndPartition) {
			partitions.add(b);
		} else {
			partitions.add(d);
		}

		assertThrows(IllegalStateException.class, () -> HashPartition.validateCompleteRanges(partitions));
	}


	@ParameterizedTest
	@MethodSource("parameterSourceForMultiPartitionRanges")
	public void verifyLogicForContainsPartitionMethod(HashPartition a, HashPartition b,
																										boolean expectedResult) {
		assertEquals(expectedResult, a.containsPartition(b));
	}

	private static Stream<Arguments> parameterSourceForChildRanges() {
		return Stream.of(
			Arguments.of(start, one),
			Arguments.of(one, two),
			Arguments.of(two, three),
			Arguments.of(three, end),
			Arguments.of(one, three),
			Arguments.of(one, end),
			Arguments.of(two, end),
			Arguments.of(start, end)
		);
	}

	private static Stream<Arguments> parameterSourceForChildRangesWithoutFullRange() {
		return Stream.of(
			Arguments.of(start, one),
			Arguments.of(one, two),
			Arguments.of(two, three),
			Arguments.of(three, end),
			Arguments.of(one, three),
			Arguments.of(one, end),
			Arguments.of(two, end)
		);
	}

	private static Stream<Arguments> parameterSourceForMultiPartitionRanges() {
		final HashPartition p_start_1 = getPartition(start, "[42, -86]");
		final HashPartition p_1_2 = getPartition("[42, -86]", "[85, 85]");

		final HashPartition p_2_3 = getPartition("[85, 85]", "[-128, 0]");
		final HashPartition p_3_4 = getPartition("[-128, 0]", "[-86, -86]");
		final HashPartition p_2_4 = getPartition("[85, 85]", "[-86, -86]");

		final HashPartition p_4_5 = getPartition("[-86, -86]", "[-43, 85]");
		final HashPartition p_5_end = getPartition("[-43, 85]", end);

		return Stream.of(
			Arguments.of(p_start_1, p_start_1, true),
			Arguments.of(p_start_1, p_1_2, false),
			Arguments.of(p_start_1, p_2_3, false),
			Arguments.of(p_start_1, p_4_5, false),
			Arguments.of(p_start_1, p_5_end, false),
			Arguments.of(p_1_2, p_start_1, false),
			Arguments.of(p_1_2, p_1_2, true),
			Arguments.of(p_1_2, p_2_3, false),
			Arguments.of(p_1_2, p_3_4, false),
			Arguments.of(p_1_2, p_4_5, false),
			Arguments.of(p_1_2, p_5_end, false),
			Arguments.of(p_2_4, p_start_1, false),
			Arguments.of(p_2_4, p_1_2, false),
			Arguments.of(p_2_4, p_2_3, true),
			Arguments.of(p_2_4, p_3_4, true),
			Arguments.of(p_2_4, p_4_5, false),
			Arguments.of(p_2_4, p_5_end, false),
			Arguments.of(p_4_5, p_start_1, false),
			Arguments.of(p_4_5, p_1_2, false),
			Arguments.of(p_4_5, p_2_3, false),
			Arguments.of(p_4_5, p_3_4, false),
			Arguments.of(p_4_5, p_4_5, true),
			Arguments.of(p_4_5, p_5_end, false),
			Arguments.of(p_5_end, p_start_1, false),
			Arguments.of(p_5_end, p_1_2, false),
			Arguments.of(p_5_end, p_2_3, false),
			Arguments.of(p_5_end, p_3_4, false),
			Arguments.of(p_5_end, p_4_5, false),
			Arguments.of(p_5_end, p_5_end, true)
		);
	}

	/**
	 * Use a dummy tabletId to form partitions.
	 * @param startKey partition start key
	 * @param endKey partition end key
	 * @return a {@link HashPartition} object
	 */
	private static HashPartition getPartition(String startKey, String endKey) {
		return HashPartition.from(tableId1, UUID.randomUUID().toString(), startKey, endKey);
	}
}
