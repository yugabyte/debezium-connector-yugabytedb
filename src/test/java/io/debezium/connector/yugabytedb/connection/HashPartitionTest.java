package io.debezium.connector.yugabytedb.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.client.Bytes;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
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
	private static final String start = "";
	private static final String one = "[55, -6]";
	private static final String two = "[113, -49]";
	private static final String three = "[-51, 113]";
	private static final String end = "";

	private static final byte[] oneArrayRepresentation = {55, -6};

	private static final String tableId1 = "3fe122ffe3f24ad39c2cf8a57fa124b3";
	private static final String tableId2 = "ddc122ffe3f24ad39c2cf8a57fa124b3";
	private static final String tabletId1 = "111111ffe3f24ad39c2cf8a57fa124b3";
	private static final String tabletId2 = "222222ffe3f24ad39c2cf8a57fa124b3";

	@ParameterizedTest
	@MethodSource("parameterSourceForChildRanges")
	public void parentShouldContainAllRanges(String childStartKeyStr, String childEndKeyStr) {
		HashPartition parent = HashPartition.from(tableId1, tabletId1, start, end);
		HashPartition child = HashPartition.from(tableId1, tabletId2, childStartKeyStr, childEndKeyStr);

		assertTrue(parent.contains(child));
	}

	@ParameterizedTest
	@MethodSource("parameterSourceForChildRangesWithoutFullRange")
	public void childShouldNotContainParent(String childStartKeyStr, String childEndKeyStr) {
		HashPartition parent = HashPartition.from(tableId1, tabletId1, start, end);
		HashPartition child = HashPartition.from(tableId1, tabletId2, childStartKeyStr, childEndKeyStr);

		assertFalse(child.contains(parent));
	}

	@Test
	public void verifyPartitionsMadeFromArrayAndStringRepresentationEqual() {
		HashPartition a = HashPartition.from(tableId1, tabletId1, start, one);
		HashPartition b = new HashPartition(tableId1, tabletId1, new byte[]{}, oneArrayRepresentation, new ArrayList<>());

		assertTrue(a.equals(b));
	}

	@Test
	public void verifyPartitionsUnequalForDifferentTables() {
		HashPartition a = HashPartition.from(tableId1, tableId1, start, one);
		HashPartition b = HashPartition.from(tableId2, tabletId2, start, one);

		assertFalse(a.equals(b));
	}

	@Test
	public void verifyPartitionsUnequalForDifferentTablets() {
		HashPartition a = HashPartition.from(tableId1, tableId1, start, one);

		// The start and end key doesn't matter here as the equality check will fail at the tablet ID itself.
		HashPartition b = HashPartition.from(tableId1, tabletId2, start, one);

		assertFalse(a.equals(b));
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
}
