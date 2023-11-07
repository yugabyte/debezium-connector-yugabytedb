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
 */
public class HashPartitionTest {
	/*
		If we try to visualise hash ranges on a number line, the following would be the representation:
		start --- one --- two --- three --- end
	 */
	private static final String start = "";
	private static final String one = "7\\xFA";
	private static final String two = "q\\xCF";
	private static final String three = "\\xCDq";
	private static final String end = "";

	private static final byte[] oneArrayRepresentation = {55, -6};

	private static final String tableId1 = "3fe122ffe3f24ad39c2cf8a57fa124b3";
	private static final String tableId2 = "3fe122ffe3f24ad39c2cf8a57fa124b3";

	@ParameterizedTest
	@MethodSource("parameterSourceForChildRanges")
	public void parentShouldContainAllRanges(String childStartKey, String childEndKey) {
//		HashPartition parent = HashPartition.from(tableId1, "", "");
//		HashPartition child = HashPartition.from(tableId1, childStartKey, childEndKey);
//
//		assertTrue(parent.containsPartition(child));
	}

	@Test
	public void verifyPartitionsMadeFromArrayAndStringRepresentationEqual() {
//		HashPartition a = HashPartition.from(tableId1, start, one);
		HashPartition b = new HashPartition(tableId1, new byte[]{}, oneArrayRepresentation, new ArrayList<>());

		System.out.println("a end key: " + one + " in partition: " + Arrays.toString(a.getPartitionKeyEnd()));
		System.out.println("b end key: " + Bytes.pretty(oneArrayRepresentation) + " in partition: " + Arrays.toString(b.getPartitionKeyEnd()));

//		assertTrue(a.equals(b));
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
}
