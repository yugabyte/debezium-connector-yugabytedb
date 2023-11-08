package io.debezium.connector.yugabytedb.connection;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.AsyncYBClient;
import org.yb.client.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * What we need at the higher level:
 * 1. all the tasks will get a range
 * 2. validate that complete set of tablets fulfil the range
 */
public class HashPartition implements Comparable<HashPartition> {
	private final String tableId;

	private String tabletId;
	private static final Logger LOGGER = LoggerFactory.getLogger(HashPartition.class);
	private final byte[] partitionKeyStart;
	private final byte[] partitionKeyEnd;

	private final byte[] rangeKeyStart;
	private final byte[] rangeKeyEnd;

	final List<Integer> hashBuckets;

	/**
	 * Size of an encoded hash bucket component in a partition key.
	 */
	private static final int ENCODED_BUCKET_SIZE = 4;

	/**
	 * Creates a new partition with the provided start and end keys, and hash buckets.
	 * @param partitionKeyStart the start partition key
	 * @param partitionKeyEnd the end partition key
	 * @param hashBuckets the partition hash buckets
	 */
	public HashPartition(String tableId, String tabletId, byte[] partitionKeyStart, byte[] partitionKeyEnd,
											 List<Integer> hashBuckets) {
		this.tableId = tableId;
		this.partitionKeyStart = partitionKeyStart;
		this.partitionKeyEnd = partitionKeyEnd;
		this.hashBuckets = hashBuckets;
		this.rangeKeyStart = rangeKey(partitionKeyStart, hashBuckets.size());
		this.rangeKeyEnd = rangeKey(partitionKeyEnd, hashBuckets.size());
		this.tabletId = tabletId;
	}

	/**
	 * Get the table ID to which the partition belongs.
	 * @return the table UUID
	 */
	public String getTableId() {
		return tableId;
	}

	public String getTabletId() {
		return tabletId;
	}

	/**
	 * Gets the start partition key.
	 * @return the start partition key
	 */
	public byte[] getPartitionKeyStart() {
		return partitionKeyStart;
	}

	/**
	 * Gets the end partition key.
	 * @return the end partition key
	 */
	public byte[] getPartitionKeyEnd() {
		return partitionKeyEnd;
	}

	/**
	 * Gets the start range key.
	 * @return the start range key
	 */
	public byte[] getRangeKeyStart() {
		return rangeKeyStart;
	}

	/**
	 * Gets the end range key.
	 * @return the end range key
	 */
	public byte[] getRangeKeyEnd() {
		return rangeKeyEnd;
	}

	/**
	 * Gets the partition hash buckets.
	 * @return the partition hash buckets
	 */
	public List<Integer> getHashBuckets() {
		return hashBuckets;
	}

	/**
	 * @return true if the partition is the absolute end partition
	 */
	public boolean isEndPartition() {
		return partitionKeyEnd.length == 0;
	}

	/**
	 * Equality only holds for partitions from the same table. Partition equality only takes into
	 * account the partition keys, since there is a 1 to 1 correspondence between partition keys and
	 * the hash buckets and range keys.
	 *
	 * @return the hash code
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		HashPartition partition = (HashPartition) o;

		if (!this.tableId.equals(partition.tableId)) {
			return false;
		}

		if (!this.tabletId.equals(partition.tabletId)) {
			return false;
		}

		LOGGER.info("Start: {}", Arrays.equals(partitionKeyStart, partition.partitionKeyStart));
		LOGGER.info("End: {}", Arrays.equals(partitionKeyEnd, partition.partitionKeyEnd));

		return Arrays.equals(partitionKeyStart, partition.partitionKeyStart)
						 && Arrays.equals(partitionKeyEnd, partition.partitionKeyEnd);
	}

	/**
	 * The hash code only takes into account the partition keys, since there is a 1 to 1
	 * correspondence between partition keys and the hash buckets and range keys.
	 *
	 * @return the hash code
	 */
	@Override
	public int hashCode() {
		return Objects.hashCode(Arrays.hashCode(partitionKeyStart), Arrays.hashCode(partitionKeyEnd));
	}

	/**
	 * Partition comparison is only reasonable when comparing partitions from the same table, and
	 * since YB does not yet allow partition splitting, no two distinct partitions can have the
	 * same start partition key. Accordingly, partitions are compared strictly by the start partition
	 * key.
	 *
	 * @param other the other partition of the same table
	 * @return the comparison of the partitions
	 */
	@Override
	public int compareTo(HashPartition other) {
		return Bytes.memcmp(this.partitionKeyStart, other.partitionKeyStart);
	}

	/**
	 * Compare two partitions
	 * @param other {@link HashPartition} to compare with
	 * @return true if other partition is contained in current, false otherwise
	 */
	public boolean containsPartition(HashPartition other) {
		if (!this.tableId.equals(other.tableId)) {
			return false;
		}

		return ((this.partitionKeyStart.length == 0) || (compareKey(this.partitionKeyStart, other.partitionKeyStart) <= 0))
						 && ((this.partitionKeyEnd.length == 0) || (compareKey(this.partitionKeyEnd, other.partitionKeyStart) > 0))
						 && ((this.partitionKeyStart.length == 0) || (compareKey(this.partitionKeyStart, other.partitionKeyEnd) <= 0))
						 && ((this.partitionKeyEnd.length == 0) || (compareKey(this.partitionKeyEnd, other.partitionKeyEnd) > 0));
	}

	public boolean isConflictingWith(HashPartition other) {
		if (!this.tableId.equals(other.tableId)) {
			return false;
		}

		boolean isStartConflicting = false;
		boolean isEndConflicting = false;

		if (this.partitionKeyStart.length != 0) {
			isStartConflicting = (compareKey(this.partitionKeyStart, other.partitionKeyStart) > 0)
														 && (compareKey(this.partitionKeyStart, other.partitionKeyEnd) <= 0);
		}

		if (this.partitionKeyEnd.length != 0) {
			isEndConflicting = (compareKey(this.partitionKeyEnd, other.partitionKeyEnd) <= 0)
													 && (compareKey(this.partitionKeyEnd, other.partitionKeyStart) > 0);
		}

		return isStartConflicting || isEndConflicting;
	}

	public static int compareKey(byte[] keyOne, byte[] keyTwo) {
		int sizeOne = keyOne.length;
		int sizeTwo = keyTwo.length;

		final int minLength = Math.min(sizeOne, sizeTwo);

		int r = Bytes.memcmp(keyOne, keyTwo);
		if (r == 0) {
			if (sizeOne < sizeTwo) {
				return -1;
			}

			if (sizeOne > sizeTwo) {
				return 1;
			}
		}

		return r;
	}

	/**
	 * Returns the range key portion of a partition key given the number of buckets in the partition
	 * schema.
	 * @param partitionKey the partition key containing the range key
	 * @param numHashBuckets the number of hash bucket components of the table
	 * @return the range key
	 */
	private static byte[] rangeKey(byte[] partitionKey, int numHashBuckets) {
		int bucketsLen = numHashBuckets * ENCODED_BUCKET_SIZE;
		if (partitionKey.length > bucketsLen) {
			return Arrays.copyOfRange(partitionKey, bucketsLen, partitionKey.length);
		} else {
			return AsyncYBClient.EMPTY_ARRAY;
		}
	}

	// We can also use Bytes.pretty to get a shorter string
	@Override
	public String toString() {
		return String.format("[%s, %s)",
			Arrays.toString(partitionKeyStart),
			Arrays.toString(partitionKeyEnd));
	}

	public static HashPartition from(CdcService.TabletCheckpointPair tabletCheckpointPair) {
		LOGGER.info("tablet while forming partition {}", tabletCheckpointPair.getTabletLocations().getTabletId().toStringUtf8());
		return new HashPartition(tabletCheckpointPair.getTabletLocations().getTableId().toStringUtf8(),
			tabletCheckpointPair.getTabletLocations().getTabletId().toStringUtf8(),
			tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyStart().toByteArray(),
			tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyEnd().toByteArray(),
			tabletCheckpointPair.getTabletLocations().getPartition().getHashBucketsList());
	}

//	public static HashPartition from(CdcService.TabletCheckpointPair tabletCheckpointPair, boolean blankTablet) {
//		return new HashPartition(tabletCheckpointPair.getTabletLocations().getTableId().toStringUtf8(),
//			"",
//			tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyStart().toByteArray(),
//			tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyEnd().toByteArray(),
//			tabletCheckpointPair.getTabletLocations().getPartition().getHashBucketsList());
//	}

	public static HashPartition from(String tableId, String tabletId, String partitionKeyStartStr, String partitionKeyEndStr) {
		return new HashPartition(tableId, tabletId, getByteArray(partitionKeyStartStr), getByteArray(partitionKeyEndStr), new ArrayList<>());
	}

	private static byte[] getByteArray(String str) {
		String[] elements;

		// length 2 means [] only
		if (str.isEmpty() || str.length() == 2) {
			return new byte[0];
		}

		elements = str.substring(1, str.length() - 1).trim().split(", ");

		byte[] byteValues = new byte[elements.length];
		for (int i = 0; i < elements.length; ++i) {
			int intValue = Integer.parseInt(elements[i].trim());
			byteValues[i] = (byte) intValue;
		}

		return byteValues;
	}

}
