package io.debezium.connector.yugabytedb.connection;

import com.google.common.base.Objects;
import io.debezium.DebeziumException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.AsyncYBClient;
import org.yb.client.Bytes;
import org.yb.client.GetTabletListToPollForCDCResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Partition class to store a representation of a tablet with its ranges. Most of the logic for
 * this class has been taken from
 * <a href="https://github.com/yugabyte/yugabyte-db/blob/master/java/yb-client/src/main/java/org/yb/client/Bytes.java">Bytes.java</a><br><br>
 * <p>
 * Part of the logic to compare the two partitions has been borrowed from
 * <a href="https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/util/slice.h#L311">slice.h</a>
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class HashPartition implements Comparable<HashPartition> {
  private final String tableId;
  private final String tabletId;
  private static final Logger LOGGER = LoggerFactory.getLogger(HashPartition.class);
  private final byte[] partitionKeyStart;
  private final byte[] partitionKeyEnd;
  private final byte[] rangeKeyStart;
  private final byte[] rangeKeyEnd;
  private final List<Integer> hashBuckets;

  /**
   * Size of an encoded hash bucket component in a partition key.
   */
  private static final int ENCODED_BUCKET_SIZE = 4;

  /**
   * Creates a new partition with the provided start and end keys, and hash buckets.
   *
   * @param partitionKeyStart the start partition key
   * @param partitionKeyEnd   the end partition key
   * @param hashBuckets       the partition hash buckets
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
   *
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
   *
   * @return the start partition key
   */
  public byte[] getPartitionKeyStart() {
    return partitionKeyStart;
  }

  /**
   * Gets the end partition key.
   *
   * @return the end partition key
   */
  public byte[] getPartitionKeyEnd() {
    return partitionKeyEnd;
  }

  /**
   * Gets the start range key.
   *
   * @return the start range key
   */
  public byte[] getRangeKeyStart() {
    return rangeKeyStart;
  }

  /**
   * Gets the end range key.
   *
   * @return the end range key
   */
  public byte[] getRangeKeyEnd() {
    return rangeKeyEnd;
  }

  /**
   * Gets the partition hash buckets.
   *
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
   * @return true if the partition is the absolute beginning partition
   */
  public boolean isStartPartition() {
    return partitionKeyStart.length == 0;
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
   *
   * @param other {@link HashPartition} to compare with
   * @return true if other partition is contained in current, false otherwise
   */
  public boolean containsPartition(HashPartition other) {
    if (!this.tableId.equals(other.tableId)) {
      return false;
    }

    /* We know that this partition can contain another for sure in case of following:
       1. If both the partitions have the same start and end key then technically it means they
          represent the same tablet.
       2. If this partition is the single partition then also it will contain the other partition,
          no matter what other partition's boundaries are.
     */
    if ((Arrays.equals(partitionKeyStart, other.partitionKeyStart)
           && Arrays.equals(partitionKeyEnd, other.partitionKeyEnd))
          || (this.isStartPartition() && this.isEndPartition())) {
      return true;
    }

    // If this is the start partition and the flow reaches here then assume that this is not the
    // end partition as the condition has been evaluated already. Now if the other partition being
    // compared is the end partition, we know it cannot be contained in this as this = ["", key)
    // and other = [someOtherKey, "") so it's different, now if that is also false then simply
    // evaluate whether this partition's end key is after the other partition's end key.
    // Note that other partition's start key doesn't matter as it can be anything and still be after
    // the start key of this partition.
    if (this.isStartPartition()) {
      return !other.isEndPartition()
               && (compareKey(this.partitionKeyEnd, other.partitionKeyEnd) > 0);
    }

    // Similar logic as being used for start partition, values are reversed.
    if (this.isEndPartition()) {
      return !other.isStartPartition()
               && (compareKey(this.partitionKeyStart, other.partitionKeyStart) <= 0);
    }

    return (compareKey(this.partitionKeyStart, other.partitionKeyStart) < 0)
             && (compareKey(this.partitionKeyStart, other.partitionKeyEnd) < 0)
             && (compareKey(this.partitionKeyEnd, other.partitionKeyStart) > 0)
             && (compareKey(this.partitionKeyEnd, other.partitionKeyEnd) > 0);
  }

  /**
   * @param other the {@link HashPartition} to compare with
   * @return true if the partitions have conflicting boundaries, false otherwise
   */
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

  private int compareKey(byte[] keyOne, byte[] keyTwo) {
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
   *
   * @param partitionKey   the partition key containing the range key
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
    return String.format("Tablet %s: [%s, %s)",
      tabletId,
      Arrays.toString(partitionKeyStart),
      Arrays.toString(partitionKeyEnd));
  }

  public YBTablet toYBTablet() {
    return new YBTablet(tableId, tabletId, partitionKeyStart, partitionKeyEnd);
  }

  /**
   * @param tabletCheckpointPair a {@link org.yb.cdc.CdcService.TabletCheckpointPair}  from the {@link org.yb.client.GetTabletListToPollForCDCResponse}
   * @return {@link HashPartition}
   */
  public static HashPartition from(CdcService.TabletCheckpointPair tabletCheckpointPair) {
    return new HashPartition(tabletCheckpointPair.getTabletLocations().getTableId().toStringUtf8(),
      tabletCheckpointPair.getTabletLocations().getTabletId().toStringUtf8(),
      tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyStart().toByteArray(),
      tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyEnd().toByteArray(),
      tabletCheckpointPair.getTabletLocations().getPartition().getHashBucketsList());
  }

  public static HashPartition from(String tableId, String tabletId, String partitionKeyStartStr, String partitionKeyEndStr) {
    return new HashPartition(tableId, tabletId, getByteArray(partitionKeyStartStr), getByteArray(partitionKeyEndStr), new ArrayList<>());
  }

  /**
   * Form a {@link List} of hash partitions based on {@link GetTabletListToPollForCDCResponse}
   *
   * @param response of type {@link GetTabletListToPollForCDCResponse}
   * @return a list of {@link HashPartition}
   */
  public static List<HashPartition> from(GetTabletListToPollForCDCResponse response) {
    List<HashPartition> result = new ArrayList<>();

    for (CdcService.TabletCheckpointPair tcp : response.getTabletCheckpointPairList()) {
      result.add(from(tcp));
    }

    return result;
  }

  /**
   * Validate that the partitions we have make up the full exhaustive range of keys.
   *
   * @param hashPartitions a list of all the HashPartitions
   */
  public static void validateCompleteRanges(List<HashPartition> hashPartitions) {
    sort(hashPartitions);

    byte[] nextKey = new byte[0];
    for (HashPartition hashPartition : hashPartitions) {
      assert Arrays.equals(nextKey, hashPartition.getPartitionKeyStart());

      nextKey = hashPartition.getPartitionKeyEnd();
    }
  }

  /**
   * Sort the given list of {@link HashPartition} so that they complete the chain.
   *
   * @param hashPartitions a list of {@link HashPartition}
   */
  public static void sort(List<HashPartition> hashPartitions) {
    List<HashPartition> tempList = new ArrayList<>();

    byte[] nextStartKey = new byte[0];

    // This loop will exit when all the partitions have been found.
    while (tempList.size() != hashPartitions.size()) {
      HashPartition hp = findPartitionByStartKey(hashPartitions, nextStartKey);
      tempList.add(hp);

      nextStartKey = hp.getPartitionKeyEnd();
    }

    Collections.copy(hashPartitions, tempList);
  }

  private static HashPartition findPartitionByStartKey(List<HashPartition> hashPartitions, byte[] startKey) {
    for (HashPartition hp : hashPartitions) {
      if (Arrays.equals(hp.getPartitionKeyStart(), startKey)) {
        return hp;
      }
    }

    throw new DebeziumException("Given start key " + Arrays.toString(startKey)
                                  + " not found in the list of partitions");
  }

  private static byte[] getByteArray(String str) {
    // Length 2 also means it is an empty byte array as it would just contain "[]"
    if (str.isEmpty() || str.length() == 2) {
      return new byte[0];
    }

    String[] elements = str.substring(1, str.length() - 1).trim().split(", ");

    byte[] byteValues = new byte[elements.length];
    for (int i = 0; i < elements.length; ++i) {
      int intValue = Integer.parseInt(elements[i].trim());
      byteValues[i] = (byte) intValue;
    }

    return byteValues;
  }
}
