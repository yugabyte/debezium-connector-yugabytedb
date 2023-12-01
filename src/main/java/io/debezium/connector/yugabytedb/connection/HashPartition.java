package io.debezium.connector.yugabytedb.connection;

import com.google.common.base.Objects;
import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.YBPartition;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.yb.cdc.CdcService;
import org.yb.client.Bytes;
import org.yb.client.GetTabletListToPollForCDCResponse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Partition class to store a representation of a tablet with its ranges. Most of the logic for
 * this class has been taken from
 * <a href="https://github.com/yugabyte/yugabyte-db/blob/master/java/yb-client/src/main/java/org/yb/client/Bytes.java">Bytes.java</a><br><br>
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class HashPartition implements Comparable<HashPartition>, Serializable {
  // BOUNDARY_KEY is the byte array used to represent the start of the first partition
  // as well as the end of the last partition. BOUNDARY_KEY_STR is the string representation of the
  // same empty byte array.
  public static final byte[] BOUNDARY_KEY = new byte[0];
  public static final String BOUNDARY_KEY_STR = "[]";

  private final String tableId;
  private final String tabletId;
  private final byte[] partitionKeyStart;
  private final byte[] partitionKeyEnd;

  /**
   * Creates a new partition with the provided start and end keys, and hash buckets.
   *
   * @param partitionKeyStart the start partition key
   * @param partitionKeyEnd   the end partition key
   */
  public HashPartition(String tableId, String tabletId, byte[] partitionKeyStart,
                       byte[] partitionKeyEnd) {
    this.tableId = tableId;
    this.partitionKeyStart = partitionKeyStart;
    this.partitionKeyEnd = partitionKeyEnd;
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
   * @return true if the partition is the absolute end partition
   */
  public boolean isEndPartition() {
    return compareKey(partitionKeyEnd, BOUNDARY_KEY) == 0;
  }

  /**
   * @return true if the partition is the absolute beginning partition
   */
  public boolean isStartPartition() {
    return compareKey(partitionKeyStart, BOUNDARY_KEY) == 0;
  }

  /**
   * @return true if this partition is the only partition for a given table
   */
  public boolean isOnlyPartition() {
    return isStartPartition() && isEndPartition();
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

    return compareKey(partitionKeyStart, partition.partitionKeyStart) == 0
             && compareKey(partitionKeyEnd, partition.partitionKeyEnd) == 0;
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
    return compareKey(this.partitionKeyStart, other.partitionKeyStart);
  }

  /**
   * Verify whether the given key is contained within the bounds of this partition. The logic has
   * been borrowed from <a href="https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/util/slice.h#L311">slice.h</a>
   * @param key the key to verify
   * @return true, if the key is within the bounds, false otherwise
   */
  public boolean containsKey(byte[] key) {
    return (this.isStartPartition() || (compareKey(key, this.partitionKeyStart) >= 0))
             && (this.isEndPartition() || (compareKey(key, this.partitionKeyEnd) < 0));
  }

  /**
   * Verify if this partition contains other one.
   * @param other {@link HashPartition} to compare with
   * @return true if other partition is contained in current, false otherwise
   */
  public boolean containsPartition(HashPartition other) {
    if (!this.tableId.equals(other.tableId)) {
      return false;
    }

    /* It is certain that this partition can contain another for sure in case of following:
       1. If both the partitions are equal.
       2. If both the partitions have the same start and end key then technically it means they
          represent the same tablet.
       3. If this partition is the single partition then also it will contain the other partition,
          no matter what other partition's boundaries are.
     */
    if (this.equals(other)
          || (compareKey(partitionKeyStart, other.partitionKeyStart) == 0 && compareKey(partitionKeyEnd, other.partitionKeyEnd) == 0)
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
      return !other.isEndPartition() && containsKey(other.getPartitionKeyEnd());
    }

    // Similar logic as being used for start partition, values are reversed.
    if (this.isEndPartition()) {
      return !other.isStartPartition() && containsKey(other.getPartitionKeyStart());
    }

    return containsKey(other.getPartitionKeyStart()) && containsKey(other.getPartitionKeyEnd());
  }

  /**
   * @param other the {@link HashPartition} to compare with
   * @return true if the partitions have conflicting boundaries, false otherwise
   */
  public boolean isConflictingWith(HashPartition other) {
    if (!this.tableId.equals(other.tableId)) {
      return false;
    }

    return !containsPartition(other)
             && (!containsKey(other.getPartitionKeyStart()) || !containsKey(other.getPartitionKeyEnd()));
  }

  private static int compareKey(byte[] keyOne, byte[] keyTwo) {
    int sizeOne = keyOne.length;
    int sizeTwo = keyTwo.length;

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

  // We can also use Bytes.pretty to get a shorter string
  @Override
  public String toString() {
    return String.format("Tablet %s: [%s, %s)",
      tabletId,
      Arrays.toString(partitionKeyStart),
      Arrays.toString(partitionKeyEnd));
  }

  public YBPartition toYBPartition() {
    return new YBPartition(tableId, tabletId);
  }

  /**
   * @param tabletCheckpointPair a {@link org.yb.cdc.CdcService.TabletCheckpointPair}  from the {@link org.yb.client.GetTabletListToPollForCDCResponse}
   * @return {@link HashPartition}
   */
  public static HashPartition from(CdcService.TabletCheckpointPair tabletCheckpointPair) {
    return new HashPartition(tabletCheckpointPair.getTabletLocations().getTableId().toStringUtf8(),
      tabletCheckpointPair.getTabletLocations().getTabletId().toStringUtf8(),
      tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyStart().toByteArray(),
      tabletCheckpointPair.getTabletLocations().getPartition().getPartitionKeyEnd().toByteArray());
  }

  public static HashPartition from(String tableId, String tabletId, String partitionKeyStartStr, String partitionKeyEndStr) {
    return new HashPartition(tableId, tabletId, getByteArray(partitionKeyStartStr), getByteArray(partitionKeyEndStr));
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
   * @param partitions a list of {@link HashPartition}
   * @return a list of pairs where each pair represents a {@code <tableId, tabletId>}
   */
  public static List<Pair<String, String>> getTableToTabletPairs(List<HashPartition> partitions) {
    List<Pair<String, String>> result = new ArrayList<>();

    for (HashPartition hp : partitions) {
      result.add(new ImmutablePair<>(hp.getTableId(), hp.getTabletId()));
    }

    return result;
  }

  /**
   * Validate that the partitions we have make up the full exhaustive range of keys.
   *
   * @param hashPartitions a list of all the HashPartitions
   */
  public static void validateCompleteRanges(List<HashPartition> hashPartitions) {
    Collections.sort(hashPartitions);

    // We will start by the lower boundary key.
    byte[] nextKey = BOUNDARY_KEY;
    for (HashPartition hashPartition : hashPartitions) {
      assert compareKey(nextKey, hashPartition.getPartitionKeyStart()) == 0;

      nextKey = hashPartition.getPartitionKeyEnd();
    }

    // Verify that the end key of the last partition is the upper boundary key.
    assert compareKey(nextKey, BOUNDARY_KEY) == 0;
  }

  /**
   * Sort the given list of {@link HashPartition} so that they complete the chain. Method meant for
   * testing purposes only, use {@code Collections.sort(hashPartitions)} to perform sorting.
   *
   * @param hashPartitions a list of {@link HashPartition}
   */
  public static void sortChain(List<HashPartition> hashPartitions) {
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

  /**
   * Find the {@link HashPartition} with the provided start key in the list of partitions.
   * @param hashPartitions a {@link List} of {@link HashPartition}
   * @param startKey the partition start key to find
   * @return a {@link HashPartition} whose partition start key matches the startKey
   */
  private static HashPartition findPartitionByStartKey(List<HashPartition> hashPartitions, byte[] startKey) {
    for (HashPartition hp : hashPartitions) {
      if (compareKey(hp.getPartitionKeyStart(), startKey) == 0) {
        return hp;
      }
    }

    throw new DebeziumException("Given start key " + Arrays.toString(startKey)
                                  + " not found in the list of partitions while validating");
  }

  /**
   * Get the byte array from its equivalent string representation. Note that this is not being used
   * in the code anywhere but is particularly useful in writing unit tests.
   * @param str string representation of the byte array
   * @return the byte array object from its string representation
   */
  public static byte[] getByteArray(String str) {
    if (str.isEmpty()) {
      throw new IllegalArgumentException("Invalid representation of a byte array");
    }

    if (str.equals(BOUNDARY_KEY_STR)) {
      return new byte[0];
    }

    // Separate all the elements to further form a byte array using them.
    String[] elements = str.substring(1, str.length() - 1).trim().split(", ");

    byte[] byteValues = new byte[elements.length];
    for (int i = 0; i < elements.length; ++i) {
      int intValue = Integer.parseInt(elements[i].trim());
      byteValues[i] = (byte) intValue;
    }

    return byteValues;
  }
}
