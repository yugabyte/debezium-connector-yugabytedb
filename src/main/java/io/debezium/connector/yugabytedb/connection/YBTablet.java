package io.debezium.connector.yugabytedb.connection;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Helper class to represent a tablet on the service. Also, the objects of this class will be
 * serialized as strings and sent to the task for further processing.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBTablet implements Serializable {
	private final String tableID;
	private final String tabletID;
	private final byte[] partitionKeyStart;
	private final byte[] partitionKeyEnd;

	public YBTablet(String tableId, String tabletId, byte[] partitionKeyStart, byte[] partitionKeyEnd) {
		this.tableID = tableId;
		this.tabletID = tabletId;
		this.partitionKeyStart = partitionKeyStart;
		this.partitionKeyEnd = partitionKeyEnd;
	}

	public String getTableID() {
		return tableID;
	}

	public String getTabletID() {
		return tabletID;
	}

	public byte[] getPartitionKeyStart() {
		return partitionKeyStart;
	}

	public byte[] getPartitionKeyEnd() {
		return partitionKeyEnd;
	}

	public HashPartition toHashPartition() {
		return new HashPartition(tableID, tabletID, partitionKeyStart, partitionKeyEnd,
														 new ArrayList<>());
	}
}
