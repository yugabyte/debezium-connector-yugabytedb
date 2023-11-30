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
	private final String tableId;
	private final String tabletId;
	private final byte[] partitionKeyStart;
	private final byte[] partitionKeyEnd;

	public YBTablet(String tableId, String tabletId, byte[] partitionKeyStart, byte[] partitionKeyEnd) {
		this.tableId = tableId;
		this.tabletId = tabletId;
		this.partitionKeyStart = partitionKeyStart;
		this.partitionKeyEnd = partitionKeyEnd;
	}

	public String getTableId() {
		return tableId;
	}

	public String getTabletId() {
		return tabletId;
	}

	public byte[] getPartitionKeyStart() {
		return partitionKeyStart;
	}

	public byte[] getPartitionKeyEnd() {
		return partitionKeyEnd;
	}

	public HashPartition toHashPartition() {
		return new HashPartition(tableId, tabletId, partitionKeyStart, partitionKeyEnd,
														 new ArrayList<>());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		YBTablet that = (YBTablet) obj;

		return this.getTableId().equals(that.getTableId())
				&& this.getTabletId().equals(that.getTabletId());
	}
}
