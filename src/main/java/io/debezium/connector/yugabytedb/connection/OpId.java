package io.debezium.connector.yugabytedb.connection;

import java.util.Arrays;
import java.util.Base64;

import org.yb.cdc.CdcService.CDCSDKCheckpointPB;

import com.google.common.base.Objects;
import org.yb.client.CdcSdkCheckpoint;
import org.yb.client.GetCheckpointResponse;

public class OpId implements Comparable<OpId> {

    private long term;
    private long index;
    private byte[] key;
    private int write_id;
    private long time;

    public OpId(long term, long index, byte[] key, int write_id, long time) {
        this.term = term;
        this.index = index;
        this.key = key;
        this.write_id = write_id;
        this.time = time;
    }

    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public byte[] getKey() {
        return key;
    }

    public int getWrite_id() {
        return write_id;
    }

    public long getTime() {
        return time;
    }

    public static OpId valueOf(String stringId) {
        if (stringId != null && !stringId.isEmpty()) {
            String[] arr = stringId.split(":");
            return new OpId(Long.valueOf(arr[0]),
                    Long.valueOf(arr[1]),
                    Base64.getDecoder().decode(arr[2]),
                    Integer.valueOf(arr[3]),
                    Long.valueOf(arr[4]));
        }
        return null;
    }

    /**
     * toSerString() returns the sequence in the string format "term:index:keyStr:write_id:time"
     * This can be further split on ":" to get the actual values, one can use the following:
     * <pre>
     * {@code
     * String sequenceString = "<term>:<index>:<keyStr>:<write_id>:<time>";
     * String[] splitValues = sequenceString.split(":");
     * int term = splitValues[0];
     * // and so on for other values
     * }
     * </pre>
     */
    public String toSerString() {
        String keyStr = Base64.getEncoder().encodeToString(key);

        return "" + term + ":" + index + ":" + keyStr + ":" + write_id + ":" + time;
    }

    // todo vaibhav: the ending bracket can be removed here
    @Override
    public String toString() {
        return "" +
                "term=" + term +
                ", index=" + index +
                ", key=" + Arrays.toString(key) +
                ", write_id=" + write_id +
                ", time=" + time +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OpId that = (OpId) o;
        return term == that.term && index == that.index && time == that.time
                && write_id == that.write_id && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(term, index, key, write_id, time);
    }

    @Override
    public int compareTo(OpId o) {
        // Unsigned comparison
        if (term != o.term)
            return term + Long.MIN_VALUE < o.term + Long.MIN_VALUE ? -1 : 1;
        else if (index != o.index)
            return index + Long.MIN_VALUE < o.index + Long.MIN_VALUE ? -1 : 1;
        else
            return write_id + Long.MIN_VALUE < o.write_id + Long.MIN_VALUE ? -1 : 1;
    }

    public static OpId from(long term, long index) {
        return new OpId(term, index, "".getBytes(), 0, 0);
    }

    public static OpId from(CDCSDKCheckpointPB checkpoint) {
        return new OpId(checkpoint.getTerm(), checkpoint.getIndex(),
                        checkpoint.getKey().toByteArray(), checkpoint.getWriteId(),
                        checkpoint.getSnapshotTime());
    }

    public static OpId from(CdcSdkCheckpoint checkpoint) {
        return new OpId(checkpoint.getTerm(), checkpoint.getIndex(),
                checkpoint.getKey(), checkpoint.getWriteId(),
                checkpoint.getTime());
    }

    public static OpId from(GetCheckpointResponse response) {
        return new OpId(response.getTerm(), response.getIndex(), response.getSnapshotKey(),
                        -1 /* write_id */ , response.getSnapshotTime());
    }

    public CdcSdkCheckpoint toCdcSdkCheckpoint() {
        return new CdcSdkCheckpoint(this.term, this.index, this.key, this.write_id, this.time);
    }

    /**
     * Verify the equality of OpId with the given {@link CdcSdkCheckpoint}
     * @param checkpoint
     * @return true if the term and index of this {@link OpId} are equal to the ones in
     * {@link CdcSdkCheckpoint}
     */
    public boolean equals(CdcSdkCheckpoint checkpoint) {
        return (this.term == checkpoint.getTerm()) && (this.index == checkpoint.getIndex());
    }
}
