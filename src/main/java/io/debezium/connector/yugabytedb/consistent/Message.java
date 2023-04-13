package io.debezium.connector.yugabytedb.consistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Assumptions made here are:<br>
 * 1. If commitTime1 == commitTime2 then recordTime1 SHOULD NOT be equal to recordTime2
 * @author Rajat Venkatesh
 */
public class Message implements Comparable<Message> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    public final CdcService.CDCSDKProtoRecordPB record;
    public final String tableId;
    public final String tablet;
    public final String txn;
    public final BigInteger commitTime;
    public final BigInteger recordTime;
    public final BigInteger snapShotTime;
    public final long sequence;

    public Message(CdcService.CDCSDKProtoRecordPB record, String tableId, String tablet, String txn,
                   BigInteger commitTime, BigInteger recordTime, BigInteger snapShotTime, long sequence) {
        this.record = record;
        this.tableId = tableId;
        this.tablet = tablet;
        this.txn = txn;
        this.commitTime = commitTime;
        this.recordTime = recordTime;
        this.snapShotTime = snapShotTime;
        this.sequence = sequence;
    }

    /**
     * Compares two messages based on the following conditions:
     * <ol>
     *   <li>Commit time of the two messages is given top priority in comparison</li>
     *   <li>
     *        If (this is BEGIN and o is NOT BEGIN) or (this is NOT COMMIT and o is COMMIT) then this
     *        would go first in the sorting order
     *   </li>
     *   <li>
     *        If (this is COMMIT and o is NOT COMMIT) or (this is NOT BEGIN and o is BEGIN) then
     *        o would go first in the sorting order, in other words - this would go after o
     *   </li>
     *   <li>
     *        If both the messages, this and o, are neither begin nor commit, then they are compared
     *        based on their record time.
     *   </li>
     * </ol>
     *
     * If all the above conditions fail, then the assumption is that the records are equal and can
     * go either way.
     * @param o the object to be compared.
     * @return -1 if this is less than o, 0 if this is equal or 1 if this is greater than o
     */
    @Override
    public int compareTo(Message o) {
        if (!this.commitTime.equals(o.commitTime)) {
            return this.commitTime.compareTo(o.commitTime);
        } else if ((isBegin(this) && !isBegin(o))
                    || (!isCommit(this) && isCommit(o))) {
            return -1;
        } else if ((isCommit(this) && !isCommit(o))
                    || (!isBegin(this) && isBegin(o))) {
            return 1;
        } else if (notBeginCommit(this, o) && !this.recordTime.equals(o.recordTime)) {
            return this.recordTime.compareTo(o.recordTime);
        }

        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        Message o = ((Message) obj);
        return this.tablet.equals(o.tablet)
                && this.commitTime.compareTo(o.commitTime) == 0
                && this.txn.equals(o.txn)
                && this.recordTime.compareTo(o.recordTime) == 0
                && this.snapShotTime.compareTo(o.snapShotTime) == 0
                && this.sequence == o.sequence
                && this.record.getRowMessage().getOp().name().equals(o.record.getRowMessage().getOp().name());
    }

    @Override
    public String toString() {
        return "Message{" +
                "tablet='" + tablet + '\'' +
                ", txn='" + txn + '\'' +
                ", commitTime=" + commitTime +
                ", recordTime=" + recordTime +
                ", snapShotTime=" + snapShotTime +
                ", sequence=" + sequence +
                ", op=" + this.record.getRowMessage().getOp().name() +
                '}';
    }

    /**
     * Whether both the messages are neither BEGIN not COMMIT
     * @param a first message to be compared
     * @param b another message to be compared
     * @return true if both the messages are neither BEGIN nor COMMIT, false otherwise
     */
    public static boolean notBeginCommit(Message a, Message b) {
        return !isBegin(a) && !isBegin(b) && !isCommit(a) && !isCommit(b);
    }

    public static boolean isBegin(Message m) {
        return m.record.getRowMessage().getOp() == CdcService.RowMessage.Op.BEGIN;
    }

    public static boolean isCommit(Message m) {
        return m.record.getRowMessage().getOp() == CdcService.RowMessage.Op.COMMIT;
    }

    public static class Builder {
        private CdcService.CDCSDKProtoRecordPB record;

        private String tableId;
        private String tabletId;
        private long snapshotTime;

        private final static AtomicLong sequence = new AtomicLong();

        public Builder setRecord(CdcService.CDCSDKProtoRecordPB record) {
            this.record = record;
            return this;
        }

        public Builder setTableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder setTabletId(String tabletId) {
            this.tabletId = tabletId;
            return this;
        }

        public Builder setSnapshotTime(long snapshotTime) {
            this.snapshotTime = snapshotTime;
            return this;
        }

        public Message build() {
            CdcService.RowMessage m = record.getRowMessage();
            return new Message(this.record, this.tableId, this.tabletId,
                    String.valueOf(m.getTransactionId()),
                    toUnsignedBigInteger(m.getCommitTime()), toUnsignedBigInteger(m.getRecordTime()), toUnsignedBigInteger(this.snapshotTime),
                    sequence.incrementAndGet());
        }
    }

    /**
     * Return a BigInteger equal to the unsigned value of the argument.
     * Code taken from <a href="https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/master/src/java.base/share/classes/java/lang/Long.java#L241">Long.java</a>
     */
    protected static BigInteger toUnsignedBigInteger(long i) {
        if (i >= 0L)
            return BigInteger.valueOf(i);
        else {
            int upper = (int) (i >>> 32);
            int lower = (int) i;

            // return (upper << 32) + lower
            return (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32).
                    add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
        }
    }
}
