package io.debezium.connector.yugabytedb.consistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rajat Venkatesh
 */
public class Message implements Comparable<Message> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    public final CdcService.CDCSDKProtoRecordPB record;
    public final String tablet;
    public final String txn;
    public final BigInteger commitTime;
    public final BigInteger recordTime;
    public final BigInteger snapShotTime;
    public final long sequence;

    public Message(CdcService.CDCSDKProtoRecordPB record, String tablet, String txn,
                   BigInteger commitTime, BigInteger recordTime, BigInteger snapShotTime, long sequence) {
        this.record = record;
        this.tablet = tablet;
        this.txn = txn;
        this.commitTime = commitTime;
        this.recordTime = recordTime;
        this.snapShotTime = snapShotTime;
        this.sequence = sequence;
    }

    @Override
    public int compareTo(Message o) {
        if (!this.commitTime.equals(o.commitTime)) {
            return this.commitTime.compareTo(o.commitTime);
        } else if (isBegin(this.record.getRowMessage().getOp()) && !isBegin(o.record.getRowMessage().getOp())) {
            return -1;
        } else if (isCommit(this.record.getRowMessage().getOp()) && !isCommit(o.record.getRowMessage().getOp())) {
            return 1;
        } else if (!isBegin(this.record.getRowMessage().getOp()) && isBegin(o.record.getRowMessage().getOp())) {
            return 1;
        } else if (!isCommit(this.record.getRowMessage().getOp()) && isCommit(o.record.getRowMessage().getOp())) {
            return -1;
        } /*else if (this.sequence != o.sequence) {
            return this.sequence < o.sequence ? -1 : 1;
        }*/ else if (notBeginCommit(this, o) && !this.recordTime.equals(o.recordTime)) {
            return this.recordTime.compareTo(o.recordTime);
        }

        LOGGER.info("Returning 0 from compareTo");

        return 0;
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
    public boolean notBeginCommit(Message a, Message b) {
        return !isBegin(a.record.getRowMessage().getOp())
                && !isBegin(b.record.getRowMessage().getOp())
                && !isCommit(a.record.getRowMessage().getOp())
                && !isCommit(b.record.getRowMessage().getOp());
    }

    public boolean isBegin(CdcService.RowMessage.Op op) {
        return op == CdcService.RowMessage.Op.BEGIN;
    }

    public boolean isCommit(CdcService.RowMessage.Op op) {
        return op == CdcService.RowMessage.Op.COMMIT;
    }

    public static class Builder {
        private CdcService.CDCSDKProtoRecordPB record;
        private String tabletId;
        private long snapshotTime;

        private final static AtomicLong sequence = new AtomicLong();

        public Builder setRecord(CdcService.CDCSDKProtoRecordPB record) {
            this.record = record;
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
            return new Message(this.record, this.tabletId,
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
