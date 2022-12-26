package io.debezium.connector.yugabytedb.consistent;

import org.yb.cdc.CdcService;

import java.util.concurrent.atomic.AtomicLong;

public class Message implements Comparable<Message> {

    public final CdcService.CDCSDKProtoRecordPB record;
    public final String tablet;
    public final String txn;
    public final long commitTime;
    public final long recordTime;
    public final long snapShotTime;
    public final long sequence;

    public Message(CdcService.CDCSDKProtoRecordPB record, String tablet, String txn,
                   long commitTime, long recordTime, long snapShotTime, long sequence) {
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
        if (this.commitTime != o.commitTime) {
            return this.commitTime < o.commitTime ? -1 : 1;
        } else if (!this.txn.equals(o.txn)) {
            return this.txn.compareTo(o.txn);
        } else if (this.sequence != o.sequence){
            return this.sequence < o.sequence ? -1 : 1;
        } else if (this.recordTime != o.recordTime) {
            return this.recordTime < o.recordTime ? -1 : 1;
        } else if (this.record.getRowMessage().getOp() == CdcService.RowMessage.Op.BEGIN) {
            return -1;
        } else if (this.record.getRowMessage().getOp() == CdcService.RowMessage.Op.COMMIT) {
            return 1;
        }


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
                    m.getCommitTime(), m.getRecordTime(), this.snapshotTime,
                    sequence.incrementAndGet());
        }
    }
}
