package io.debezium.connector.yugabytedb.metrics.meters;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.meters.CommonEventMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBCommonEventMeter extends CommonEventMeter {
  private final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBCommonEventMeter.class);
  private final Clock clock;
  private final EventMetadataProvider metadataProvider;

  protected final AtomicLong events = new AtomicLong();
  protected long regularEvents = 0;

  public YugabyteDBCommonEventMeter(Clock clock, EventMetadataProvider metadataProvider) {
    super(clock, metadataProvider);
    this.clock = clock;
    this.metadataProvider = metadataProvider;
  }

  @Override
  public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Operation operation) {
    // TODO Auto-generated method stub
    super.onEvent(source, offset, key, value, operation);
    ++regularEvents;
    long afterIncrementAndGet = events.incrementAndGet();
    LOGGER.info("Regular long value: {}", regularEvents);
    LOGGER.info("After incrementAndGet: {}", afterIncrementAndGet);
    LOGGER.info("After calling onEvent inside YugabyteDBCommonEventMeter, events: {}", events.get());
  }
}
