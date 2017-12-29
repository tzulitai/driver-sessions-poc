package com.dataartisans.operators;

import com.dataartisans.schemas.DriverAvailabilityEvent;
import com.dataartisans.schemas.DriverStateEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * A custom Flink operator that performs a "best-effort" sort on the incoming stream of
 * {@link DriverStateEvent} and {@link DriverAvailabilityEvent}.
 *
 * Incoming driver events are buffered in a min-heap that is checkpointed for fault-tolerance.
 * When the operator receives a watermark, all events with timestamps lower than that watermark is
 * emitted and removed from the min-heap.
 *
 * In the case that the min-heap is full before a watermark is received, the event with the lowest watermark
 * will be emitted pre-maturely.
 *
 * Therefore, the trade-off for a completely sorted stream w.r.t. watermarks lies within the
 * given min-heap buffer size for this operator. I.e., with enough memory and that the buffer size cap
 * is never hit, the operator can sort events completely.
 */
public class DriverEventsSorter
        extends AbstractStreamOperator<Either<DriverStateEvent, DriverAvailabilityEvent>>
        implements TwoInputStreamOperator<DriverStateEvent, DriverAvailabilityEvent, Either<DriverStateEvent, DriverAvailabilityEvent>> {

    public static DataStream<Either<DriverStateEvent, DriverAvailabilityEvent>> sort(
            ConnectedStreams<DriverStateEvent, DriverAvailabilityEvent> connectedStreams,
            int bufferSize) {

        return connectedStreams.transform(
                "DriverEventsSorter",
                new TypeHint<Either<DriverStateEvent, DriverAvailabilityEvent>>() {}.getTypeInfo(),
                new DriverEventsSorter(bufferSize));
    }

    private final int bufferSize;

    private PriorityQueue<Either<DriverStateEvent, DriverAvailabilityEvent>> eventsBuffer;

    private ListState<Either<DriverStateEvent, DriverAvailabilityEvent>> checkpointedEventsBuffer;

    public DriverEventsSorter(int bufferSize) {
        Preconditions.checkArgument(bufferSize > 0);
        this.bufferSize = bufferSize;
    }
    @Override
    public void open() throws Exception {
        super.open();

        this.eventsBuffer = new PriorityQueue<>(bufferSize, new EitherStateAvailabilityComparator());
    }

    @Override
    public void processElement1(StreamRecord<DriverStateEvent> streamRecord) throws Exception {
        if (this.eventsBuffer.size() >= bufferSize) {
            Either<DriverStateEvent, DriverAvailabilityEvent> minTimestampEvent =
                    eventsBuffer.peek();

            if (streamRecord.getTimestamp() < getEitherStateAvailabilityTimestamp(minTimestampEvent)) {
                output.collect(new StreamRecord<>(Either.<DriverStateEvent, DriverAvailabilityEvent>Left(streamRecord.getValue())));
            } else {
                output.collect(new StreamRecord<>(minTimestampEvent));
                this.eventsBuffer.add(Either.<DriverStateEvent, DriverAvailabilityEvent>Left(streamRecord.getValue()));
            }
        }
    }

    @Override
    public void processElement2(StreamRecord<DriverAvailabilityEvent> streamRecord) throws Exception {
        if (this.eventsBuffer.size() >= bufferSize) {
            Either<DriverStateEvent, DriverAvailabilityEvent> minTimestampEvent =
                    eventsBuffer.peek();

            if (streamRecord.getTimestamp() < getEitherStateAvailabilityTimestamp(minTimestampEvent)) {
                output.collect(new StreamRecord<>(Either.<DriverStateEvent, DriverAvailabilityEvent>Right(streamRecord.getValue())));
            } else {
                output.collect(new StreamRecord<>(minTimestampEvent));
                this.eventsBuffer.add(Either.<DriverStateEvent, DriverAvailabilityEvent>Right(streamRecord.getValue()));
            }
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);

        while (eventsBuffer.size() > 0 && getEitherStateAvailabilityTimestamp(eventsBuffer.peek()) < mark.getTimestamp()) {
            output.collect(new StreamRecord<Either<DriverStateEvent, DriverAvailabilityEvent>>(eventsBuffer.poll()));
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        this.checkpointedEventsBuffer = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "eventsBuffer",
                        new TypeHint<Either<DriverStateEvent, DriverAvailabilityEvent>>() {}.getTypeInfo()));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        this.checkpointedEventsBuffer.clear();

        Iterator<Either<DriverStateEvent, DriverAvailabilityEvent>> itr = eventsBuffer.iterator();
        while (itr.hasNext()) {
            this.checkpointedEventsBuffer.add(itr.next());
        }
    }

    private static class EitherStateAvailabilityComparator
            implements Comparator<Either<DriverStateEvent, DriverAvailabilityEvent>> {

        @Override
        public int compare(
                Either<DriverStateEvent, DriverAvailabilityEvent> first,
                Either<DriverStateEvent, DriverAvailabilityEvent> second) {

            long firstEventTimestamp = first.isLeft()
                    ? first.left().getEventTimestamp()
                    : first.right().getEventTimestamp();

            long secondEventTimestamp = second.isLeft()
                    ? second.left().getEventTimestamp()
                    : second.right().getEventTimestamp();

            return Long.compare(firstEventTimestamp, secondEventTimestamp);
        }
    }

    private static long getEitherStateAvailabilityTimestamp(Either<DriverStateEvent, DriverAvailabilityEvent> stateOrAvailability) {
        return stateOrAvailability.isLeft() ? stateOrAvailability.left().getEventTimestamp() : stateOrAvailability.right().getEventTimestamp();
    }
}
