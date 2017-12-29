package com.dataartisans.operators;

import com.dataartisans.schemas.DriverAvailabilityEvent;
import com.dataartisans.schemas.DriverSessionEvent;
import com.dataartisans.schemas.DriverStateEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

/**
 * Joins driver state & availability events into sessions.
 * This function simply assumes that the stream is already sorted.
 *
 * TODO: Note - the to-do comments below are only a placeholder.
 * TODO: Actual join logic would probably be best implemented as a FSM, with the FSM state checkpointed
 */
public class DriverSessionJoiner
        extends RichFlatMapFunction<Either<DriverStateEvent, DriverAvailabilityEvent>, DriverSessionEvent> {

    private transient ValueState<Either<DriverStateEvent, DriverAvailabilityEvent>> lastSeenEvent;
    private transient ValueState<DriverSessionEvent> lastJoinedSession;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // TODO register states for lastSeenEvent and lastJoinedSession
    }

    @Override
    public void flatMap(
            Either<DriverStateEvent, DriverAvailabilityEvent> event,
            Collector<DriverSessionEvent> collector) throws Exception {
        // TODO join events into sessions, and output a session whenever possible
        // here you can assume that the incoming events are already "best-effort sorted"
    }
}
