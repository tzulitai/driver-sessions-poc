package com.dataartisans;

import com.dataartisans.operators.DriverSessionJoiner;
import com.dataartisans.operators.DriverEventsSorter;
import com.dataartisans.schemas.DriverAvailabilityEvent;
import com.dataartisans.schemas.DriverSessionEvent;
import com.dataartisans.schemas.DriverStateEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;

/**
 * Job entry point.
 */
public class DriverSessionsPocJob {

    private static final Time MAX_OUT_OF_ORDERNESS = Time.hours(1);
    private static final int SORT_BUFFER_SIZE = 5000;
    private static final int CHECKPOINT_INTERVAL_MILLIS = 5000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MILLIS);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<DriverStateEvent> driverStateEvents = env
                .socketTextStream("localhost", 1111) // TODO: just a playground source
                .map(new DriverStateEventParser())
                .assignTimestampsAndWatermarks(new DriverStateEvent.WatermarkExtractor(MAX_OUT_OF_ORDERNESS));

        final DataStream<DriverAvailabilityEvent> driverAvailabilityEvents = env
                .socketTextStream("localhost", 2222) // TODO: just a playground source
                .map(new DriverAvailabilityEventParser())
                .assignTimestampsAndWatermarks(new DriverAvailabilityEvent.WatermarkExtractor(MAX_OUT_OF_ORDERNESS));

        final ConnectedStreams<DriverStateEvent, DriverAvailabilityEvent> connectedDriverEvents =
                driverStateEvents.connect(driverAvailabilityEvents).keyBy("driverId", "driverId");

        final DataStream<Either<DriverStateEvent, DriverAvailabilityEvent>> sortedDriverEventsStream =
                DriverEventsSorter.sort(connectedDriverEvents, SORT_BUFFER_SIZE);

        final DataStream<DriverSessionEvent> joinedDriverSessions = sortedDriverEventsStream
                .keyBy(new KeySelector<Either<DriverStateEvent, DriverAvailabilityEvent>, String>() {
                    @Override
                    public String getKey(Either<DriverStateEvent, DriverAvailabilityEvent> event) throws Exception {
                        return event.isLeft() ? event.left().getDriverId() : event.right().getDriverId();
                    }
                })
                .flatMap(new DriverSessionJoiner());

        joinedDriverSessions.print(); // TODO: just a playground sink

        env.execute();
    }

    public static class DriverStateEventParser implements MapFunction<String, DriverStateEvent> {
        @Override
        public DriverStateEvent map(String s) throws Exception {
            return DriverStateEvent.fromString(s);
        }
    }

    public static class DriverAvailabilityEventParser implements MapFunction<String, DriverAvailabilityEvent> {
        @Override
        public DriverAvailabilityEvent map(String s) throws Exception {
            return DriverAvailabilityEvent.fromString(s);
        }
    }
}
