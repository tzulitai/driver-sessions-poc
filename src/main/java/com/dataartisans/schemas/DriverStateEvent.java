package com.dataartisans.schemas;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

/**
 * POJO defining the schema for a driver state event.
 */
public class DriverStateEvent {

    private long eventTimestamp;
    private String driverId;
    private DriverState lastState;
    private DriverState currentState;

    public DriverStateEvent() {}

    public DriverStateEvent(long eventTimestamp, String driverId, DriverState lastState, DriverState currentState) {
        this.eventTimestamp = eventTimestamp;
        this.driverId = driverId;
        this.lastState = lastState;
        this.currentState = currentState;
    }

    public DriverState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(DriverState currentState) {
        this.currentState = currentState;
    }

    public DriverState getLastState() {
        return lastState;
    }

    public void setLastState(DriverState lastState) {
        this.lastState = lastState;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }


    public static DriverStateEvent fromString(String string) {
        //String string = "2017-01-01T00:20:14.664315708Z,2018,AVAILABLE,OFFLINE";
        String[] parts = string.split(",");
        String eventTimeString = parts[0].substring(0,19).replace("T", " ");
        Long eventTimestamp = java.sql.Timestamp.valueOf(eventTimeString).getTime();
        String driverId = parts[1];
        DriverState lastState = DriverState.valueOf(parts[2] );
        DriverState currentState = DriverState.valueOf(parts[3]);
        return new DriverStateEvent(eventTimestamp, driverId, lastState, currentState);
    }

    public static class WatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<DriverStateEvent> {

        public WatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(DriverStateEvent driverStateEvent) {
            return driverStateEvent.eventTimestamp;
        }
    }
}
