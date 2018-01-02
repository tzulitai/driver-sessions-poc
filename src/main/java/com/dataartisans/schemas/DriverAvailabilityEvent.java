package com.dataartisans.schemas;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * POJO defining the schema for a driver availability event.
 */
public class DriverAvailabilityEvent {

    private String driverId;
    private boolean availability;
    private long eventTimestamp;

    public DriverAvailabilityEvent() {}

    public DriverAvailabilityEvent(String driverId, boolean availability, long eventTimestamp) {
        this.driverId = driverId;
        this.availability = availability;
        this.eventTimestamp = eventTimestamp;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public void setAvailability(boolean availability) {
        this.availability = availability;
    }

    public boolean getAvailability() {
        return this.availability;
    }

    public static DriverAvailabilityEvent fromString(String string) {

        // String string = "2018,false,2017-01-01T00:00:00.972411087Z"
        String[] parts = string.split(",");
        String eventTimeString = parts[2].substring(0,19).replace("T", " ");
        Long eventTimestamp = java.sql.Timestamp.valueOf(eventTimeString).getTime();
        String driverId = parts[0];
        boolean availability = Boolean.valueOf(parts[1]);
        return new DriverAvailabilityEvent(driverId, availability, eventTimestamp);

    }

    public static class WatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<DriverAvailabilityEvent> {

        public WatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(DriverAvailabilityEvent driverAvailabilityEvent) {
            return driverAvailabilityEvent.eventTimestamp;
        }
    }
}
