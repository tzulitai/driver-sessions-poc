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
        return new DriverAvailabilityEvent();
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
