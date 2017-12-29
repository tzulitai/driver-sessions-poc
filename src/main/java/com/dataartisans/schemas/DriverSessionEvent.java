package com.dataartisans.schemas;

/**
 * POJO defining the schema for a joined driver session event.
 */
public class DriverSessionEvent {

    private String driverId;
    private JoinedGlobalState globalState;
    private JoinedAvailabilityState availabilityState;
    private long startTimestamp;
    private long endTimestamp;
    private boolean isAvailable;
    private boolean isOnline;

    public DriverSessionEvent() {}

    public DriverSessionEvent(
            String driverId,
            JoinedGlobalState globalState,
            JoinedAvailabilityState availabilityState,
            long startTimestamp,
            long endTimestamp,
            boolean isAvailable,
            boolean isOnline) {

        this.driverId = driverId;
        this.globalState = globalState;
        this.availabilityState = availabilityState;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.isAvailable = isAvailable;
        this.isOnline = isOnline;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public JoinedGlobalState getGlobalState() {
        return globalState;
    }

    public void setGlobalState(JoinedGlobalState globalState) {
        this.globalState = globalState;
    }

    public JoinedAvailabilityState getAvailabilityState() {
        return availabilityState;
    }

    public void setAvailabilityState(JoinedAvailabilityState availabilityState) {
        this.availabilityState = availabilityState;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public void setIsAvailable(boolean isAvailable) {
        this.isAvailable = isAvailable;
    }

    public boolean getIsAvailable() {
        return this.isAvailable;
    }

    public void setIsOnline(boolean isOnline) {
        this.isOnline = isOnline;
    }

    public boolean getIsOnline() {
        return this.isOnline;
    }
}
