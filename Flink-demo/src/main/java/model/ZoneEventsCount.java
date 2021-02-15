package model;

import lombok.Builder;
import lombok.Getter;

import static Utils.SerializerProvider.GSON;

@Builder
@Getter
public class ZoneEventsCount {

    private int eastZoneCount;
    private int westZoneCount;
    private int northZoneCount;
    private int southZoneCount;
    private int totalCount;

    public String toString() {
        return GSON.toJson(this);
    }
}
