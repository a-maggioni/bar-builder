package it.intre.barbuilder.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class DateUtils {

    private DateUtils() {
    }

    public static Long truncateSeconds(final Long timestamp) {
        return toZonedDateTime(timestamp).truncatedTo(ChronoUnit.MINUTES).toInstant().toEpochMilli();
    }

    public static int getMinuteDifference(final Long firstTimestamp, final Long secondTimestamp) {
        return toZonedDateTime(secondTimestamp).getMinute() - toZonedDateTime(firstTimestamp).getMinute();
    }

    public static ZonedDateTime toZonedDateTime(final Long timestamp) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    }

}
