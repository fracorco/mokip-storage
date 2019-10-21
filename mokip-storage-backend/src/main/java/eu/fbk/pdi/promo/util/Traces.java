package eu.fbk.pdi.promo.util;

import java.text.ParseException;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Utility functions dealing with traces.
 */
public final class Traces {

    public static List<JsonObject> computePartialTraces(final Iterable<JsonObject> traces) {

        // Explode all traces into partial traces
        List<JsonObject> partialTraces = Lists.newArrayList();
        for (JsonObject trace : traces) {
            partialTraces.addAll(computePartialTraces(trace));
        }

        // Sort partial traces by timestamp
        partialTraces.sort((t1, t2) -> {
            long ts1 = t1.get("sampling_ts").getAsLong();
            long ts2 = t2.get("sampling_ts").getAsLong();
            return Long.compare(ts1, ts2);
        });
        return partialTraces;
    }

    public static List<JsonObject> computePartialTraces(final JsonObject trace) {

        // Retrieve path in BPMN model and array of activities
        final String path = trace.get("path").getAsString();
        final JsonArray activities = trace.get("activities").getAsJsonArray();

        // Track position in the path and current date (as string and timestamp)
        int pathIndex = -1;
        String samplingDate = null;
        long samplingTs = -1;

        // Explode into partial traces by moving along the path, one activity at a time
        final List<JsonObject> partialTraces = Lists.newArrayList();
        for (int i = 0; i < activities.size(); ++i) {

            // Get current activity in path (assumption: they are sorted as per the path)
            final JsonObject activity = (JsonObject) activities.get(i);

            // Update sampling date and ts, checking they are in increasing temporal order
            try {
                samplingDate = activity.get("end").getAsString();
                final long ts = Constants.DATE_FORMAT.parse(samplingDate).getTime();
                Preconditions.checkArgument(ts >= samplingTs);
                samplingTs = ts;
            } catch (final ParseException ex) {
                throw new RuntimeException(ex);
            }

            // Update position in the path, checking it is advancing
            final String id = activity.get("BP_activity_name").getAsString();
            final String token = "_" + id.substring(id.indexOf('.') + 1);
            int index = path.indexOf(token, pathIndex);
            Preconditions.checkArgument(index >= 0);
            index += token.length();
            Preconditions.checkArgument(index > pathIndex);
            pathIndex = index;
            final String partialPath = path.substring(0, pathIndex);

            // Generate partial trace, using partial path and seen activities only
            final JsonObject partialTrace = new JsonObject();
            for (final Entry<String, JsonElement> e : trace.entrySet()) {
                if (e.getKey().equals("path")) {
                    partialTrace.addProperty("path", partialPath);
                } else if (e.getKey().equals("activities")) {
                    final JsonArray partialActivities = new JsonArray();
                    partialTrace.addProperty("step", i + 1);
                    partialTrace.addProperty("sampling_ts", samplingTs);
                    partialTrace.addProperty("sampling_date", samplingDate);
                    partialTrace.add("activities", partialActivities);
                    for (int j = 0; j <= i; ++j) {
                        partialActivities.add(activities.get(j).deepCopy());
                    }
                } else {
                    partialTrace.add(e.getKey(), e.getValue());
                }
            }
            partialTraces.add(partialTrace);
        }

        // Generate a final partial trace covering the whole input trace, if needed
        if (pathIndex < path.length()) {
            trace.addProperty("step", activities.size() + 1);
            trace.addProperty("sampling_ts", samplingTs);
            trace.addProperty("sampling_date", samplingDate);
            trace.add("activities", trace.remove("activities"));
            partialTraces.add(trace);
        }
        return partialTraces;
    }

}
