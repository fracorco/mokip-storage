package eu.fbk.pdi.promo.simulation.elements;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;

public class ProcessType {

    private final List<Activity> activities;

    private final Map<String, Integer> emptyPathExecutedActivities;

    public ProcessType(final List<Activity> activities) {

        this.activities = activities;
        this.emptyPathExecutedActivities = new HashMap<String, Integer>();

        for (final Activity a : activities) {
            this.emptyPathExecutedActivities.put(a.getId(), new Integer(0));
        }
    }

    public List<Activity> getActivities() {
        return this.activities;
    }

    public Map<String, Integer> getEmptyPathExecutedActivities() {
        return this.emptyPathExecutedActivities;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("activities", activities) //
                .add("emptyPathExecutedActivities", emptyPathExecutedActivities) //
                .toString();
    }

}
