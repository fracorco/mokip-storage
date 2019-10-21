package eu.fbk.pdi.promo.simulation;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.MoreObjects;

import eu.fbk.pdi.promo.simulation.elements.Pool;

public class ActorInstance {

    private final Pool type;

    private final List<Entry<String, String>> idValues;

    public ActorInstance(final Pool type, final List<Entry<String, String>> idValues) {
        super();
        this.type = type;
        this.idValues = idValues;
    }

    public Pool getType() {
        return this.type;
    }

    public String toStr() {
        String res = new String("");
        final Iterator<Entry<String, String>> i = this.idValues.iterator();
        while (i.hasNext()) {
            res = res + i.next().getValue() + ".";
        }
        return res;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("type", this.type) //
                .add("idValues", this.idValues) //
                .toString();
    }

}
