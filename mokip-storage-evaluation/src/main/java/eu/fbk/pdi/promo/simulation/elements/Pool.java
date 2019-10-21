package eu.fbk.pdi.promo.simulation.elements;

import java.util.AbstractMap.SimpleEntry;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import eu.fbk.pdi.promo.simulation.ActorInstance;
import eu.fbk.pdi.promo.simulation.Randoms;

public class Pool implements Comparable<Pool> {

    private final String actorClass;

    private final String idName;

    public Pool(final String actorClass, final String idName) {
        this.actorClass = actorClass;
        this.idName = idName;
    }

    public String getActorClass() {
        return this.actorClass;
    }

    public ActorInstance makeRandom() {

        String rstring;
        if (this.idName.startsWith("date")) {
            rstring = Randoms.randDate();
        } else {
            rstring = Randoms.randUUID();
        }

        return new ActorInstance(this,
                Lists.newArrayList(new SimpleEntry<String, String>(this.idName, rstring)));
    }

    @Override
    public int compareTo(final Pool other) {
        return this.idName.compareTo(other.idName);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Pool)) {
            return false;
        }
        final Pool other = (Pool) object;
        return Objects.equals(this.idName, other.idName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.idName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("actorClass", this.actorClass) //
                .add("idName", this.idName) //
                .toString();
    }

}
