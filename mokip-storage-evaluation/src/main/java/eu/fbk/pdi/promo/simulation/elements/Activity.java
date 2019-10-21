package eu.fbk.pdi.promo.simulation.elements;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.MoreObjects;

import eu.fbk.pdi.promo.simulation.ActivityInstance;

public class Activity {

    private final ActivityClasses classSuper;

    private final ActivityKinds classActual;

    private final String id;

    private final String name;

    private final Pool owner;

    private final ArrayList<Document> inputDocs;

    private final ArrayList<Document> outputDocs;

    private final ArrayList<Activity> precededBy;

    private final ArrayList<Activity> followedBy;

    private String randomPolicy;

    private final String randomTimingPolicy;

    private final List<Emitter> emitters;

    private final int maxNumberOfExecutions;

    private int enableOutput;

    public Activity(final ActivityClasses classSuper, final ActivityKinds classActual,
            final String id, final String name, final Pool owner, final String randomPolicy,
            final String randomTimingPolicy) {
        super();
        this.classSuper = classSuper;
        this.classActual = classActual;
        this.id = id;
        this.name = name;
        this.owner = owner;
        this.randomPolicy = randomPolicy;
        this.randomTimingPolicy = randomTimingPolicy;
        this.maxNumberOfExecutions = -1;
        this.enableOutput = 0;
        this.inputDocs = new ArrayList<Document>();
        this.outputDocs = new ArrayList<Document>();
        this.precededBy = new ArrayList<Activity>();
        this.followedBy = new ArrayList<Activity>();
        this.emitters = new ArrayList<Emitter>();
    }

    public Activity(final ActivityClasses classSuper, final ActivityKinds classActual,
            final String id, final String name, final Pool owner) {
        super();
        this.classSuper = classSuper;
        this.classActual = classActual;
        this.id = id;
        this.name = name;
        this.owner = owner;
        if (classActual != ActivityKinds.XOR) {
            this.randomPolicy = new String("NONE");
        } else {
            this.randomPolicy = new String("RANDOM");
        }
        this.randomTimingPolicy = new String("FIXED");
        this.maxNumberOfExecutions = -1;
        this.enableOutput = 0;

        this.inputDocs = new ArrayList<Document>();
        this.outputDocs = new ArrayList<Document>();
        this.precededBy = new ArrayList<Activity>();
        this.followedBy = new ArrayList<Activity>();
        this.emitters = new ArrayList<Emitter>();
    }

    public int getMaxNumberOfExecutions() {
        return this.maxNumberOfExecutions;
    }

    public boolean containedInto(final List<ActivityInstance> al) {
        final Iterator<ActivityInstance> i = al.iterator();
        boolean res = false;
        while (i.hasNext()) {
            final ActivityInstance ai = i.next();
            res = res || ai.sameType(this);
        }
        return res;
    }

    public int countInstances(final List<ActivityInstance> newfiring) {
        int res = 0;
        final Iterator<ActivityInstance> i = newfiring.iterator();
        while (i.hasNext()) {
            final ActivityInstance ai = i.next();
            if (ai.sameType(this)) {
                res++;
            }
        }
        return res;
    }

    public List<Emitter> getEmitters() {
        return this.emitters;
    }

    public void addEmitter(final Emitter e) {
        this.emitters.add(e);
    }

    public String getRandomTimingPolicy() {
        return this.randomTimingPolicy;
    }

    public List<Document> getInputDocs() {
        return this.inputDocs;
    }

    public List<Document> getOutputDocs() {
        return this.outputDocs;
    }

    public void addOutputDoc(final Document outputDoc) {
        this.outputDocs.add(outputDoc);
    }

    public ActivityClasses getClassSuper() {
        return this.classSuper;
    }

    public ActivityKinds getClassActual() {
        return this.classActual;
    }

    public String getId() {
        return this.id;
    }

    public Pool getOwner() {
        return this.owner;
    }

    public List<Activity> getPrecededBy() {
        return this.precededBy;
    }

    private void addPrecededBy(final Activity precededBy) {
        this.precededBy.add(precededBy);
    }

    public ArrayList<Activity> getFollowedBy() {
        return this.followedBy;
    }

    public void LinkTo(final Activity followedBy) {
        this.followedBy.add(followedBy);
        followedBy.addPrecededBy(this);
    }

    public String getRandomPolicy() {
        return this.randomPolicy;
    }

    public void setEnableOutput(final int eO) {
        this.enableOutput = eO;
    }

    public int getEnableOutput() {
        return this.enableOutput;
    }

    public int getFollowerIndex(final String actId) {
        int idx = -1;
        final Iterator<Activity> itF = this.followedBy.iterator();
        while (itF.hasNext()) {
            idx++;
            final Activity curAct = itF.next();
            if (curAct.getId().compareTo(actId) == 0) {
                break;
            }
        }
        return idx;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("classSuper", this.classSuper) //
                .add("classActual", this.classActual) //
                .add("id", this.id) //
                .add("name", this.name) //
                .add("owner", this.owner) //
                .add("inputDocs", this.inputDocs) //
                .add("outputDocs", this.outputDocs) //
                .add("precededBy", this.precededBy) //
                .add("followedBy", this.followedBy) //
                .add("randomPolicy", this.randomPolicy) //
                .add("randomTimingPolicy", this.randomTimingPolicy) //
                .add("emitters", this.emitters) //
                .add("maxNumberOfExecutions", this.maxNumberOfExecutions) //
                .add("enableOutput", this.enableOutput) //
                .toString();
    }

}
