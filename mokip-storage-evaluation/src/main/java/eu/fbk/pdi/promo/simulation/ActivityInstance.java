package eu.fbk.pdi.promo.simulation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.MoreObjects;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.JsonObject;

import eu.fbk.pdi.promo.simulation.elements.Activity;
import eu.fbk.pdi.promo.simulation.elements.Document;
import eu.fbk.pdi.promo.simulation.elements.Emitter;

public class ActivityInstance {

    private static int globalIndex = 1;

    private final Activity actType;

    private final ActorInstance owner;

    private ArrayList<DocInstance> inputDocs;

    private ArrayList<DocInstance> outputDocs;

    private final ArrayList<ActivityInstance> precededBy;

    private final ArrayList<ActivityInstance> followedBy;

    private final String randomPolicy;

    private int randomSwitch;

    private int startTime;

    private int endTime;

    private final int instanceIndex;

    public ActivityInstance(final Activity actType, final ActorInstance owner) {
        this.actType = actType;
        this.owner = owner;
        this.inputDocs = new ArrayList<DocInstance>();
        this.outputDocs = new ArrayList<DocInstance>();
        this.precededBy = new ArrayList<ActivityInstance>();
        this.followedBy = new ArrayList<ActivityInstance>();
        this.randomPolicy = actType.getRandomPolicy();
        this.instanceIndex = globalIndex;
        globalIndex++;
    }

    public String toStr() {
        return this.instanceIndex + "." + getActType().getId();
    }

    public int getInstanceIndex() {
        return this.instanceIndex;
    }

    public int getStartTime() {
        return this.startTime;
    }

    public void setStartTime(final int startTime) {
        this.startTime = startTime;
    }

    public int getEndTime() {
        return this.endTime;
    }

    public void setEndTime(final int endTime) {
        this.endTime = endTime;
    }

    public int getRandomSwitch() {
        return this.randomSwitch;
    }

    public void setRandomSwitch(final int randomSwitch) {
        this.randomSwitch = randomSwitch;
    }

    public Activity getActType() {
        return this.actType;
    }

    public ActorInstance getOwner() {
        return this.owner;
    }

    public boolean containedTypeInto(final List<ActivityInstance> newfiring) {
        boolean res = false;
        final Iterator<ActivityInstance> i = newfiring.iterator();
        while (i.hasNext()) {
            final ActivityInstance ai = i.next();
            res = res || ai.sameType(this);
        }
        return res;
    }

    public boolean sameType(final ActivityInstance ai) {
        return getActType().getId().compareTo(ai.getActType().getId()) == 0;
    }

    public boolean sameType(final Activity at) {
        return getActType().getId().compareTo(at.getId()) == 0;
    }

    public void setInputDocs(final ArrayList<DocInstance> inputDocs) {
        this.inputDocs = inputDocs;
    }

    public void setOutputDocs(final ArrayList<DocInstance> outputDocs) {
        this.outputDocs = outputDocs;
    }

    public void addToPrecededBy(final ActivityInstance precededBy) {
        this.precededBy.add(precededBy);
    }

    public ArrayList<ActivityInstance> getFollowedBy() {
        return this.followedBy;
    }

    public void addToFollowedBy(final ActivityInstance followedBy) {
        this.followedBy.add(followedBy);
    }

    public String getRandomPolicy() {
        return this.randomPolicy;
    }

    private boolean hasFiredType(final Activity at) {
        boolean res = false;
        final Iterator<ActivityInstance> i = this.followedBy.iterator();
        while (i.hasNext()) {
            res = res || i.next().getActType().getId() == at.getId();
        }
        return res;
    }

    public boolean mustFireType(final Activity at) {

        if (hasFiredType(at)) {
            return false;
        }

        if (getFollowedBy().size() == getActType().getFollowedBy().size()) {
            return false;
        }

        if (getRandomPolicy().compareTo("RANDOM") == 0 && getFollowedBy().size() == 1) {
            return false;
        }

        final List<Activity> prec = at.getPrecededBy();
        final Iterator<Activity> iteprec = prec.iterator();
        boolean found = false;
        while (iteprec.hasNext()) {
            final Activity ip = iteprec.next();
            found = found || ip.getId().compareTo(getActType().getId()) == 0;
        }
        return found;
    }

    public String digest(final DocInstance referenceDocument) {

        final Map<String, String> meanings = referenceDocument.getDocType().getMeanings();

        final Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putInt(this.startTime);
        hasher.putInt(this.endTime);
        hasher.putUnencodedChars(getActType().getId());
        hasher.putByte((byte) 0);
        hasher.putUnencodedChars(this.actType.getOwner().getActorClass());
        for (final String role : new String[] { "IN", "OUT" }) {
            for (final DocInstance di : role.equalsIgnoreCase("IN") ? this.inputDocs
                    : this.outputDocs) {
                for (final Emitter emit : getActType().getEmitters()) {
                    final String dtype = emit.getDocId();
                    final boolean okDocType = dtype.equals(di.getDocType().getId());
                    final boolean okRole = emit.equals("ANY") || emit.getDocrole().equals(role);
                    if (okDocType && okRole) {
                        final List<Entry<String, String>> varvals = emit
                                .generateValues_noDefault(di, referenceDocument);
                        for (final Entry<String, String> currentField : varvals) {
                            final String fieldMeaning = meanings.get(currentField.getKey());
                            if (fieldMeaning != null) {
                                if (fieldMeaning.compareTo("startdate") == 0
                                        || fieldMeaning.compareTo("startenddate") == 0) {
                                    currentField.setValue("" + this.startTime);
                                } else if (fieldMeaning.compareTo("enddate") == 0) {
                                    currentField.setValue("" + this.endTime);
                                }
                            }
                        }
                        hasher.putByte((byte) 0);
                        hasher.putUnencodedChars(role);
                        hasher.putByte((byte) 0);
                        // for (final Entry<String, String> varval : varvals) {
                        // hasher.putUnencodedChars(varval.getKey());
                        // hasher.putByte((byte) 0);
                        // hasher.putUnencodedChars(structure ? "" : varval.getValue());
                        // }
                    }
                }
            }
        }

        return hasher.hash().toString();
    }

    public JsonObject toJson(final SimulatorConfig config, final String tracename,
            final String role, final DocInstance referenceDocument, final String startDate,
            final String endDate) {

        final Map<String, String> meanings = referenceDocument.getDocType().getMeanings();

        final JsonObject js = new JsonObject();
        for (final DocInstance di : role.equalsIgnoreCase("IN") ? this.inputDocs
                : this.outputDocs) {
            for (final Emitter emit : getActType().getEmitters()) {
                final String dtype = emit.getDocId();
                final boolean okDocType = dtype.equals(di.getDocType().getId());
                final boolean okRole = emit.equals("ANY") || emit.getDocrole().equals(role);
                if (okDocType && okRole) {
                    final List<Entry<String, String>> varvals = emit.generateValues_noDefault(di,
                            referenceDocument);
                    for (final Entry<String, String> currentField : varvals) {
                        final String fieldMeaning = meanings.get(currentField.getKey());
                        if (fieldMeaning != null) {
                            if (fieldMeaning.compareTo("startdate") == 0
                                    || fieldMeaning.compareTo("startenddate") == 0) {
                                currentField.setValue(startDate);
                            } else if (fieldMeaning.compareTo("enddate") == 0) {
                                currentField.setValue(endDate);
                            }
                        }
                    }
                    js.addProperty("filename", tracename + "_" + role + "_" + toStr());
                    js.add(dtype, toJson(config, varvals));
                }
            }
        }
        return js;
    }

    private static JsonObject toJson(final SimulatorConfig config,
            final List<Entry<String, String>> varvalues) {

        final JsonObject js = new JsonObject();
        for (final Entry<String, String> varval : varvalues) {
            final String[] path = varval.getKey().split("\\.");
            final String val = varval.getValue();
            JsonObject childJs = js;
            for (int i = 0; i < path.length - 1; ++i) {
                if (!childJs.has(path[i])) {
                    childJs.add(path[i], new JsonObject());
                }
                childJs = childJs.get(path[i]).getAsJsonObject();
            }
            childJs.addProperty(path[path.length - 1], val);
        }
        return js;
    }

    /**
     * Sets the filled flags on documents. These flags are used for the reconstruction of the
     * document instance before to print it in the trace
     *
     * @param tracename
     * @param role
     * @param kind
     * @return
     */
    public List<String> fillFields(final Document docId, final DocInstance referenceDocument) {

        final List<String> res = new ArrayList<String>();

        final Iterator<Emitter> iemit = getActType().getEmitters().iterator();
        while (iemit.hasNext()) {
            final Emitter emit = iemit.next();
            final Iterator<Entry<String, String>> imap = emit.getOutputMap().iterator();
            while (imap.hasNext()) {
                final Entry<String, String> onemap = imap.next();
                docId.fillFields(onemap.getKey(), referenceDocument);
            }
        }
        return res;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("actType", this.actType) //
                .add("owner", this.owner) //
                .add("inputDocs", this.inputDocs) //
                .add("outputDocs", this.outputDocs) //
                .add("precededBy", this.precededBy) //
                .add("followedBy", this.followedBy) //
                .add("randomPolicy", this.randomPolicy) //
                .add("randomSwitch", this.randomSwitch) //
                .add("startTime", this.startTime) //
                .add("endTime", this.endTime) //
                .add("instanceIndex", this.instanceIndex) //
                .toString();
    }

}
