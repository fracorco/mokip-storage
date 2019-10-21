package eu.fbk.pdi.promo.simulation;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import eu.fbk.pdi.promo.simulation.elements.Activity;
import eu.fbk.pdi.promo.simulation.elements.ActivityClasses;
import eu.fbk.pdi.promo.simulation.elements.ActivityKinds;
import eu.fbk.pdi.promo.simulation.elements.Document;
import eu.fbk.pdi.promo.simulation.elements.IncomingFlowElement;
import eu.fbk.pdi.promo.simulation.elements.Pool;
import eu.fbk.pdi.promo.simulation.elements.ProcessType;
import eu.fbk.pdi.promo.util.Constants;

public class ProcessInstance {

    private final ProcessType ptype;

    private final List<ActivityInstance> executedActivities;

    private List<ActivityInstance> firingActivities;

    private final Set<ActorInstance> actors;

    private final Set<DocInstance> documents;

    private final DocInstance referenceDocument;

    private final List<ProcessIterableSwitchElement> pise;

    private String executionMode;

    private final StringBuffer executedPathActivityList;

    private final Map<String, Integer> executedPathActivityFlags;

    public ProcessInstance(final ProcessType ptype, final Set<ActorInstance> actors,
            final DocInstance referenceDocument, final int sdx, final int eId,
            final List<ProcessIterableSwitchElement> pise) {
        super();
        this.ptype = ptype;
        this.actors = actors;
        this.referenceDocument = referenceDocument;
        this.documents = new HashSet<>();
        this.executedActivities = new ArrayList<>();
        this.firingActivities = new ArrayList<>();
        this.pise = pise;
        this.executedPathActivityList = new StringBuffer();
        this.executedPathActivityFlags = this.ptype.getEmptyPathExecutedActivities();
    }

    public List<ActivityInstance> getExecutedActivities() {
        return this.executedActivities;
    }

    public void setExecutionMode(final String em) {
        this.executionMode = em;
    }

    public String getExecutedPathActivityList() {
        return this.executedPathActivityList.toString();
    }

    public Map<String, Integer> getExecutedPathActivityFlags() {
        return this.executedPathActivityFlags;
    }

    public ActivityInstance executeActivity(SimulatorConfig config, final Activity at) {

        final ActivityInstance res = new ActivityInstance(at, getOwnerWithType(at.getOwner()));

        // Step1: links to the preceding firing activities: scan all precedessor types, identifes
        // the firing instance of it, adds the link.
        final List<ActivityInstance> prec = actualPredecessors(at);
        Iterator<ActivityInstance> ite = prec.iterator();
        while (ite.hasNext()) {
            final ActivityInstance p = ite.next();
            res.addToPrecededBy(p);
            p.addToFollowedBy(res);
        }

        // Step 2: recompute the firing instances, removing those who fired all they could
        final List<ActivityInstance> newfiring = new ArrayList<ActivityInstance>();
        final Iterator<ActivityInstance> ifiring = this.firingActivities.iterator();
        while (ifiring.hasNext()) {
            final ActivityInstance firing = ifiring.next();
            boolean candidate = false;
            final int nfollow = firing.getFollowedBy().size();
            if (firing.getRandomPolicy().compareTo("NONE") == 0
                    && nfollow < firing.getActType().getFollowedBy().size()) {
                candidate = true;
            }
            if (firing.getRandomPolicy().compareTo("RANDOM") == 0 && nfollow == 0
                    && firing.getActType().getFollowedBy().size() > 0) {
                candidate = true;
            }
            if (firing.getRandomPolicy().startsWith("SEQUENCE") && nfollow == 0
                    && firing.getActType().getFollowedBy().size() > 0) {
                candidate = true;
            }
            if (candidate) {
                final boolean duplicate = firing.containedTypeInto(newfiring);
                if (!duplicate) {
                    newfiring.add(firing);
                }
            }
        }

        this.firingActivities = new ArrayList<ActivityInstance>();
        this.firingActivities.addAll(newfiring);

        // Step 2: generate the input docs
        final ArrayList<DocInstance> idocs = new ArrayList<DocInstance>();
        final Iterator<Document> ite_idocs = at.getInputDocs().iterator();
        while (ite_idocs.hasNext()) {
            final Document id = ite_idocs.next();
            final DocInstance idoc = id.reconstructFromReference(this.referenceDocument);
            idocs.add(idoc);
            this.documents.add(idoc);
        }
        res.setInputDocs(idocs);

        // Step 3: generate the output docs
        final ArrayList<DocInstance> odocs = new ArrayList<DocInstance>();
        final Iterator<Document> ite_odocs = at.getOutputDocs().iterator();
        while (ite_odocs.hasNext()) {
            final Document id = ite_odocs.next();
            res.fillFields(id, this.referenceDocument);
            final DocInstance odoc = id.reconstructFromReference(this.referenceDocument);
            odocs.add(odoc);
            this.documents.add(odoc);
        }
        res.setOutputDocs(odocs);

        // Step 4: set the timing
        int startTime;
        int endTime;
        int lastPrecTime = 0;
        ite = prec.iterator();
        while (ite.hasNext()) {
            final ActivityInstance p = ite.next();
            lastPrecTime = Math.max(lastPrecTime, p.getEndTime());
        }

        startTime = lastPrecTime + 3600;
        endTime = startTime;
        res.setStartTime(startTime);

        if (at.getRandomTimingPolicy().compareTo("FIXED") == 0) {
            endTime = lastPrecTime + 3600;
        }
        if (at.getRandomTimingPolicy().compareTo("RANDOM") == 0) {
            endTime = lastPrecTime + Randoms.randInt(3600);
        }
        endTime = startTime + 3600;
        res.setEndTime(endTime);

        // Step 5: set the random switch
        if (at.getRandomPolicy().compareTo("NONE") == 0) {
            res.setRandomSwitch(-1);
        }
        if (at.getRandomPolicy().compareTo("RANDOM") == 0) {
            res.setRandomSwitch(Randoms.randInt(at.getFollowedBy().size()));
        }
        if (at.getRandomPolicy().startsWith("SEQUENCE")) {

            // Checks if there are conditions for executing the activities following the switch
            // and select which activity may be executed
            final List<Activity> actualFollowers = at.getFollowedBy();
            final List<Activity> potentialFollowers = new ArrayList<Activity>();
            final Iterator<Activity> afIt = actualFollowers.iterator();
            while (afIt.hasNext()) {
                final Activity curFollower = afIt.next();
                final List<IncomingFlowElement> conditions = config.getIncoming()
                        .get(config.getActivitiesNames().get(curFollower.getId()));
                if (conditions != null) {
                    int conditionsToSatisfy = conditions.size();
                    for (int i = 0; i < conditions.size(); i++) {
                        final List<String> preConditions = conditions.get(i).getPreconditions();
                        if (preConditions.size() < 1) {
                            continue;
                        }
                        if (preConditions.get(0) == null) {
                            conditionsToSatisfy--;
                        } else {
                            final String currentCondition = config.getActivities()
                                    .get(preConditions.get(0));
                            if (currentCondition.compareTo(at.getId()) == 0) {
                                conditionsToSatisfy--;
                            }
                            final Iterator<ActivityInstance> eaIt = this.executedActivities
                                    .iterator();
                            while (eaIt.hasNext()) {
                                final ActivityInstance curAI = eaIt.next();
                                if (curAI.getActType().getId().compareTo(currentCondition) == 0) {
                                    conditionsToSatisfy--;
                                }
                            }
                        }
                    }
                    if (conditionsToSatisfy == 0) {
                        potentialFollowers.add(curFollower);
                    }
                }
            }

            int thernd = 0;
            if (potentialFollowers.size() == 0) {
                final String rndseq = at.getRandomPolicy().substring(9); // After "SEQUENCE", a
                                                                         // sequence
                // of dot-separated numbers
                final String[] rnds = rndseq.split("\\.");
                final int alreadyexec = at.countInstances(getExecutedActivities());
                final String r = rnds[alreadyexec % rnds.length];
                thernd = Integer.parseInt(r);
            } else if (potentialFollowers.size() == 1) {
                final Activity potentialFollower = potentialFollowers.get(0);
                thernd = at.getFollowerIndex(potentialFollower.getId());
            } else {
                final int rnd = (int) (Math.random() * potentialFollowers.size());
                final Activity potentialFollower = potentialFollowers.get(rnd);
                thernd = at.getFollowerIndex(potentialFollower.getId());
            }

            if (this.executionMode.compareTo("COMPLETE") == 0) {
                final Iterator<ProcessIterableSwitchElement> piseIt = this.pise.iterator();
                ProcessIterableSwitchElement p = null;
                int currentBranch = -1;
                while (piseIt.hasNext()) {
                    p = piseIt.next();
                    if (p.getSwitchName().compareTo(at.getId()) == 0) {
                        currentBranch = p.getActiveOutgoing();
                    }
                }
                final Activity selectedActivity = actualFollowers.get(currentBranch);
                if (potentialFollowers.size() > 0
                        && potentialFollowers.contains(selectedActivity)) {
                    thernd = currentBranch;
                } else if (potentialFollowers.size() == 0
                        && actualFollowers.contains(selectedActivity)) {
                    thernd = currentBranch;
                } else {
                    return null;
                }
            }

            res.setRandomSwitch(thernd);
        }

        // Step 6: add to executed, and if some follow-up is expected, to firing
        addExecutedActivity(res);

        this.executedPathActivityList.append("_" + res.getActType().getId());
        this.executedPathActivityFlags.put(res.getActType().getId(), new Integer(1));
        return res;
    }

    public boolean execute(SimulatorConfig config) {

        ArrayList<Activity> fireable;
        fireable = getFireable();

        while (fireable.size() > 0 && !hasDuplicateFiringTypes()) {

            int index = Randoms.randInt(fireable.size());
            index = 0;
            final Activity firing = fireable.get(index);
            final ActivityInstance fired = executeActivity(config, firing);

            if (fired == null) {
                return false;
            }
            fireable = getFireable();
        }

        return true;
    }

    public String digest() {

        Hasher hasher = Hashing.murmur3_128().newHasher();

        getExecutedActivities().stream() //
                .filter(a -> a.getActType().getEnableOutput() != 0) //
                .map(a -> a.digest(this.referenceDocument)) //
                .sorted() //
                .forEach(a -> hasher.putUnencodedChars(a));

        return hasher.hash().toString();
    }

    public JsonObject toJson(SimulatorConfig config, final String traceId, long curTime) {

        curTime = curTime > 0 ? curTime : System.currentTimeMillis();

        final JsonArray activitiesJs = new JsonArray();
        for (final ActivityInstance ai : getExecutedActivities()) {

            if (ai.getActType().getEnableOutput() == 0) {
                continue;
            }

            final Date sd = new Date(curTime + 1000 * ai.getStartTime());
            final String sdt = Constants.DATE_FORMAT.format(sd);
            final Date ed = new Date(curTime + 1000 * ai.getEndTime());
            final String edt = Constants.DATE_FORMAT.format(ed);

            for (final String role : new String[] { "IN", "OUT" }) {
                final JsonObject js = ai.toJson(config, traceId, role, this.referenceDocument, sdt,
                        edt);
                if (js.size() > 0) {
                    final JsonObject activityJs = new JsonObject();
                    activityJs.addProperty("BP_activity_name", ai.toStr());
                    activityJs.addProperty("actorClass",
                            ai.getActType().getOwner().getActorClass());
                    activityJs.addProperty("actorInstanceID", ai.getOwner().toStr());
                    activityJs.addProperty("start", sdt);
                    activityJs.addProperty("end", edt);
                    activityJs.add("dataContent", js);
                    activitiesJs.add(activityJs);
                }
            }
        }

        final JsonObject traceJs = new JsonObject();
        traceJs.addProperty("trace_id", traceId);
        traceJs.addProperty("trace_process", "Nascita-Astratto3");
        traceJs.addProperty("BP_Process_Class", "AGAP");
        traceJs.addProperty("BP_Process_ID", "1");
        traceJs.addProperty("IT_Process_ID", "1");
        traceJs.addProperty("digest", digest());
        traceJs.addProperty("path", getExecutedPathActivityList());
        traceJs.add("activities", activitiesJs);
        return traceJs;
    }

    private void addExecutedActivity(final ActivityInstance ai) {

        if (!this.executedActivities.contains(ai)) {
            this.executedActivities.add(ai);
        }

        if (ai.getActType().getFollowedBy().size() > 0 && !this.firingActivities.contains(ai)) {
            final boolean duplicate = ai.containedTypeInto(this.firingActivities);
            if (!duplicate) {
                this.firingActivities.add(ai);
            }
        }
    }

    private List<ActivityInstance> actualPredecessors(final Activity at) {

        final List<ActivityInstance> res = new ArrayList<ActivityInstance>();

        final List<Activity> prec = at.getPrecededBy();
        final Iterator<Activity> iprec = prec.iterator();

        // For all preceding activity types, I check if there is a firing instance,
        // checking that has not fired already this type (to handle the case of loops)
        while (iprec.hasNext()) {
            final Activity preceding = iprec.next();
            final Iterator<ActivityInstance> ifiring = this.firingActivities.iterator();
            while (ifiring.hasNext()) {
                final ActivityInstance firing = ifiring.next();
                if (firing.getActType().getId() == preceding.getId()) {
                    // Found a potential firing instance for the activity type under exam.
                    // If really it wants to fire this type, we add it
                    if (firing.mustFireType(at) == true) {
                        res.add(firing);
                    }
                }
            }
        }

        return res;
    }

    private ArrayList<Activity> potentiallyFireable() {

        final ArrayList<Activity> res = new ArrayList<Activity>();

        // For all firing activities, I get all their actual following activity types, and check
        // if they have not been fired already, and could be fired. If not, they are candidate for
        // firing.

        // For all firing activities...
        final Iterator<ActivityInstance> it = this.firingActivities.iterator();
        while (it.hasNext()) {
            final ActivityInstance ai = it.next();
            ArrayList<ActivityInstance> actualFollowers;
            ArrayList<Activity> potentialFollowers;

            if (ai.getRandomSwitch() == -1) {
                actualFollowers = ai.getFollowedBy();
                potentialFollowers = ai.getActType().getFollowedBy();
            } else {
                actualFollowers = new ArrayList<ActivityInstance>();
                if (ai.getFollowedBy().size() > 0) {
                    actualFollowers.add(ai.getFollowedBy().get(ai.getRandomSwitch()));
                }
                potentialFollowers = new ArrayList<Activity>();
                potentialFollowers.add(ai.getActType().getFollowedBy().get(ai.getRandomSwitch()));
            }

            // For every following activity type...
            final Iterator<Activity> pot = potentialFollowers.iterator();
            while (pot.hasNext()) {
                final Activity maybePotential = pot.next();
                final boolean alreadyRan = maybePotential.containedInto(actualFollowers);
                // ...if none of the fired matches the candidate, the candidate has to be
                // considered.
                final int no_executed = maybePotential.countInstances(getExecutedActivities());
                final boolean can_execute = maybePotential.getMaxNumberOfExecutions() == -1
                        || maybePotential.getMaxNumberOfExecutions() > no_executed;
                if (!alreadyRan && can_execute && !res.contains(maybePotential)) {
                    res.add(maybePotential);
                }
            }
        }

        return res;
    }

    private boolean hasDuplicateFiringTypes() {
        boolean res = false;
        final Iterator<ActivityInstance> i1 = this.firingActivities.iterator();
        while (i1.hasNext()) {
            final ActivityInstance ai1 = i1.next();
            final Iterator<ActivityInstance> i2 = this.firingActivities.iterator();
            while (i2.hasNext()) {
                final ActivityInstance ai2 = i2.next();
                if (ai1.getInstanceIndex() != ai2.getInstanceIndex() && ai1.sameType(ai2)) {
                    res = true;
                }
            }
        }
        return res;
    }

    private boolean isFireable(final Activity at) {

        // Activity can be fired if all linked activities have run (for message catching) or one
        // has run.
        final List<ActivityInstance> precs = actualPredecessors(at);
        boolean res;
        if (at.getClassSuper() == ActivityClasses.CATCH
                && at.getClassActual() == ActivityKinds.INTERMEDIATE_MSG) {
            res = precs.size() == at.getPrecededBy().size();
        } else {
            res = precs.size() > 0;
        }

        return res;
    }

    private ArrayList<Activity> getFireable() {

        final ArrayList<Activity> potential = potentiallyFireable();
        final ArrayList<Activity> actual = new ArrayList<Activity>();
        final Iterator<Activity> i = potential.iterator();
        while (i.hasNext()) {
            final Activity pot = i.next();
            final boolean isf = isFireable(pot);
            if (isf) {
                actual.add(pot);
            }
        }
        return actual;
    }

    private ActorInstance getOwnerWithType(final Pool act) {
        ActorInstance res = null;
        final Iterator<ActorInstance> i = this.actors.iterator();
        while (i.hasNext()) {
            final ActorInstance ai = i.next();
            if (ai.getType().getActorClass() == act.getActorClass()) {
                res = ai;
            }
        }
        assert res != null;
        return res;

    }

}
