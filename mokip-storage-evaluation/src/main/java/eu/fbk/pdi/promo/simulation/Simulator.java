package eu.fbk.pdi.promo.simulation;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pdi.promo.simulation.elements.Activity;
import eu.fbk.pdi.promo.simulation.elements.ActivityClasses;
import eu.fbk.pdi.promo.simulation.elements.ActivityKinds;
import eu.fbk.pdi.promo.simulation.elements.Document;
import eu.fbk.pdi.promo.simulation.elements.Emitter;
import eu.fbk.pdi.promo.simulation.elements.Pool;
import eu.fbk.pdi.promo.simulation.elements.ProcessType;
import eu.fbk.pdi.promo.util.Constants;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.utils.core.CommandLine;

public class Simulator {

    public static final int DEFAULT_NUM_TRACES = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(Simulator.class);

    private static final Gson GSON = new Gson();

    private SimulatorConfig config;

    private List<Pool> poolList;

    private final Pool basicPool;

    private final List<Activity> taskList;

    private final List<Document> docList;

    private Map<String, Pool> poolMap;

    private final Map<String, Activity> taskMap;

    private final Map<String, Document> docMap;

    private Document referenceDocument;

    private final List<ProcessIterableSwitchElement> pise;

    public Simulator(SimulatorConfig config) {

        Objects.requireNonNull(config);

        this.config = config;
        this.basicPool = new Pool("__mokiBasicPool", "__mokiBasicPool");
        this.taskList = new ArrayList<Activity>();
        this.docList = new ArrayList<Document>();
        this.poolMap = config.getPoolMap();
        this.poolList = config.getPoolList();
        this.taskMap = new HashMap<String, Activity>();
        this.docMap = new HashMap<String, Document>();
        this.pise = new ArrayList<ProcessIterableSwitchElement>();

        createDocumentObjects(config.getDocumentsFields());
        createActivityObjects(config.getActivitiesDocuments());
        createEmitterObjects(config.getActivitiesFields());
    }

    public boolean setNextIteration() {
        boolean availableExecution = false;
        final Iterator<ProcessIterableSwitchElement> itSwitch = this.pise.iterator();
        while (itSwitch.hasNext()) {
            final ProcessIterableSwitchElement curP = itSwitch.next();
            availableExecution = curP.setNextIteration();
            if (availableExecution == true) {
                break;
            }
        }
        return availableExecution;
    }

    public void resetSwitches() {
        final Iterator<ProcessIterableSwitchElement> itSwitch = this.pise.iterator();
        while (itSwitch.hasNext()) {
            final ProcessIterableSwitchElement curP = itSwitch.next();
            curP.resetIterations();
        }
    }

    public ProcessInstance createProcessInstance() {

        ProcessInstance pI = null;
        final Set<ActorInstance> actorInstances = createRoleObjects();

        // Creates the process instance
        final DocInstance referenceDocInstance = this.referenceDocument.makeRandom();
        final ProcessType agap = new ProcessType(this.taskList);
        pI = new ProcessInstance(agap, actorInstances, referenceDocInstance, 0, 0, this.pise);

        // Starts each pool
        final Map<String, String> startElements = config.getStartElements();
        final Iterator<String> it = startElements.keySet().iterator();
        while (it.hasNext()) {
            final String currentElementId = it.next();
            final String currentElementName = startElements.get(currentElementId);
            if (currentElementName.startsWith("start")) {
                pI.executeActivity(config, this.taskMap.get(currentElementName));
            }
        }

        return pI;
    }

    private Set<ActorInstance> createRoleObjects() {

        final Set<ActorInstance> actorInstances = new HashSet<ActorInstance>();

        for (Pool pool : this.poolList) {
            actorInstances.add(pool.makeRandom());
        }
        final ActorInstance basicRole = this.basicPool.makeRandom();
        actorInstances.add(basicRole);
        return actorInstances;
    }

    private void createDocumentObjects(final Map<String, List<String>> documentsFields) {

        final Iterator<String> it = documentsFields.keySet().iterator();

        this.referenceDocument = new Document("reference");
        this.referenceDocument.setMeanings(config.getMeaningFields());

        while (it.hasNext()) {
            final String currentDocument = it.next();
            final List<String> currentDocumentFields = documentsFields
                    .getOrDefault(currentDocument, ImmutableList.of());

            final Document currentDocumentObject = new Document(currentDocument.toLowerCase());
            for (int i = 0; i < currentDocumentFields.size(); i++) {
                final String fieldId = currentDocumentFields.get(i);
                if (fieldId.toLowerCase().compareTo("") != 0) {
                    this.referenceDocument.addDataName(fieldId.toLowerCase());
                    currentDocumentObject.addAndMap1to1(fieldId.toLowerCase());
                }
            }
            this.docList.add(currentDocumentObject);
            this.docMap.put(currentDocument.toLowerCase(), currentDocumentObject);
        }
    }

    private void createActivityObjects(final Map<String, List<String>> activitiesDocuments) {

        // USED FOR FORCING THE XOR BEHAVIOR IN THE APSS TEST CASE
        final String randomCF2 = "SEQUENCE 1.0.1.1";
        final String switchComune2SEQ = "SEQUENCE 0.1.0.1.1.1"; // changed
        final String switchApss2SEQ = "SEQUENCE 1.0.1.1.1";
        final String switchSaia2SEQ = "SEQUENCE 0.1.0.0.0";
        final String gen01 = "SEQUENCE 0.1.0.1";
        final String apss01 = "SEQUENCE 0.1.0";
        final String apss03 = "SEQUENCE 1.0.1.0"; // changed
        final String com01 = "SEQUENCE 1.0.1";
        final String com03 = "SEQUENCE 0.1.0.1";

        // Builds the task list
        Iterator<String> it = activitiesDocuments.keySet().iterator();
        while (it.hasNext()) {
            final String currentActivity = it.next();
            final String actPoolName = config.getActivitiesPools()
                    .get(currentActivity.toLowerCase());
            Pool actPool = null;
            if (actPoolName == null) {
                actPool = this.basicPool;
            } else {
                actPool = this.poolMap.get(getLowerCase(actPoolName));
            }
            final Activity newActivity = new Activity(ActivityClasses.ACT, ActivityKinds.TASK,
                    currentActivity.toLowerCase(), currentActivity.toLowerCase(), actPool);
            this.taskList.add(newActivity);
            this.taskMap.put(currentActivity.toLowerCase(), newActivity);
        }

        final Map<String, String> endElements = config.getEndElements();
        it = endElements.keySet().iterator();
        while (it.hasNext()) {
            final String currentElementId = it.next();
            final String currentElementName = endElements.get(currentElementId);
            final String currentElementType = config.getElementsType().get(currentElementId);
            final String actPoolName = config.getActivitiesPools()
                    .get(currentElementName.toLowerCase());
            Pool actPool = null;
            if (actPoolName == null) {
                actPool = this.basicPool;
            } else {
                actPool = this.poolMap.get(getLowerCase(actPoolName));
            }

            Activity newActivity = null;
            if (currentElementType.compareTo("EndMessageEvent") == 0) {
                newActivity = new Activity(ActivityClasses.END, ActivityKinds.END_MSG,
                        currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                        actPool);
            } else {
                newActivity = new Activity(ActivityClasses.END, ActivityKinds.END,
                        currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                        actPool);
            }

            this.taskList.add(newActivity);
            this.taskMap.put(currentElementName.toLowerCase(), newActivity);
        }

        final Map<String, String> startElements = config.getStartElements();
        it = startElements.keySet().iterator();
        while (it.hasNext()) {
            final String currentElementId = it.next();
            final String currentElementName = startElements.get(currentElementId);
            final String currentElementType = config.getElementsType().get(currentElementId);
            final String actPoolName = config.getActivitiesPools()
                    .get(currentElementName.toLowerCase());
            Pool actPool = null;
            if (actPoolName == null) {
                actPool = this.basicPool;
            } else {
                actPool = this.poolMap.get(getLowerCase(actPoolName));
            }

            Activity newActivity = null;
            if (currentElementType.compareTo("startmessageevent") == 0) {
                newActivity = new Activity(ActivityClasses.START, ActivityKinds.START_MSG,
                        currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                        actPool);
            } else if (currentElementType.compareTo("intermediatemessageeventcatching") == 0
                    || currentElementType.compareTo("intermediatemessageeventthrowing") == 0) {
                newActivity = new Activity(ActivityClasses.CATCH, ActivityKinds.INTERMEDIATE_MSG,
                        currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                        actPool);
            } else {
                newActivity = new Activity(ActivityClasses.START, ActivityKinds.START,
                        currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                        actPool);
            }

            this.taskList.add(newActivity);
            this.taskMap.put(currentElementName.toLowerCase(), newActivity);
        }
        final Map<String, String> gatewaysElements = config.getGatewaysMap();
        it = gatewaysElements.keySet().iterator();
        while (it.hasNext()) {
            final String currentElementId = it.next();
            final String currentElementName = gatewaysElements.get(currentElementId);
            final String currentElementType = config.getElementsType().get(currentElementId);
            final String actPoolName = config.getActivitiesPools()
                    .get(currentElementName.toLowerCase());
            Pool actPool = null;
            if (actPoolName == null) {
                actPool = this.basicPool;
            } else {
                actPool = this.poolMap.get(actPoolName.toLowerCase());
            }

            Activity newActivity = null;

            final int outgoingsSize = config.getOutgoing()
                    .get(config.getActivitiesNames().get(currentElementName)).size();
            if (outgoingsSize == 1) {
                newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.JOIN,
                        currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                        actPool);
            } else {
                if (currentElementType.compareTo("exclusivedatabasedgateway") == 0
                        || currentElementType.compareTo("eventbasedgateway") == 0) {

                    if (currentElementName.compareTo("genitoresplit1") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, gen01, "FIXED");
                    } else if (currentElementName.compareTo("apsssplit1") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, apss01, "FIXED");
                    } else if (currentElementName.compareTo("apsssplit2") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, switchApss2SEQ, "FIXED");
                    } else if (currentElementName.compareTo("apsssplit3") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, apss03, "FIXED");
                    } else if (currentElementName.compareTo("comunesplit1") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, com01, "FIXED");
                    } else if (currentElementName.compareTo("comunesplit2") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, switchComune2SEQ, "FIXED");
                    } else if (currentElementName.compareTo("comunesplit3") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, com03, "FIXED");
                    } else if (currentElementName.compareTo("saiasplit1") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, randomCF2, "FIXED");
                    } else if (currentElementName.compareTo("saiasplit2") == 0) {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, switchSaia2SEQ, "FIXED");
                    } else {
                        newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.XOR,
                                currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                                actPool, "SEQUENCE 0.1", "FIXED");
                    }

                } else if (currentElementType.compareTo("ORGateway") == 0) {
                    newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.INCLUSIVE,
                            currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                            actPool);
                } else if (currentElementType.compareTo("ANDGateway") == 0) {
                    newActivity = new Activity(ActivityClasses.GATE, ActivityKinds.PARALLEL,
                            currentElementName.toLowerCase(), currentElementName.toLowerCase(),
                            actPool);
                }
            }

            this.taskList.add(newActivity);
            this.taskMap.put(currentElementName.toLowerCase(), newActivity);
        }

        // Creates the links between the activities in order to simulate the process
        final Iterator<Activity> itAct = this.taskList.iterator();
        while (itAct.hasNext()) {
            final Activity curAct = itAct.next();
            final String currentActivity = curAct.getId();

            final List<String> outActivities = config.getOutgoing()
                    .get(config.getActivitiesNames().get(currentActivity));
            if (outActivities != null) {
                final Iterator<String> itA = outActivities.iterator();
                while (itA.hasNext()) {
                    final String currentOutActivity = itA.next();
                    final String currentOutActivityName = config.getAllElements()
                            .get(currentOutActivity);
                    if (currentOutActivityName != null) {
                        final Activity actFrom = this.taskMap.get(currentActivity.toLowerCase());
                        final Activity actTo = this.taskMap
                                .get(currentOutActivityName.toLowerCase());
                        try {
                            actFrom.LinkTo(actTo);
                        } catch (final Exception e) {
                            System.out.println(currentActivity + " - " + currentOutActivityName);
                        }
                    }
                }
            }
        }

        // Retrieves all XOR and OR gateways in order to build the complete set of iterable object
        // for performing a complete simulation of the model
        final Iterator<String> itTask = this.taskMap.keySet().iterator();
        while (itTask.hasNext()) {
            final String taskName = itTask.next();
            final Activity curAct = this.taskMap.get(taskName);
            if (curAct.getClassActual() == ActivityKinds.INCLUSIVE
                    || curAct.getClassActual() == ActivityKinds.XOR) {
                final ProcessIterableSwitchElement currentPise = new ProcessIterableSwitchElement(
                        taskName.toLowerCase(), curAct.getFollowedBy().size());
                this.pise.add(currentPise);
            }
        }

        // Associates the documents to the output of the related activities
        for (String currentActivity : activitiesDocuments.keySet()) {
            final Activity curAct = this.taskMap.get(currentActivity.toLowerCase());
            for (final String curDocument : activitiesDocuments.getOrDefault(currentActivity,
                    ImmutableList.of())) {
                if (curDocument.compareTo("") != 0) {
                    final Document curDoc = this.docMap.get(curDocument.toLowerCase());
                    curAct.addOutputDoc(curDoc);
                    curAct.setEnableOutput(1);
                }
            }
        }
    }

    private void createEmitterObjects(final Map<String, List<String>> activitiesFields) {

        final Iterator<String> it = activitiesFields.keySet().iterator();
        while (it.hasNext()) {
            final String currentActivity = it.next();
            final List<String> currentFieldList = activitiesFields.getOrDefault(currentActivity,
                    ImmutableList.of());

            final Activity curAct = this.taskMap.get(currentActivity.toLowerCase());
            final Iterator<Document> actDoc = curAct.getOutputDocs().iterator();
            while (actDoc.hasNext()) {
                final Document doc = actDoc.next();
                Emitter emitField = null;
                try {
                    emitField = new Emitter(doc.getId().toLowerCase(), "OUT");
                } catch (final Exception e) {
                    System.out.println(currentActivity);
                    System.exit(0);
                }

                for (final String curField : currentFieldList) {
                    final String completeFieldName = curField.toLowerCase();
                    emitField.addToOutputMap(completeFieldName, completeFieldName);
                }
                curAct.addEmitter(emitField);
            }

        }
    }

    private static String getLowerCase(final String string) {
        final String newString = string.replaceAll("\\s", "").toLowerCase();
        return newString;
    }

    public static void main(final String[] args) {
        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser()
                    .withOption("o", "ontology",
                            "the ontology FILE (default: " + Constants.DEFAULT_ONTOLOGY_FILE + ")",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption("d", "diagram",
                            "the diagram FILE (default: " + Constants.DEFAULT_DIAGRAM_FILE + ")",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption("r", "result",
                            "the FILE where to store resulting traces (default: "
                                    + Constants.DEFAULT_TRACES_FILE + ")",
                            "FILE", CommandLine.Type.FILE, true, false, true)
                    .withOption("n", "num-traces",
                            "the NUMBER of traces to generate (default: " + DEFAULT_NUM_TRACES
                                    + ")",
                            "NUMBER", CommandLine.Type.NON_NEGATIVE_INTEGER, true, false, false)
                    .withHeader("Test the population of a PROMO repository") //
                    .withLogger(LoggerFactory.getLogger("eu.fbk.pdi.promo")) //
                    .parse(args);

            // Read command line options
            final Path ontologyFile = cmd.getOptionValue("o", Path.class,
                    Constants.DEFAULT_ONTOLOGY_FILE);
            final Path diagramFile = cmd.getOptionValue("d", Path.class,
                    Constants.DEFAULT_DIAGRAM_FILE);
            final Path resultFile = cmd.getOptionValue("r", Path.class,
                    Constants.DEFAULT_TRACES_FILE);
            final int numTraces = cmd.getOptionValue("n", Integer.class, DEFAULT_NUM_TRACES);

            // Delegate
            simulate(ontologyFile, diagramFile, resultFile, numTraces);

        } catch (final Throwable ex) {
            // Report failure
            CommandLine.fail(ex);
        }
    }

    public static void simulate(final Path ontologyFile, final Path diagramFile,
            final Path resultFile, final int numTraces) throws IOException {

        final SimulatorConfig config = new SimulatorConfig(ontologyFile, diagramFile);
        final Simulator simulator = new Simulator(config);

        try (Writer out = IO.utf8Writer(IO.buffer(IO.write(resultFile.toString())))) {
            final long traceSeparation = 62946; // in ms, ~501K traces / year
            final long ts = System.currentTimeMillis();
            long curTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault())
                    .toInstant().toEpochMilli();
            for (int i = 0; i < numTraces; i++, curTime += traceSeparation) {
                // This code is inherently serial due to shared state. Do not parallelize!
                final ProcessInstance pI = simulator.createProcessInstance();
                pI.setExecutionMode("COMPLETE");
                pI.execute(config);
                final JsonObject trace = pI.toJson(config, "t" + i, curTime);
                GSON.toJson(trace, out);
                out.write("\n");
                final boolean availableExecutions = simulator.setNextIteration();
                if (!availableExecutions) {
                    simulator.resetSwitches();
                }
                if ((i + 1) % 1000 == 0) {
                    LOGGER.info("{} traces processed", i + 1);
                }
            }
            LOGGER.info("{} traces generated in {} ms", numTraces,
                    System.currentTimeMillis() - ts);
        }
    }

}
