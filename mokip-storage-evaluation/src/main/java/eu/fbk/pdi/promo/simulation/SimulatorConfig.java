package eu.fbk.pdi.promo.simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pdi.promo.simulation.elements.IncomingFlowElement;
import eu.fbk.pdi.promo.simulation.elements.Pool;
import eu.fbk.pdi.promo.vocab.DOMAIN;
import eu.fbk.rdfpro.RDFSources;

public class SimulatorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimulatorConfig.class);

    // Document Name => sorted List of Fields
    private Map<String, List<String>> documentsFields;

    // Activity Name => sorted List of Output Documents
    private Map<String, List<String>> activitiesDocuments;

    // Activity Name => sorted List of Created Fields
    private Map<String, List<String>> activitiesFields;

    // One Field Name => Meaning in the Document model (key, startdate, enddate, startenddate)
    private Map<String, String> meaningFields;

    // Pool Name => Pool Object
    private Map<String, Pool> poolMap;

    // Sorted list of Pool Objects
    private List<Pool> poolList;

    // Element ID => Element Type
    private Map<String, String> elementsType;

    // Element ID => Element Name
    private Map<String, String> allElements;

    // Oryx Activity ID => Activity Name
    private Map<String, String> activities;

    // Oryx Activity Name => Activity ID
    private Map<String, String> activitiesNames;

    // Oryx Activity Name => Container Pool Name
    private Map<String, String> activitiesPools;

    // Oryx Start Element ID => Start Element Name
    private Map<String, String> startElements;

    // Oryx End Element ID => End Element Name
    private Map<String, String> endElements;

    // Oryx Gateway ID => Gateway Name
    private Map<String, String> gatewaysMap;

    // Oryx Element ID => sorted Vector containing All Outgoings
    private Map<String, List<String>> outgoing;

    // Oryx Element ID => sorted Vector containing All Incoming
    private Map<String, List<IncomingFlowElement>> incoming;

    public SimulatorConfig(final Path ontologyPath, final Path jsonDiagramPath) {

        LOGGER.debug("Initialization started");

        this.documentsFields = new HashMap<>();
        this.activitiesDocuments = new HashMap<>();
        this.activitiesFields = new HashMap<>();
        this.meaningFields = new HashMap<>();
        this.poolMap = new HashMap<>();
        this.poolList = new ArrayList<>();

        this.elementsType = new HashMap<>();
        this.allElements = new HashMap<>();
        this.activities = new HashMap<>();
        this.activitiesNames = new HashMap<>();
        this.activitiesPools = new HashMap<>();
        this.startElements = new HashMap<>();
        this.endElements = new HashMap<>();
        this.gatewaysMap = new HashMap<>();
        this.outgoing = new HashMap<>();
        this.incoming = new HashMap<>();

        createMoKiEntityMaps(ontologyPath);
        buildDomainKnowledge(jsonDiagramPath);
        // dumpEntitiesMaps();

        // Sort lists and make data immutable
        this.documentsFields.values().forEach(l -> Ordering.natural().immutableSortedCopy(l));
        this.documentsFields = ImmutableMap.copyOf(this.documentsFields);
        this.activitiesDocuments.values().forEach(l -> Ordering.natural().immutableSortedCopy(l));
        this.activitiesDocuments = ImmutableMap.copyOf(this.activitiesDocuments);
        this.activitiesFields.values().forEach(l -> Ordering.natural().immutableSortedCopy(l));
        this.activitiesFields = ImmutableMap.copyOf(this.activitiesFields);
        this.meaningFields = ImmutableMap.copyOf(this.meaningFields);
        this.activitiesPools = ImmutableMap.copyOf(this.activitiesPools);
        this.elementsType = ImmutableMap.copyOf(this.elementsType);
        this.allElements = ImmutableMap.copyOf(this.allElements);
        this.activities = ImmutableMap.copyOf(this.activities);
        this.activitiesNames = ImmutableMap.copyOf(this.activitiesNames);
        this.startElements = ImmutableMap.copyOf(this.startElements);
        this.endElements = ImmutableMap.copyOf(this.endElements);
        this.gatewaysMap = ImmutableMap.copyOf(this.gatewaysMap);
        this.outgoing = ImmutableMap.copyOf(this.outgoing);
        this.incoming = ImmutableMap.copyOf(this.incoming);
        this.poolMap = ImmutableMap.copyOf(this.poolMap);
        this.poolList = Ordering.natural().immutableSortedCopy(this.poolList);

        LOGGER.info("Associations Documents->Fields ({}/{}): {}", //
                this.documentsFields.size(),
                this.documentsFields.values().stream().flatMap(l -> l.stream()).count(),
                this.documentsFields.keySet());
        LOGGER.info("Associations Activities->Documents ({}/{}): {}",
                this.activitiesDocuments.size(),
                this.activitiesDocuments.values().stream().flatMap(l -> l.stream()).count(),
                this.activitiesDocuments.keySet());
        LOGGER.info("Associations Activities->Fields ({}/{}): {}", //
                this.activitiesFields.size(),
                this.activitiesFields.values().stream().flatMap(l -> l.stream()).count(),
                this.activitiesFields.keySet());
        LOGGER.info("Pools ({}): {}", this.poolList.size(), this.poolList);
    }

    /**
     * Reads the axioms from the ontology and initialize the maps used for quickly access to the
     * ontology entities
     */
    private void createMoKiEntityMaps(final Path ontologyPath) {

        // Allocate an in-memory repository for loading and querying the ontology
        final Sail sail = new MemoryStore();
        final Repository repo = new SailRepository(sail);
        repo.init();
        try (final RepositoryConnection connection = repo.getConnection()) {

            // Load ontology
            final long ts = System.currentTimeMillis();
            final List<Statement> ontologyStmts = Lists.newArrayList();
            RDFSources.read(false, true, null, null, null, true, ontologyPath.toString())
                    .emit(new StatementCollector(ontologyStmts), 1);
            connection.begin();
            connection.add(ontologyStmts);
            connection.commit();
            LOGGER.debug("Ontological model loaded from {} in {} ms: {} triples", ontologyPath,
                    System.currentTimeMillis() - ts, ontologyStmts.size());

            querySubClasses(connection, DOMAIN.DOCUMENT, c -> {
                final String id = getLowerCaseWithoutUnderscore(c.getLocalName());
                this.documentsFields.put(id, Lists.newArrayList());
            });

            querySubClasses(connection, DOMAIN.ACTIVITY, c -> {
                final String id = getLowerCaseWithoutUnderscore(c.getLocalName());
                this.activitiesDocuments.put(id, Lists.newArrayList());
                this.activitiesFields.put(id, Lists.newArrayList());
            });

            querySubClasses(connection, DOMAIN.ROLE, c -> {
                final String id = getLowerCaseWithoutUnderscore(c.getLocalName());
                final Pool pool = new Pool(id, id);
                this.poolList.add(pool);
                this.poolMap.put(id, pool);
            });

            queryAllowedPropertyValues(connection, DOMAIN.HAS_FIELD, (c, v) -> {
                final String documentId = getLowerCaseWithoutUnderscore(c.getLocalName());
                final String fieldId = getLowerCaseWithoutUnderscore(v.getLocalName());
                this.documentsFields.computeIfAbsent(documentId, k -> Lists.newArrayList())
                        .add(fieldId);
            });

            queryAllowedPropertyValues(connection, DOMAIN.HAS_OUTPUT, (c, v) -> {
                final String activityId = getLowerCaseWithoutUnderscore(c.getLocalName());
                final String documentId = getLowerCaseWithoutUnderscore(v.getLocalName());
                this.activitiesDocuments.computeIfAbsent(activityId, k -> Lists.newArrayList())
                        .add(documentId);
            });

            queryAllowedPropertyValues(connection, DOMAIN.CREATES, (c, v) -> {
                final String activityId = getLowerCaseWithoutUnderscore(c.getLocalName());
                final String fieldId = getLowerCaseWithoutUnderscore(v.getLocalName());
                this.activitiesFields.computeIfAbsent(activityId, k -> Lists.newArrayList())
                        .add(fieldId);
            });

        } finally {
            // Release the in-memory repository
            repo.shutDown();
        }
    }

    private static void querySubClasses(final RepositoryConnection connection,
            final IRI superClass, final Consumer<IRI> callback) {

        final String query = ""//
                + "SELECT DISTINCT ?c \n" //
                + "WHERE { \n" //
                + "  BIND (<" + superClass + "> AS ?sc) \n" //
                + "  ?c rdfs:subClassOf+ ?sc .\n" //
                + "  FILTER (?c != ?sc) \n" //
                + "  FILTER (isIri(?c)) \n" //
                + "  FILTER (STRSTARTS(STR(?c), '" + superClass.getNamespace() + "')) \n" //
                + "  FILTER NOT EXISTS { \n" //
                + "    ?c1 rdfs:subClassOf ?c . \n" //
                + "    FILTER (?c1 != ?c) \n" //
                + "    FILTER (isIri(?c1)) \n" //
                + "    FILTER (STRSTARTS(STR(?c1), '" + superClass.getNamespace() + "')) \n" //
                + "  } \n" //
                + "}";

        try (final TupleQueryResult cursor = connection.prepareTupleQuery(query).evaluate()) {
            while (cursor.hasNext()) {
                final BindingSet bindings = cursor.next();
                final IRI c = (IRI) bindings.getValue("c");
                callback.accept(c);
            }
        }
    }

    private static void queryAllowedPropertyValues(final RepositoryConnection connection,
            final IRI property, final BiConsumer<IRI, IRI> callback) {

        final String query = ""//
                + "SELECT DISTINCT ?c ?v \n" //
                + "WHERE { \n" //
                + "  ?c rdfs:subClassOf ?r .\n" //
                + "  ?r owl:onProperty <" + property + "> .\n" //
                + "  { ?r owl:allValuesFrom ?v } UNION \n" //
                + "  { ?r owl:allValuesFrom/owl:unionOf/rdf:rest*/rdf:first ?v } \n" //
                + "  FILTER (isIRI(?c)) \n" //
                + "  FILTER (isIRI(?v)) \n" //
                + "}";

        try (final TupleQueryResult cursor = connection.prepareTupleQuery(query).evaluate()) {
            while (cursor.hasNext()) {
                final BindingSet bindings = cursor.next();
                final IRI c = (IRI) bindings.getValue("c");
                final IRI v = (IRI) bindings.getValue("v");
                callback.accept(c, v);
            }
        }
    }

    /**
     * Gets information from the JSON of the Oryx BPMN Diagram
     */
    private void buildDomainKnowledge(final Path jsonDiagramPath) {

        // Reads json file and calls GSON for transforming the json input file into objects
        JsonObject oE;
        try (BufferedReader bR = new BufferedReader(new FileReader(jsonDiagramPath.toFile()))) {
            oE = new Gson().fromJson(bR, JsonObject.class);
        } catch (final Throwable ex) {
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }

        // Analyzes diagram elements, building different vectors: activities, XORGateways,
        // ORGateways, ANDGateways, dataObjects and connectingFlow
        final Map<String, List<String>> incomings = Maps.newHashMap();
        final Map<String, List<String>> outgoings = Maps.newHashMap();
        final Map<String, List<String>> conditions = Maps.newHashMap();
        final AtomicReference<String> currentPool = new AtomicReference<>(null);
        analyzeOryxElements(incomings, outgoings, conditions, currentPool, oE);

        // Builds the outgoing and incoming map for each type of element that has to be considered
        // for the rules. Creates an item in the actvityVector for each activity.
        for (final String flowElement : this.allElements.keySet()) {
            buildOutgoingAndIncomingMap(incomings, outgoings, conditions, flowElement);
        }
    }

    /**
     * Analyzes the diagram elements, building different vectors: pools, activities, XORGateways,
     * ORGateways, ANDGateways, dataObjects and connectingFlow
     *
     * @param oryxElement
     */
    private void analyzeOryxElements( //
            final Map<String, List<String>> incomings, //
            final Map<String, List<String>> outgoings, //
            final Map<String, List<String>> conditions, //
            final AtomicReference<String> currentPool, //
            final JsonObject oryxElement) {

        final String oryxElementResourceId = oryxElement.get("resourceId").getAsString();

        final JsonObject propertiesJs = oryxElement.getAsJsonObject("properties");
        final String oryxElementName = propertiesJs.has("name")
                ? propertiesJs.get("name").getAsString()
                : null;
        final String conditionExpression = propertiesJs.has("conditionexpression")
                ? propertiesJs.get("conditionexpression").getAsString()
                : null;

        final String oryxElementType = oryxElement.getAsJsonObject("stencil").get("id")
                .getAsString();

        final List<String> outgoingRefs = Lists.newArrayList();
        if (oryxElement.has("outgoing")) {
            for (final JsonElement element : oryxElement.get("outgoing").getAsJsonArray()) {
                outgoingRefs.add(element.getAsJsonObject().get("resourceId").getAsString());
            }
        }

        this.elementsType.put(oryxElementResourceId,
                getLowerCaseWithoutUnderscore(oryxElementType));

        OryxElementType enumOryxElementType = null;
        try {
            enumOryxElementType = OryxElementType.valueOf(oryxElementType);
        } catch (final IllegalArgumentException e) {
            enumOryxElementType = OryxElementType.valueOf("BPMNDiagram");
        }

        switch (enumOryxElementType) {
        case Pool:
            currentPool.set(getLowerCaseWithoutUnderscore(oryxElementName));
            break;
        case Task:
        case Activity:
        case CollapsedSubprocess:
            final String name = getLowerCase(oryxElementName);
            this.allElements.put(oryxElementResourceId, name);
            this.activities.put(oryxElementResourceId, name);
            this.activitiesNames.put(name, oryxElementResourceId);
            this.activitiesPools.put(name, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case StartNoneEvent:
            String nameStart = getLowerCase(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameStart);
            this.startElements.put(oryxElementResourceId, nameStart);
            this.activitiesNames.put(nameStart, oryxElementResourceId);
            this.activitiesPools.put(nameStart, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case StartMessageEvent:
        case IntermediateMessageEventCatching:
        case IntermediateMessageEventThrowing:
            nameStart = getLowerCase(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameStart);
            this.activities.put(oryxElementResourceId, nameStart);
            this.startElements.put(oryxElementResourceId, nameStart);
            this.activitiesNames.put(nameStart, oryxElementResourceId);
            this.activitiesPools.put(nameStart, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case EndNoneEvent:
            String nameEnd = getLowerCase(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameEnd);
            this.endElements.put(oryxElementResourceId, nameEnd);
            this.activitiesNames.put(nameEnd, oryxElementResourceId);
            this.activitiesPools.put(nameEnd, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case EndMessageEvent:
            nameEnd = getLowerCase(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameEnd);
            this.activities.put(oryxElementResourceId, nameEnd);
            this.endElements.put(oryxElementResourceId, nameEnd);
            this.activitiesNames.put(nameEnd, oryxElementResourceId);
            this.activitiesPools.put(nameEnd, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case Exclusive_Databased_Gateway:
        case EventBased_Gateway:
        case EventbasedGateway:
            String nameGtw = getLowerCaseWithoutUnderscore(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameGtw);
            this.gatewaysMap.put(oryxElementResourceId, nameGtw);
            this.activitiesNames.put(nameGtw, oryxElementResourceId);
            this.activitiesPools.put(nameGtw, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case InclusiveGateway:
            nameGtw = getLowerCaseWithoutUnderscore(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameGtw);
            this.gatewaysMap.put(oryxElementResourceId, nameGtw);
            this.activitiesNames.put(nameGtw, oryxElementResourceId);
            this.activitiesPools.put(nameGtw, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case ParallelGateway:
            nameGtw = getLowerCaseWithoutUnderscore(oryxElementName);
            this.allElements.put(oryxElementResourceId, nameGtw);
            this.gatewaysMap.put(oryxElementResourceId, nameGtw);
            this.activitiesNames.put(nameGtw, oryxElementResourceId);
            this.activitiesPools.put(nameGtw, currentPool.get());
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            break;
        case SequenceFlow:
        case Association_Undirected:
        case Association_Unidirectional:
        case MessageFlow:
            addIncomingsAndOutgoings(incomings, outgoings, oryxElementResourceId, outgoingRefs);
            if (conditionExpression != null && !conditionExpression.equals("")) {
                final String[] conditionsList = conditionExpression.split(";");
                if (conditionsList.length > 0) {
                    final Vector<String> andConditions = new Vector<String>();
                    for (int i = 0; i < conditionsList.length; i++) {
                        final String condition = conditionsList[i].substring(1,
                                conditionsList[i].length() - 1);
                        final String conditionRef = this.activitiesNames
                                .get(getLowerCase(condition));
                        andConditions.add(conditionRef);
                    }
                    conditions.put(oryxElementResourceId, andConditions);
                }
            }
            break;
        default:
        }

        if (oryxElement.has("childShapes")) {
            for (final JsonElement child : oryxElement.get("childShapes").getAsJsonArray()) {
                analyzeOryxElements(incomings, outgoings, conditions, currentPool,
                        child.getAsJsonObject());
            }
        }
    }

    /**
     * Starting from the allOutgoings and allIncomings maps, creates two finer maps, outgoing and
     * incoming, respectively, in which the incoming and outgoing relationships are set only among
     * flowObjects. In case of conditions related to executed activities they are also added to
     * the condition map (allConditions).
     *
     * @param flowElementRef
     */
    private void buildOutgoingAndIncomingMap( //
            final Map<String, List<String>> incomings, //
            final Map<String, List<String>> outgoings, //
            final Map<String, List<String>> conditions, //
            final String flowElementRef) {

        final List<String> outgoingRefs = outgoings.get(flowElementRef);
        final List<String> fEOutgoingRefs = new Vector<String>();
        for (final String outgoingRef : outgoingRefs) {
            final List<String> sFOutgoingRefs = outgoings.get(outgoingRef);
            if (sFOutgoingRefs != null) {
                for (final String sFOutgoingRef : sFOutgoingRefs) {
                    fEOutgoingRefs.add(sFOutgoingRef);
                }
            }
        }
        this.outgoing.put(flowElementRef, fEOutgoingRefs);

        final List<String> incomingRefs = incomings.get(flowElementRef);
        if (incomingRefs != null) {
            final Vector<IncomingFlowElement> fEIncomingRefs = new Vector<IncomingFlowElement>();
            for (final String incomingRef : incomingRefs) {
                final List<String> sFIncomingRefs = incomings.get(incomingRef);
                if (sFIncomingRefs != null) {
                    final List<String> conditionVector = conditions.get(incomingRef);
                    for (@SuppressWarnings("unused")
                    final String sFIncomingRef : sFIncomingRefs) {
                        IncomingFlowElement incomingElement;
                        if (conditionVector == null) {
                            incomingElement = new IncomingFlowElement(new ArrayList<>());
                        } else {
                            incomingElement = new IncomingFlowElement(conditionVector);
                        }
                        fEIncomingRefs.add(incomingElement);
                    }
                }
            }
            this.incoming.put(flowElementRef, fEIncomingRefs);
        }
    }

    /**
     * (i) For each element, builds a vector with the id of each outgoing element and associates
     * it to the specific element (id) in the allOutgoingsMap; (ii) for each outgoing element of
     * the current element, adds the current element (id) to the vector with the ids of incoming
     * elements in the allIncomings map.
     *
     * @param oryxElementResourceId
     * @param outgoingRefs
     */
    private void addIncomingsAndOutgoings( //
            final Map<String, List<String>> incomings, //
            final Map<String, List<String>> outgoings, //
            final String oryxElementResourceId, //
            final List<String> outgoingRefs) {

        final Vector<String> tempOutgoing = new Vector<String>();
        List<String> tempIncoming = new Vector<String>();
        for (final String outgoingRef : outgoingRefs) {
            tempOutgoing.add(outgoingRef);
            tempIncoming = incomings.get(outgoingRef);
            if (tempIncoming == null) {
                tempIncoming = new Vector<String>();
            }
            tempIncoming.add(oryxElementResourceId);
            incomings.put(outgoingRef, tempIncoming);
        }
        outgoings.put(oryxElementResourceId, tempOutgoing);
    }

    /**
     ***************************************************
     ****** ATTRIBUTES GETTER AND SETTER FUNCTIONS *****
     ***************************************************
     */

    public Map<String, List<String>> getDocumentsFields() {
        return this.documentsFields;
    }

    public Map<String, List<String>> getActivitiesDocuments() {
        return this.activitiesDocuments;
    }

    public Map<String, List<String>> getActivitiesFields() {
        return this.activitiesFields;
    }

    public Map<String, String> getMeaningFields() {
        return this.meaningFields;
    }

    public Map<String, String> getActivitiesPools() {
        return this.activitiesPools;
    }

    public Map<String, String> getElementsType() {
        return this.elementsType;
    }

    public Map<String, String> getAllElements() {
        return this.allElements;
    }

    public Map<String, String> getActivities() {
        return this.activities;
    }

    public Map<String, String> getActivitiesNames() {
        return this.activitiesNames;
    }

    public Map<String, String> getStartElements() {
        return this.startElements;
    }

    public Map<String, String> getEndElements() {
        return this.endElements;
    }

    public Map<String, String> getGatewaysMap() {
        return this.gatewaysMap;
    }

    public Map<String, List<String>> getOutgoing() {
        return this.outgoing;
    }

    public Map<String, List<IncomingFlowElement>> getIncoming() {
        return this.incoming;
    }

    public Map<String, Pool> getPoolMap() {
        return this.poolMap;
    }

    public List<Pool> getPoolList() {
        return this.poolList;
    }

    private static String getLowerCase(final String string) {
        final String newString = string.replaceAll("\\s", "").toLowerCase();
        return newString;
    }

    private static String getLowerCaseWithoutUnderscore(final String string) {
        final String newString = string.replaceAll("\\s", "").replace("_", "").toLowerCase();
        return newString;
    }

    private enum OryxElementType {

        Pool,

        Task,

        Activity,

        CollapsedSubprocess,

        StartNoneEvent,

        StartMessageEvent,

        IntermediateMessageEventCatching,

        IntermediateMessageEventThrowing,

        EndNoneEvent,

        EndMessageEvent,

        Exclusive_Databased_Gateway,

        EventBased_Gateway,

        EventbasedGateway,

        InclusiveGateway,

        ParallelGateway,

        DataObject,

        SequenceFlow,

        Association_Undirected,

        Association_Unidirectional,

        MessageFlow,

        BPMNDiagram,

        greenDocumentObjectId_doc,

        Lane;

    }

}
