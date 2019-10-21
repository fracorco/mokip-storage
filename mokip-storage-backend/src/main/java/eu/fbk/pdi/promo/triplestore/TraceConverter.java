package eu.fbk.pdi.promo.triplestore;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pdi.promo.util.Constants;
import eu.fbk.pdi.promo.vocab.BPMN;
import eu.fbk.pdi.promo.vocab.DOMAIN;
import eu.fbk.pdi.promo.vocab.TRACE;
import eu.fbk.rdfpro.Ruleset;

// Query to generate field instances
//
// PREFIX domain: <https://dkm.fbk.eu/#>
// CONSTRUCT {
// ?docInstance domain:hasField ?fieldInstance .
// ?fieldInstance a ?fieldType .
// }
// WHERE {
// ?docInstance a ?restriction , domain:Document.
// ?restriction owl:onProperty domain:hasField; owl:allValuesFrom ?union .
// { SELECT ?union ?fieldType WHERE { ?union owl:unionOf/rdf:rest*/rdf:first ?fieldType } }
// BIND (IRI(CONCAT(STR(?docInstance), "_", STRAFTER(STR(?fieldType), STR(domain:)))) AS
// ?fieldInstance)
// }

public class TraceConverter {

    public static final String DEFAULT_TRACE_NAMESPACE = "trace:";

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceConverter.class);

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private final String namespace;

    private final Map<String, IRI> processRefs;

    private final Map<String, IRI> actorRefs;

    private final Map<String, IRI> activityRefs;

    private final Map<String, IRI> documentRefs;

    private final Map<String, IRI> fieldRefs;

    public TraceConverter(@Nullable final String namespace, final Iterable<Statement> ontoModel) {

        // Initialize data structures
        this.namespace = MoreObjects.firstNonNull(namespace, DEFAULT_TRACE_NAMESPACE);
        this.processRefs = Maps.newHashMap();
        this.actorRefs = Maps.newHashMap();
        this.activityRefs = Maps.newHashMap();
        this.documentRefs = Maps.newHashMap();
        this.fieldRefs = Maps.newHashMap();

        // Compute closure of onto model
        final List<Statement> closedOntoModel;
        try (InferenceEngine engine = InferenceEngine.newRDFproEngine(null, Ruleset.RDFS)) {
            closedOntoModel = engine.apply(ontoModel);
        }

        // Allocate an in-memory repository for querying the ontological model
        final Sail sail = new MemoryStore();
        final Repository repo = new SailRepository(sail);
        repo.init();
        try (final RepositoryConnection connection = repo.getConnection()) {

            // Add inferred ontological model to working repository
            long ts = System.currentTimeMillis();
            connection.begin();
            connection.add(closedOntoModel);
            connection.commit();
            LOGGER.debug("Ontological model loaded for indexing in {} ms",
                    System.currentTimeMillis() - ts);

            // Extract and index process types
            ts = System.currentTimeMillis();
            final String processQuery = "" //
                    + "PREFIX bpmn: <" + BPMN.NAMESPACE + ">\n" //
                    + "SELECT DISTINCT ?name ?instance\n" //
                    + "WHERE {\n" //
                    + "  ?instance a bpmn:business_process_diagram;\n" //
                    + "     bpmn:has_business_process_diagram_name ?name.\n" //
                    + "}";
            try (final TupleQueryResult cursor = connection.prepareTupleQuery(processQuery)
                    .evaluate()) {
                while (cursor.hasNext()) {
                    final BindingSet bindings = cursor.next();
                    final String name = bindings.getValue("name").stringValue();
                    final IRI ref = (IRI) bindings.getValue("instance");
                    if (ref != null) {
                        this.processRefs.put(name, ref);
                    }
                }
            }
            LOGGER.info("Indexed {} processes in {} ms", this.processRefs.size(),
                    System.currentTimeMillis() - ts);

            // Extract and index actor types
            ts = System.currentTimeMillis();
            final String actorQuery = "" //
                    + "PREFIX bpmn: <" + BPMN.NAMESPACE + ">\n" //
                    + "SELECT DISTINCT ?name ?instance\n" //
                    + "WHERE {\n" //
                    + "  ?instance a bpmn:participant;\n" //
                    + "     bpmn:has_participant_name ?name.\n" //
                    + "}";
            try (final TupleQueryResult cursor = connection.prepareTupleQuery(actorQuery)
                    .evaluate()) {
                while (cursor.hasNext()) {
                    final BindingSet bindings = cursor.next();
                    final String name = bindings.getValue("name").stringValue().toLowerCase();
                    final IRI ref = (IRI) bindings.getValue("instance");
                    if (ref != null) {
                        this.actorRefs.put(name, ref);
                    }
                }
            }
            LOGGER.info("Indexed {} actors in {} ms", this.actorRefs.size(),
                    System.currentTimeMillis() - ts);

            // Extract and index activity types
            ts = System.currentTimeMillis();
            final String activityQuery = "" //
                    + "PREFIX bpmn: <" + BPMN.NAMESPACE + ">\n" //
                    + "SELECT DISTINCT ?name ?instance\n" //
                    + "WHERE {\n" //
                    + "  ?instance a bpmn:flow_object;\n" //
                    + "     bpmn:has_flow_object_name ?n.\n" //
                    + "  BIND (REPLACE(LCASE(?n),'[ _]+','') AS ?name)\n" //
                    + "}";
            try (final TupleQueryResult cursor = connection.prepareTupleQuery(activityQuery)
                    .evaluate()) {
                while (cursor.hasNext()) {
                    final BindingSet bindings = cursor.next();
                    final String name = bindings.getValue("name").stringValue().toLowerCase();
                    final IRI ref = (IRI) bindings.getValue("instance");
                    if (ref != null) {
                        this.activityRefs.put(name, ref);
                    }
                }
            }
            LOGGER.info("Indexed {} activities in {} ms", this.activityRefs.size(),
                    System.currentTimeMillis() - ts);

            // Extract and index document and field types
            ts = System.currentTimeMillis();
            final String docQuery = "" //
                    + "PREFIX domain: <" + DOMAIN.NAMESPACE + ">\n" //
                    + "PREFIX bpmn: <" + BPMN.NAMESPACE + ">\n" //
                    + "SELECT DISTINCT ?actorName ?docName ?docInstance ?fieldName ?fieldInstance\n" //
                    + "WHERE {\n" //
                    + "  ?docInstance a domain:Document; bpmn:has_data_object_name ?name.\n" //
                    + "  BIND (REPLACE(LCASE(?name), '[ _]+', '') AS ?docName)\n" //
                    + "  OPTIONAL {" //
                    + "    ?docInstance ^bpmn:has_process_graphical_elements/\n" //
                    + "                 ^bpmn:has_pool_process_ref/\n" //
                    + "                  bpmn:has_pool_participant_ref/\n" //
                    + "                  bpmn:has_participant_name ?actorName.\n" //
                    + "  }\n" //
                    + "  OPTIONAL {\n" //
                    + "    ?docInstance domain:hasField ?fieldInstance .\n" //
                    + "    BIND(STRAFTER(STRAFTER(STR(?fieldInstance), '@'), '.') AS ?suffix)\n" //
                    + "    BIND (REPLACE(LCASE(?suffix),'[ _]+','') AS ?fieldName)\n" //
                    + "  }\n" //
                    + "}";
            try (final TupleQueryResult cursor = connection.prepareTupleQuery(docQuery)
                    .evaluate()) {
                while (cursor.hasNext()) {
                    final BindingSet b = cursor.next();
                    final String actorName = b.getValue("actorName").stringValue().toLowerCase();
                    final String docName = b.getValue("docName").stringValue().toLowerCase();
                    final String fieldName = b.getValue("fieldName").stringValue().toLowerCase();
                    final IRI docRef = (IRI) b.getValue("docInstance");
                    final IRI fieldRef = (IRI) b.getValue("fieldInstance");
                    if (docRef != null) {
                        this.documentRefs.put(actorName + " " + docName, docRef);
                    }
                    if (fieldRef != null) {
                        this.fieldRefs.put(actorName + " " + docName + " " + fieldName, fieldRef);
                    }
                }
            }
            LOGGER.info("Indexed {} documents and {} fields in {} ms", this.documentRefs.size(),
                    this.fieldRefs.size(), System.currentTimeMillis() - ts);

        } finally {
            // Release the in-memory repository
            repo.shutDown();
        }
    }

    public void convertTrace(final RDFHandler handler, final JsonObject js) {
        new Conversion(handler, js);
    }

    public IRI convertTraceId(final String traceId) {
        return createIRI(this.namespace, traceId);
    }

    private static IRI createIRI(final String base, final Object... components) {
        final StringBuilder builder = new StringBuilder(base);
        String separator = base.endsWith("#") || base.endsWith("/") ? "" : ".";
        for (final Object component : components) {
            builder.append(separator);
            separator = ".";
            final String str = component.toString();
            for (int i = 0; i < str.length(); ++i) {
                final char c = str.charAt(i);
                if (c == ' ' || c == '@') {
                    builder.append('_');
                } else if (c != '.' || i < str.length() - 1) {
                    builder.append(c);
                }
            }
        }
        return VF.createIRI(builder.toString());
    }

    private class Conversion {

        private final RDFHandler handler;

        private final Set<IRI> actors;

        private int numActivities;

        private int numDocuments;

        private int numSimpleFields;

        private int numComplexFields;

        private int numTriples;

        public Conversion(final RDFHandler handler, final JsonObject js) {

            // Initialize state
            this.handler = handler;
            this.actors = Sets.newHashSet();
            this.numActivities = 0;
            this.numDocuments = 0;
            this.numSimpleFields = 0;
            this.numComplexFields = 0;

            // Extract process and traceId
            final String process = js.get("trace_process").getAsString();
            final String traceId = js.get("trace_id").getAsString();

            // Log operation
            LOGGER.debug("Converting trace {}", traceId);

            // Generate trace IRI
            final IRI traceIri = convertTraceId(traceId);

            // Emit triple linking the trace instance to the BPD ontology
            final IRI processRef = Objects.requireNonNull(
                    TraceConverter.this.processRefs.get(process),
                    () -> "Unknown process type for " + process);
            emit(traceIri, TRACE.EXECUTION_OF, processRef);

            // Emit triple specifying the compliance level of the trace w.r.t. the process
            emit(traceIri, TRACE.COMPLIANCE, VF.createLiteral("fully-compliant"));

            // Emit triples for the activities (and actors, documents and fields) in the trace
            final JsonArray activitiesJs = js.get("activities").getAsJsonArray();
            final List<IRI> activityIris = Lists.newArrayList();
            for (final JsonElement activityJs : activitiesJs) {
                ++this.numActivities;
                final IRI activityIri = convertActivity(traceIri, activityJs.getAsJsonObject());
                activityIris.add(activityIri);
            }
            list(new Resource[] { traceIri }, new IRI[] { TRACE.HAS_PROCESS_TRACE_FLOW_OBJECT },
                    Iterables.toArray(activityIris, IRI.class));

            // Emit start/end dates
            if (this.numActivities > 0) {
                final JsonObject as = activitiesJs.get(0).getAsJsonObject();
                final JsonObject ae = activitiesJs.get(this.numActivities - 1).getAsJsonObject();
                convertDate(this.handler, traceIri, TRACE.INITIAL_START_DATETIME, "start", "from",
                        as);
                convertDate(this.handler, traceIri, TRACE.FINAL_START_DATETIME, "start", "to", as);
                convertDate(this.handler, traceIri, TRACE.INITIAL_END_DATETIME, "end", "from", ae);
                convertDate(this.handler, traceIri, TRACE.FINAL_END_DATETIME, "end", "to", ae);
            }

            // Emit counters
            emit(traceIri, TRACE.NUMBER_OF_ACTIVITIES, VF.createLiteral(this.numActivities));
            emit(traceIri, TRACE.NUMBER_OF_DOCUMENTS, VF.createLiteral(this.numDocuments));
            emit(traceIri, TRACE.NUMBER_OF_SIMPLE_FIELDS, VF.createLiteral(this.numSimpleFields));
            emit(traceIri, TRACE.NUMBER_OF_COMPLEX_FIELDS,
                    VF.createLiteral(this.numComplexFields));
            emit(traceIri, TRACE.NUMBER_OF_FIELDS,
                    VF.createLiteral(this.numSimpleFields + this.numComplexFields));

            // Log statistics
            LOGGER.debug(
                    "Trace {} converted: {} actors, {} activities, {} documents, "
                            + "{} simple fields, {} complex fields, {} triples",
                    traceId, this.actors.size(), this.numActivities, this.numDocuments,
                    this.numSimpleFields, this.numComplexFields, this.numTriples);
        }

        private IRI convertActivity(final IRI traceIri, final JsonObject js) {

            // Generate activity IRI starting from activity ID and activity name in the trace
            final int activityNum = js.has("unfolding") ? js.get("unfolding").getAsInt() : 0;
            final String[] nameTokens = js.get("BP_activity_name").getAsString().split("\\.");
            final String activityId = nameTokens[0];
            final String activityName = nameTokens[1] + (activityNum > 0 ? "_" + activityNum : "");
            final IRI activityIri = createIRI(traceIri.stringValue(), activityName + activityId);
            LOGGER.debug("Converting activity {}", activityName);

            // Generate actor IRI starting from actor ID in the trace
            final String actorId = js.get("actorInstanceID").getAsString();
            final IRI actorIri = createIRI(TraceConverter.this.namespace, "actor", actorId);

            // Emit triple linking the activity instance in the trace to the BPD ontology
            final IRI activityType = Objects.requireNonNull(
                    TraceConverter.this.activityRefs.get(activityName),
                    () -> "Unknown activity type for " + activityName);
            emit(activityIri, TRACE.EXECUTION_OF, activityType);

            // Emit triples linking the activity to the trace and the actor that performed it
            // emit(traceIri, TRACE.HAS_PROCESS_TRACE_FLOW_OBJECT, activityIri);
            emit(activityIri, TRACE.HAS_FLOW_OBJECT_PERFORMER, actorIri);

            // Emit triple linking the actor instance in the trace to the BPD ontology
            final String actorName = js.get("actorClass").getAsString();
            if (this.actors.add(actorIri)) {
                final IRI actorRef = Objects.requireNonNull(
                        TraceConverter.this.actorRefs.get(actorName),
                        () -> "Unknown actor type for " + actorName);
                emit(actorIri, TRACE.EXECUTION_OF, actorRef);
            }

            // Emit triple encoding activity probability (default 1.0 if missing)
            final double probability = js.has("probability") ? //
                    js.get("probability").getAsDouble() : 1.0;
            emit(activityIri, TRACE.PROBABILITY, VF.createLiteral(probability));

            // Emit triples for activity start and end from-to date intervals
            convertDate(this.handler, activityIri, TRACE.INITIAL_START_DATETIME, "start", "from",
                    js);
            convertDate(this.handler, activityIri, TRACE.FINAL_START_DATETIME, "start", "to", js);
            convertDate(this.handler, activityIri, TRACE.INITIAL_END_DATETIME, "end", "from", js);
            convertDate(this.handler, activityIri, TRACE.FINAL_END_DATETIME, "end", "to", js);

            // Emit triples for the documents manipulated by the activity
            convertDocument(traceIri, activityIri, actorName,
                    js.get("dataContent").getAsJsonObject());

            // TODO ADD HERE ALSO THE PRECEDES RELATIONSHIPS!!!! (from original code)

            // Return activity IRI
            return activityIri;
        }

        private void convertDocument(final IRI traceIri, final IRI activityIri,
                final String actorName, final JsonObject js) {

            // Extract document name
            String documentName = null;
            for (final String field : js.keySet()) {
                if (!field.equals("filename")) {
                    documentName = field;
                }
            }

            // Abort if document name cannot be found
            if (documentName == null) {
                LOGGER.warn("Could not identify document name in {}", js);
                return;
            }

            // Generate document IRI
            ++this.numDocuments;
            final IRI documentIri = createIRI(activityIri.stringValue(), documentName);

            // Emit triples linking document to trace and activity
            emit(traceIri, TRACE.HAS_PROCESS_TRACE_DATA_OBJECT, documentIri);
            emit(activityIri, DOMAIN.HAS_OUTPUT, documentIri);

            // Emit a triple linking the document instance in the trace to the BPD ontology
            final String key = actorName + " " + documentName;
            final IRI documentRef = TraceConverter.this.documentRefs.get(key);
            if (documentRef != null) {
                emit(documentIri, TRACE.EXECUTION_OF, documentRef);
            } else {
                LOGGER.warn("Could not find document for {}", key);
            }

            // Extract and process fields, recursively
            final List<IRI> fieldIris = Lists.newArrayList();
            convertFields(documentIri.stringValue(), "",
                    js.getAsJsonObject(documentName).getAsJsonObject(documentName), documentName,
                    actorName, fieldIris);
            list(new IRI[] { activityIri, documentIri },
                    new IRI[] { DOMAIN.HAS_OUTPUT_FIELD, DOMAIN.HAS_FIELD },
                    Iterables.toArray(fieldIris, IRI.class));
        }

        private void convertFields(final String uriPrefix, final String namePrefix,
                final JsonObject js, final String documentName, final String actorName,
                final List<IRI> collectedFieldIris) {

            // Process fields
            for (final Entry<String, JsonElement> entry : js.entrySet()) {

                // Extract field name and simple value from the map
                final String fieldName = entry.getKey();
                final JsonElement fieldJson = entry.getValue();

                // Generate field IRI
                final IRI fieldIri = createIRI(uriPrefix, fieldName);

                if (fieldJson instanceof JsonPrimitive) {

                    // Handle simple field. Increment counter
                    ++this.numSimpleFields;

                    // Collect simple field IRI
                    collectedFieldIris.add(fieldIri);

                    // Emit triple linking the field instance in the trace to the BPD ontology
                    final String key = actorName + " " + documentName + " " + namePrefix
                            + fieldName;
                    final IRI fieldRef = TraceConverter.this.fieldRefs.get(key);
                    if (fieldRef != null) {
                        emit(fieldIri, TRACE.EXECUTION_OF, fieldRef);
                    } else {
                        LOGGER.warn("Could not find field for {}", key);
                    }

                    // Emit triples encoding field value and probability
                    emit(fieldIri, TRACE.PROBABILITY, VF.createLiteral(1.0));
                    emit(fieldIri, DOMAIN.VALUE, VF.createLiteral(fieldJson.getAsString()));

                } else if (entry.getValue() instanceof JsonObject) {

                    // Handle complex field. Increment counter
                    ++this.numComplexFields;

                    // Recursively process sub-fields, collecting their IRIs
                    convertFields(fieldIri.stringValue(), namePrefix + fieldName + ".",
                            entry.getValue().getAsJsonObject(), documentName, actorName,
                            collectedFieldIris);
                }
            }
        }

        private void convertDate(final RDFHandler handler, final IRI subjectIri,
                final IRI property, final String field, @Nullable final String subField,
                final JsonObject js) {

            // Extract date value from field / optional subfield in the map
            JsonElement value = js.get(field);
            if (value instanceof JsonObject) {
                value = subField == null ? null : value.getAsJsonObject().get(subField);
            }

            // Abort with a warning if the value is undefined
            if (!(value instanceof JsonPrimitive)) {
                LOGGER.warn("No value for property {} of {}", property, subjectIri);
            }

            // Emit a triple linking the subject to the date literal, if conversion if successful
            synchronized (Constants.DATE_FORMAT) {
                try {
                    final Date date = Constants.DATE_FORMAT.parse(value.getAsString());
                    emit(subjectIri, property, VF.createLiteral(date));
                } catch (final Throwable ex) {
                    LOGGER.warn("Could not parse date from field " + field + " / subfield "
                            + subField + " in " + js, ex);
                }
            }
        }

        private void list(final Resource[] subjects, final IRI[] predicates,
                final Resource[] elements) {

            final int n = elements.length;
            if (n == 0) {
                return;
            }

            for (int j = 0; j < subjects.length; ++j) {
                final Resource s = subjects[j];
                final IRI p = predicates[j];
                for (int i = 0; i < n; ++i) {
                    emit(s, p, elements[i]);
                }
            }
        }

        private void emit(final Resource subject, final IRI predicate, final Value object) {
            this.handler.handleStatement(VF.createStatement(subject, predicate, object));
            ++this.numTriples;
        }

    }

}
