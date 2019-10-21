package eu.fbk.pdi.promo.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Constants for the TRACE vocabulary.
 */
public class TRACE {

    /** Recommended prefix for the vocabulary namespace: "trace". */
    public static final String PREFIX = "trace";

    /** Vocabulary namespace: "http://dkm.fbk.eu/index.php/BPMN_Ontology#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/Trace_Ontology#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    // PROPERTIES

    /** Property trace:compliance. */
    public static final IRI COMPLIANCE = createIRI("compliance");

    /** Property trace:has_flow_object_performer. */
    public static final IRI HAS_FLOW_OBJECT_PERFORMER = createIRI("has_flow_object_performer");

    /** Property trace:has_process_trace_flow_object. */
    public static final IRI HAS_PROCESS_TRACE_FLOW_OBJECT = createIRI(
            "has_process_trace_flow_object");

    /** Property trace:has_process_trace_data_object. */
    public static final IRI HAS_PROCESS_TRACE_DATA_OBJECT = createIRI(
            "has_process_trace_data_object");

    /** Property trace:initial_start_dateTime. */
    public static final IRI INITIAL_START_DATETIME = createIRI("initial_start_dateTime");

    /** Property trace:final_start_dateTime. */
    public static final IRI FINAL_START_DATETIME = createIRI("final_start_dateTime");

    /** Property trace:initial_end_dateTime. */
    public static final IRI INITIAL_END_DATETIME = createIRI("initial_end_dateTime");

    /** Property trace:final_end_dateTime. */
    public static final IRI FINAL_END_DATETIME = createIRI("final_end_dateTime");

    /** Property trace:probability. */
    public static final IRI PROBABILITY = createIRI("probability");

    /** Property trace:execution_of */
    public static final IRI EXECUTION_OF = createIRI("execution_of");

    /** Property trace:number_of_activities */
    public static final IRI NUMBER_OF_ACTIVITIES = createIRI("number_of_activities");

    /** Property trace:number_of_documents */
    public static final IRI NUMBER_OF_DOCUMENTS = createIRI("number_of_documents");

    /** Property trace:number_of_documents */
    public static final IRI NUMBER_OF_FIELDS = createIRI("number_of_fields");

    /** Property trace:number_of_documents */
    public static final IRI NUMBER_OF_SIMPLE_FIELDS = createIRI("number_of_simple_fields");

    /** Property trace:number_of_documents */
    public static final IRI NUMBER_OF_COMPLEX_FIELDS = createIRI("number_of_complex_fields");

    // HELPER METHODS

    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(NAMESPACE, localName);
    }

    private TRACE() {
    }

}
