package eu.fbk.pdi.promo.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Constants for the DOMAIN vocabulary.
 */
public class DOMAIN {

    /** Recommended prefix for the vocabulary namespace: "domain". */
    public static final String PREFIX = "domain";

    /** Vocabulary namespace: "https://dkm.fbk.eu/#". */
    public static final String NAMESPACE = "https://dkm.fbk.eu/#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    // CLASSES

    /** Class domain:Document. */
    public static final IRI DOCUMENT = createIRI("Document");

    /** Class domain:Activity. */
    public static final IRI ACTIVITY = createIRI("Activity");

    /** Class domain:Role. */
    public static final IRI ROLE = createIRI("Role");

    // PROPERTIES

    /** Property domain:hasOutput. */
    public static final IRI HAS_OUTPUT = createIRI("hasOutput");

    /** Property domain:hasOutputField. */
    public static final IRI HAS_OUTPUT_FIELD = createIRI("hasOutputField");

    /** Property domain:hasField. */
    public static final IRI HAS_FIELD = createIRI("hasField");

    /** Property domain:value. */
    public static final IRI VALUE = createIRI("value");

    /** Property domain:creates. */
    public static final IRI CREATES = createIRI("creates");

    /** Property domain:hasMeaning. */
    public static final IRI HAS_MEANING = createIRI("hasMeaning");

    // HELPER METHODS

    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(NAMESPACE, localName);
    }

    private DOMAIN() {
    }

}
