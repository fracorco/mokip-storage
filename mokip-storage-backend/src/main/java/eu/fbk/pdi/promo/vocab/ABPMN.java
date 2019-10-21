package eu.fbk.pdi.promo.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Constants for the ABPMN vocabulary.
 */
public class ABPMN {

    /** Recommended prefix for the vocabulary namespace: "abpmn". */
    public static final String PREFIX = "abpmn";

    /** Vocabulary namespace: "http://dkm.fbk.eu/index.php/ABPMN_Ontology#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/index.php/ABPMN_Ontology#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    // HELPER METHODS

    @SuppressWarnings("unused")
    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(NAMESPACE, localName);
    }

    private ABPMN() {
    }

}
