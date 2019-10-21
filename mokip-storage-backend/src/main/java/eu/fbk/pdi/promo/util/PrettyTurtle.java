package eu.fbk.pdi.promo.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;

import eu.fbk.rdfpro.util.Statements;

public final class PrettyTurtle implements RDFWriterFactory {

    private static final Ordering<Value> VALUE_ORDERING = Ordering
            .from(Statements.valueComparator(RDF.NAMESPACE));

    public static PrettyTurtle INSTANCE = new PrettyTurtle();

    public static final RDFFormat FORMAT = new RDFFormat("Pretty Turtle",
            "text/turtle;prettify=true", Charset.forName("UTF-8"), "ttl", true, true);

    private volatile Predicate<? super BNode> defaultBNodePreservationPolicy;

    private volatile boolean defaultGeneratePrefixes;

    public PrettyTurtle() {
        this.defaultBNodePreservationPolicy = Predicates.alwaysFalse();
        this.defaultGeneratePrefixes = false;
    }

    public Predicate<? super BNode> getDefaultBNodePreservationPolicy() {
        return this.defaultBNodePreservationPolicy;
    }

    public void setDefaultBNodePreservationPolicy(
            final Predicate<? super BNode> defaultBNodePreservationPolicy) {
        if (defaultBNodePreservationPolicy != null) {
            this.defaultBNodePreservationPolicy = defaultBNodePreservationPolicy;
        } else {
            this.defaultBNodePreservationPolicy = Predicates.alwaysFalse();
        }
    }

    public boolean isDefaultGeneratePrefixes() {
        return this.defaultGeneratePrefixes;
    }

    public void setDefaultGeneratePrefixes(final boolean defaultGeneratePrefixes) {
        this.defaultGeneratePrefixes = defaultGeneratePrefixes;
    }

    @Override
    public RDFFormat getRDFFormat() {
        return FORMAT;
    }

    @Override
    public RDFWriter getWriter(final OutputStream stream) {
        return getWriter(stream, this.defaultBNodePreservationPolicy,
                this.defaultGeneratePrefixes);
    }

    @Override
    public RDFWriter getWriter(final OutputStream out, final String baseURI)
            throws URISyntaxException {
        return getWriter(out); // baseURI ignored
    }

    public RDFWriter getWriter(final OutputStream stream,
            final Predicate<? super BNode> bnodePreservationPolicy,
            final boolean generatePrefixes) {
        return new RDFWriter(stream, bnodePreservationPolicy, generatePrefixes);
    }

    @Override
    public RDFWriter getWriter(final Writer writer) {
        return getWriter(writer, this.defaultBNodePreservationPolicy,
                this.defaultGeneratePrefixes);
    }

    @Override
    public RDFWriter getWriter(final Writer writer, final String baseURI)
            throws URISyntaxException {
        return getWriter(writer); // baseURI ignored
    }

    public RDFWriter getWriter(final Writer writer,
            final Predicate<? super BNode> bnodePreservationPolicy,
            final boolean generatePrefixes) {
        return new RDFWriter(writer, bnodePreservationPolicy, generatePrefixes);
    }

    private static class RDFWriter extends TurtleWriter {

        private final Predicate<? super BNode> bnodePreservationPolicy;

        private final boolean generatePrefixes;

        private final Map<Resource, Multimap<IRI, Value>> subjectProperties;

        // value true if bnode must be emitted
        private final Map<BNode, Boolean> objectBNodes;

        private final Set<String> mentionedNamespaces;

        private final Set<BNode> inlinedBNodes;

        public RDFWriter(final OutputStream out,
                final Predicate<? super BNode> bnodePreservationPolicy,
                final boolean generatePrefixes) {
            // Assume UTF-8 is the encoding, as for specification.
            this(new OutputStreamWriter(out, Charset.forName("UTF-8")), bnodePreservationPolicy,
                    generatePrefixes);
        }

        public RDFWriter(final Writer writer,
                final Predicate<? super BNode> bnodePreservationPolicy,
                final boolean generatePrefixes) {
            super(writer);

            this.bnodePreservationPolicy = bnodePreservationPolicy;
            this.generatePrefixes = generatePrefixes;

            this.subjectProperties = Maps.newLinkedHashMap();
            this.objectBNodes = Maps.newHashMap();
            this.mentionedNamespaces = Sets.newHashSet();
            this.inlinedBNodes = Sets.newLinkedHashSet();
        }

        @Override
        public RDFFormat getRDFFormat() {
            return FORMAT;
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            Preconditions.checkState(this.writingStarted, "Writing not yet started");

            // Temporarily change the state, in order for the overridden method to work properly
            // (if writingStarted, namespaces will be emitted)
            final boolean writingStarted = this.writingStarted;
            this.writingStarted = false;

            super.handleNamespace(prefix, uri);

            this.writingStarted = writingStarted;
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            Preconditions.checkState(this.writingStarted, "Writing not yet started");

            final Resource subject = statement.getSubject();
            final IRI predicate = statement.getPredicate();
            final Value object = statement.getObject();

            registerMentionedNamespaces(subject);
            registerMentionedNamespaces(predicate);
            registerMentionedNamespaces(object);
            if (object instanceof Literal
                    && !((Literal) object).getDatatype().equals(XMLSchema.STRING)) {
                registerMentionedNamespaces(((Literal) object).getDatatype());
            }

            Multimap<IRI, Value> properties = this.subjectProperties.get(subject);
            if (properties == null) {
                properties = LinkedHashMultimap.create();
                this.subjectProperties.put(subject, properties);
            }
            properties.put(predicate, object);

            if (object instanceof BNode && !this.bnodePreservationPolicy.apply((BNode) object)) {
                this.objectBNodes.put((BNode) object,
                        this.objectBNodes.containsKey(object) || object.equals(subject)
                                ? Boolean.TRUE
                                : Boolean.FALSE);
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // FIXME Comments currently ignored.
        }

        @Override
        public void endRDF() throws RDFHandlerException {

            try {
                if (this.generatePrefixes) {
                    generatePrefixes();
                }

                writeNamespaces();
                writeStatements();

            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);

            } finally {
                super.endRDF();
            }
        }

        private void registerMentionedNamespaces(final Value value) {
            if (value instanceof IRI) {
                this.mentionedNamespaces.add(((IRI) value).getNamespace());
            }
        }

        private void generatePrefixes() throws RDFHandlerException {
            final boolean writingStarted = this.writingStarted;
            this.writingStarted = false;

            for (final String namespace : Sets.difference(this.mentionedNamespaces,
                    this.namespaceTable.keySet())) {
                final int endIndex = Math.max(namespace.lastIndexOf(':'),
                        Math.max(namespace.lastIndexOf('/'), namespace.lastIndexOf('#')));
                int startIndex = endIndex;
                while (startIndex > 0 && Character.isLetter(namespace.charAt(startIndex - 1))) {
                    --startIndex;
                }
                if (startIndex >= endIndex) {
                    continue;
                }
                final String candidatePrefix = namespace.substring(startIndex, endIndex)
                        .toLowerCase();
                if (!this.namespaceTable.containsKey(candidatePrefix)) {
                    super.handleNamespace(candidatePrefix, namespace);
                }
            }

            this.writingStarted = writingStarted;
        }

        private void writeNamespaces() throws IOException {
            if (!this.namespaceTable.isEmpty()) {
                for (final Map.Entry<String, String> namespace : this.namespaceTable.entrySet()) {
                    final String prefix = namespace.getValue();
                    final String uri = namespace.getKey();
                    if (this.mentionedNamespaces.contains(uri)) {
                        writeNamespace(prefix, uri);
                    }
                }
            }
        }

        private void writeStatements() throws IOException {
            // Keep track of BNodes not emitted as subjects.
            final Set<BNode> skippedBNodes = Sets.newLinkedHashSet();

            // Emit subjects and their properties, skipping bnodes that can be potentially inlined
            boolean first = true;
            for (final Resource subject : VALUE_ORDERING
                    .sortedCopy(this.subjectProperties.keySet())) {

                final Multimap<IRI, Value> properties = this.subjectProperties.get(subject);

                final boolean emitSubject = !(subject instanceof BNode)
                        || this.bnodePreservationPolicy.apply((BNode) subject)
                        || this.objectBNodes.get(subject) != Boolean.FALSE;

                if (emitSubject) {
                    if (!first) {
                        this.writer.writeEOL();
                    }
                    writeSubject(subject, properties);
                    first = false;

                } else {
                    skippedBNodes.add((BNode) subject);
                }
            }

            // Emit bnodes skipped as subject but not inlined as objects.
            while (true) {
                skippedBNodes.removeAll(this.inlinedBNodes);
                this.inlinedBNodes.clear();
                if (skippedBNodes.isEmpty()) {
                    break;
                }
                if (!first) {
                    this.writer.writeEOL();
                }
                final Iterator<BNode> iterator = skippedBNodes.iterator();
                final BNode node = iterator.next();
                iterator.remove();
                writeSubject(node, this.subjectProperties.get(node));
                first = false;
            }
        }

        private void writeSubject(final Resource subject, final Multimap<IRI, Value> properties)
                throws IOException {
            this.writer.writeEOL();

            if (!(subject instanceof BNode) || this.bnodePreservationPolicy.apply((BNode) subject)
                    || this.objectBNodes.containsKey(subject)) {
                writeResource(subject, false);
                this.writer.write(" ");
            } else {
                this.writer.write("[] ");
            }

            this.writer.increaseIndentation();
            writeProperties(properties);
            this.writer.write(" .");
            this.writer.decreaseIndentation();
        }

        private void writeProperties(final Multimap<IRI, Value> properties) throws IOException {
            boolean first = true;
            for (final IRI property : VALUE_ORDERING.sortedCopy(properties.keySet())) {
                if (!first) {
                    this.writer.write(" ;");
                    this.writer.writeEOL();
                }
                writeProperty(property, properties.get(property));
                first = false;
            }
        }

        private void writeProperty(final IRI predicate, final Collection<Value> values)
                throws IOException {
            if (predicate.equals(RDF.TYPE)) {
                this.writer.write("a");
            } else {
                writeURI(predicate);
            }
            this.writer.write(" ");

            // Emit the property values in two phases. First, IRIs, literals and BNodes whose ID
            // must be preserved are emitted (phase = 0). Then, BNodes that can be expanded inline
            // are emitted. The expansion check is done here and passed to writeObject() as hint.
            boolean first = true;
            for (int phase = 0; phase < 2 && !values.isEmpty(); ++phase) {
                for (final Iterator<Value> iterator = values.iterator(); iterator.hasNext();) {
                    final Value value = iterator.next();
                    final boolean bnodeExpansion = value instanceof BNode
                            && !this.bnodePreservationPolicy.apply((BNode) value)
                            && this.objectBNodes.get(value) != Boolean.TRUE;
                    if (!bnodeExpansion && phase == 0 || bnodeExpansion && phase == 1) {
                        if (!first) {
                            this.writer.write(" , ");
                        }
                        writeObject(value, bnodeExpansion);
                        first = false;
                    }
                }
            }
        }

        private void writeObject(final Value value, final Boolean bnodeExpansionHint)
                throws IOException {
            // Determine whether a BNode expansion must occur, possibly reusing the supplied hint.
            final boolean bnodeExpansion = bnodeExpansionHint != null
                    ? bnodeExpansionHint.booleanValue()
                    : value instanceof BNode && !this.bnodePreservationPolicy.apply((BNode) value)
                            && this.objectBNodes.get(value) != Boolean.TRUE;

            if (!bnodeExpansion) {
                writeValue(value, false);
            } else {
                this.inlinedBNodes.add((BNode) value);
                Multimap<IRI, Value> properties = this.subjectProperties.get(value);

                if (properties == null) {
                    // No properties: emit an empty blank node.
                    this.writer.write("[]");

                } else if (!properties.containsKey(RDF.FIRST)) {
                    // Some properties, not a collection: emit the properties inline.
                    this.writer.write("[");
                    this.writer.increaseIndentation();
                    this.writer.writeEOL();
                    writeProperties(properties);
                    this.writer.decreaseIndentation();
                    this.writer.writeEOL();
                    this.writer.write("]");

                } else {
                    // A collection: emit it inline.
                    this.writer.write("(");
                    Value node = value;
                    while (true) {
                        this.writer.write(" ");
                        final Value element = Iterables.getFirst(properties.get(RDF.FIRST), null);
                        writeObject(element, null); // no expansion hint here
                        node = Iterables.getFirst(properties.get(RDF.REST), null);
                        if (node != null && !node.equals(RDF.NIL)) {
                            properties = this.subjectProperties.get(node);
                            this.inlinedBNodes.add((BNode) node);
                        } else {
                            break;
                        }
                    }
                    this.writer.write(" )");
                }
            }
        }

    }

}
