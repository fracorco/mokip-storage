package eu.fbk.pdi.promo.util;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.core.CommandLine.Type;
import eu.fbk.utils.core.IO;

/**
 * Generates a trace dataset of the size requested, starting from the RDF files of the ontological
 * model and of some input sample trace files.
 */
public class DatasetGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetGenerator.class);

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    public static void main(final String... args) {
        try {
            // Define options and parse command line
            final CommandLine cmd = CommandLine.parser()
                    .withName("java -cp ... eu.fbk.pdi.promo.evaluation")
                    .withOption("k", "keep-traces",
                            "the input RDF FILE providing traces to necessarily emit", "FILE",
                            Type.FILE_EXISTING, true, false, true)
                    .withOption("e", "extra-traces",
                            "the input RDF FILE providing extra traces that may be emitted)",
                            "FILE", Type.FILE_EXISTING, true, false, true)
                    .withOption("m", "model",
                            "the FILE containing the ontological model RDF graph", "FILE",
                            Type.FILE_EXISTING, true, false, false)
                    .withOption("o", "output", "the output dataset FILE to generate", "FILE",
                            Type.FILE, true, false, true)
                    .withOption("n", "num-traces", "the number of traces to include in the output",
                            "NUM", Type.POSITIVE_INTEGER, true, false, true)
                    .withOption("i", "model-iri",
                            "the IRI of the emitted named graph with the ontological model", "IRI",
                            Type.STRING, true, false, false)
                    .withHeader("Generates a trace dataset, emitting the ontological model and "
                            + "specified number of traces and taking as example the "
                            + "traces supplied in input files")
                    .parse(args);

            // Extract option values
            final Path pathKeep = cmd.getOptionValue("k", Path.class);
            final Path pathExtra = cmd.getOptionValue("e", Path.class);
            final Path pathModel = cmd.getOptionValue("m", Path.class);
            final Path pathOut = cmd.getOptionValue("o", Path.class);
            final int numTraces = cmd.getOptionValue("n", Integer.class);
            final IRI ontoModelIri = VF
                    .createIRI(cmd.getOptionValue("i", String.class, "promo:ontomodel"));

            // Allocate a counter to track the number of triples emitted
            final AtomicInteger counter = new AtomicInteger();

            // Allocate a set to keep track of trace graphs being emitted, so to stop once we
            // reach the required number of traces
            final Set<IRI> traceContexts = Sets.newHashSet();

            // Open the output file for RDF writing
            final RDFHandler sink = RDFHandlers.write(null, 1, pathOut.toString());
            try {
                // Signal RDF start (not done by called methods below)
                final long ts = System.currentTimeMillis();
                sink.startRDF();

                // Emit ontological model in its named graph
                copy(pathModel, sink, stmt -> VF.createStatement(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject(), ontoModelIri), counter);

                // Emit the mandatory traces, tracking their graphs so avoid emitting them next
                copy(pathKeep, sink, stmt -> {
                    if (!(stmt.getContext() instanceof IRI)
                            || ontoModelIri.equals(stmt.getContext())) {
                        return null;
                    }
                    synchronized (traceContexts) {
                        final boolean isNew = traceContexts.add((IRI) stmt.getContext());
                        if (isNew) {
                            LOGGER.debug("Starting trace {} ({}/{})", stmt.getContext(),
                                    traceContexts.size(), numTraces);
                        }
                    }
                    return stmt;
                }, counter);

                // Emit additional traces up to requested numbers, taking them from extra file
                final Set<IRI> blockedContexts = Sets.newHashSet(traceContexts);
                for (int i = 0; traceContexts.size() < numTraces; ++i) {
                    final int iteration = i;
                    copy(pathExtra, sink, stmt -> {

                        // Drop all statements not referring to a trace, based on named graph
                        if (!(stmt.getContext() instanceof IRI)
                                || stmt.getContext().equals(ontoModelIri)) {
                            return null;
                        }

                        // Extract context IRI that identifies the trace
                        final IRI ctx = (IRI) stmt.getContext();

                        // Rewrite context IRI to generate "new" traces, if needed. Abort if a
                        // trace was emitted in previous iterations for the same context
                        final IRI c = rewrite(ctx, ctx, iteration);
                        if (blockedContexts.contains(c)) {
                            return null;
                        }

                        // Rewrite subject, predicate, and object of "new" trace, if needed
                        final Resource s = rewrite(stmt.getSubject(), ctx, iteration);
                        final IRI p = stmt.getPredicate();
                        final Value o = p.equals(RDF.TYPE) ? stmt.getObject()
                                : rewrite(stmt.getObject(), ctx, iteration);

                        // Drop the statement, if it refers to a new (rewritten) trace and we have
                        // already committed to emit statements for the requested number of traces
                        synchronized (traceContexts) {
                            if (!traceContexts.contains(c)) {
                                if (traceContexts.size() >= numTraces) {
                                    return null;
                                }
                                traceContexts.add(c);
                                LOGGER.debug("Starting trace {} ({}/{})", c, traceContexts.size(),
                                        numTraces);
                            }
                        }

                        // Emit rewritten statement finally
                        return VF.createStatement(s, p, o, c);
                    }, counter);
                }

                // Signal RDF end
                sink.endRDF();
                LOGGER.info("Emitted {} with {} traces and {} triples in {} ms", pathOut,
                        traceContexts.size(), counter, System.currentTimeMillis() - ts);

            } finally {
                // Close the output RDF file anyway, once done
                IO.closeQuietly(sink);
            }

        } catch (final Throwable ex) {
            // Kaputt
            CommandLine.fail(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Value> T rewrite(final T value, final IRI ctx, final int iteration) {

        // Rewrite only if we already ran out of input traces
        if (iteration > 0 && value instanceof IRI) {

            // Trace contexts and actor instances have to be rewritten
            final String valueStr = value.stringValue();
            if (value.equals(ctx)
                    || valueStr.startsWith("http://dkm.fbk.eu/Pop_TTrace_Ontology#actor/")) {
                return (T) VF.createIRI(value.stringValue() + "_dup" + iteration);
            }

            // Any other IRI that refers to the trace context has to be rewritten
            final String ctxId = ctx.getLocalName();
            final int start = valueStr.indexOf(ctxId);
            if (start >= 0) {
                final int end = start + ctxId.length();
                return (T) VF.createIRI(
                        valueStr.substring(0, end) + "_dup" + iteration + valueStr.substring(end));
            }
        }

        // Emit value unchanged, if rewriting should not be done
        return value;
    }

    private static void copy(final Path in, final RDFHandler out,
            final Function<Statement, Statement> filter, final AtomicInteger counter) {

        // Read and map each input statement into an output statement to be sent to the handler
        RDFSources.read(true, true, null, null, null, true, in.toString())
                .emit(new AbstractRDFHandler() {

                    @Override
                    public void handleNamespace(final String prefix, final String uri)
                            throws RDFHandlerException {
                        out.handleNamespace(prefix, uri);
                    }

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        final Statement filteredStmt = filter.apply(stmt);
                        if (filteredStmt != null) {
                            out.handleStatement(filteredStmt);
                            final int emitted = counter.incrementAndGet();
                            if (emitted % 1000000 == 0) {
                                LOGGER.debug("Emitted {} triples", emitted);
                            }
                        }
                    }

                }, 1);
    }

}
