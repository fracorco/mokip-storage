package eu.fbk.pdi.promo.util;

import java.nio.file.Path;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.core.CommandLine.Type;
import eu.fbk.utils.core.IO;

public class DatasetFilter {

    public static void main(final String... args) {
        try {
            final CommandLine cmd = CommandLine.parser()
                    .withOption("i", "input", "the input dataset FILE to filter", "FILE",
                            Type.FILE_EXISTING, true, false, true)
                    .withOption("k", "keep", "the input dataset FILE to preserve as-is in output",
                            "FILE", Type.FILE_EXISTING, true, false, false)
                    .withOption("o", "output", "the output dataset FILE to generate", "FILE",
                            Type.FILE, true, false, true)
                    .withOption("n", "num-traces", "the number of traces to include in the output",
                            "NUM", Type.POSITIVE_INTEGER, true, false, true)
                    .withOption("m", "model",
                            "the IRI of the named graph with the ontological model", "IRI",
                            Type.STRING, true, false, false)
                    .withHeader("Filters a trace dataset, "
                            + "emitting a new dataset with the number of traces specified")
                    .parse(args);

            final Path pathIn = cmd.getOptionValue("i", Path.class);
            final Path pathKeep = cmd.getOptionValue("k", Path.class);
            final Path pathOut = cmd.getOptionValue("o", Path.class);
            final int numTraces = cmd.getOptionValue("n", Integer.class);
            final IRI ontoModelIri = SimpleValueFactory.getInstance()
                    .createIRI(cmd.getOptionValue("m", String.class, "promo:ontomodel"));

            final Set<IRI> keepFileContexts = Sets.newConcurrentHashSet();
            final Set<IRI> traceContexts = Sets.newHashSet();

            final RDFHandler sink = RDFHandlers.write(null, 1, pathOut.toString());
            try {
                sink.startRDF();

                if (pathKeep != null) {
                    copy(pathKeep, sink, stmt -> {
                        if (stmt.getContext() instanceof IRI) {
                            keepFileContexts.add((IRI) stmt.getContext());
                            if (!stmt.getContext().equals(ontoModelIri)) {
                                traceContexts.add((IRI) stmt.getContext());
                            }
                        }
                        return true;
                    });
                }

                copy(pathIn, sink, stmt -> {
                    if (stmt.getContext() instanceof IRI) {
                        final IRI ctx = (IRI) stmt.getContext();
                        if (!keepFileContexts.contains(ctx)) {
                            if (!ctx.equals(ontoModelIri)) {
                                synchronized (traceContexts) {
                                    if (!traceContexts.contains(ctx)) {
                                        if (traceContexts.size() >= numTraces) {
                                            return false;
                                        }
                                        traceContexts.add((IRI) stmt.getContext());
                                    }
                                }
                            }
                            return true;
                        }
                    }
                    return false;
                });

                sink.endRDF();

            } finally {
                IO.closeQuietly(sink);
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static void copy(final Path in, final RDFHandler out,
            final Predicate<Statement> filter) {
        RDFSources.read(true, true, null, null, null, true, in.toString())
                .emit(new AbstractRDFHandler() {

                    @Override
                    public void handleNamespace(final String prefix, final String uri)
                            throws RDFHandlerException {
                        out.handleNamespace(prefix, uri);
                    }

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        if (filter.test(stmt)) {
                            out.handleStatement(stmt);
                        }
                    }

                }, 1);
    }

}
