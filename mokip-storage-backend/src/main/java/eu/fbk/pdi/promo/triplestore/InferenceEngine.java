package eu.fbk.pdi.promo.triplestore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import com.google.common.io.Resources;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.eclipse.rdf4j.sail.config.SailRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.RuleEngine;
import eu.fbk.rdfpro.Ruleset;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;

public abstract class InferenceEngine implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferenceEngine.class);

    private static final int MAX_FAILURES = 5;

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private volatile boolean closed = false;

    public static InferenceEngine newGraphDBEngine(@Nullable final Iterable<Statement> schemaStmts,
            @Nullable final String ruleset, final boolean schemaImport) {
        return new GraphDBInferencer(schemaStmts, ruleset, schemaImport);
    }

    public static InferenceEngine newRDFproEngine(@Nullable final Iterable<Statement> schemaStmts,
            final Ruleset ruleset) {
        return new RDFproInferenceEngine(schemaStmts, ruleset);
    }

    public static InferenceEngine newIdentityEngine() {
        return new InferenceEngine() {

            @Override
            public RDFHandler doApply(final RDFHandler sink) {
                return sink;
            }

        };
    }

    public final List<Statement> apply(final Iterable<Statement> stmts) {

        Objects.requireNonNull(stmts);
        Preconditions.checkState(!this.closed);

        final List<Statement> sink = stmts instanceof Collection
                ? Lists.newArrayListWithCapacity(4 * Iterables.size(stmts))
                : Lists.newArrayList();

        int numFailures = 0;
        while (true) {
            try {
                final RDFHandler handler = doApply(new StatementCollector(sink));
                try {
                    handler.startRDF();
                    for (final Statement stmt : stmts) {
                        handler.handleStatement(stmt);
                    }
                    handler.endRDF();
                } finally {
                    IO.closeQuietly(handler);
                }
                return sink;

            } catch (final Throwable ex) {
                ++numFailures;
                if (numFailures >= MAX_FAILURES) {
                    Throwables.throwIfUnchecked(ex);
                    throw new RuntimeException(ex);
                }
                LOGGER.warn("Inference computation failed on attempt " + numFailures + "/"
                        + MAX_FAILURES + ", trying again (error: " + ex.getMessage() + ")");
                sink.clear();
            }
        }
    }

    public final boolean isClosed() {
        return this.closed;
    }

    @Override
    public final synchronized void close() {
        if (!this.closed) {
            this.closed = true;
            doClose();
        }
    }

    abstract RDFHandler doApply(RDFHandler sink);

    void doClose() {
    }

    static final long size(final Repository repository) {
        try (RepositoryConnection connection = repository.getConnection()) {
            try (TupleQueryResult c = connection.prepareTupleQuery( //
                    "SELECT (COUNT(*) AS ?n) { ?s ?p ?o }").evaluate()) {
                return ((Literal) c.next().getValue("n")).longValue();
            }
        }
    }

    private static final class GraphDBInferencer extends InferenceEngine {

        private final List<Statement> schema;

        private final Path schemaPath;

        private final Set<Statement> blacklist;

        private final String ruleset;

        private final AtomicInteger counter;

        private final List<Backend> created;

        private final List<Backend> free;

        GraphDBInferencer(@Nullable final Iterable<Statement> schemaStmts,
                @Nullable String ruleset, final boolean schemaImport) {

            // Allocate a temporary file for the ruleset, if it is not a file
            if (ruleset == null) {
                ruleset = "empty";
            } else if (ruleset.endsWith(".pie")) {
                try {
                    final URL url = new URL(ruleset);
                    final byte[] bytes = Resources.toByteArray(url);
                    final File tempFile = File.createTempFile("promo-", ".pie");
                    tempFile.deleteOnExit();
                    Files.write(bytes, tempFile);
                    ruleset = tempFile.getAbsolutePath();
                } catch (final MalformedURLException ex) {
                    // Ignore: assume the ruleset is already a file path
                } catch (final IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            // Deduplicate scema
            final Set<Statement> schema = schemaStmts == null || Iterables.isEmpty(schemaStmts)
                    ? null
                    : ImmutableSet.copyOf(schemaStmts);

            // Emit schema to file, if needed
            Path schemaPath = null;
            if (schema != null && schemaImport) {
                try {
                    schemaPath = File.createTempFile("promo-schema-", ".trig").toPath();
                    final String filename = schemaPath.toString();
                    final RDFFormat format = Rio.getWriterFormatForFileName(filename).orElse(null);
                    try (final Writer out = IO.utf8Writer(IO.buffer(IO.write(filename)))) {
                        Rio.write(schema, out, format);
                    }
                } catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }

            // Initialize object except blacklist of statements not to return as inferences
            this.schema = schema == null ? null : ImmutableList.copyOf(schema);
            this.schemaPath = schemaPath;
            this.ruleset = ruleset;
            this.counter = new AtomicInteger(0);
            this.created = Lists.newArrayList();
            this.free = Lists.newArrayList();

            // Initialize blacklisted statements, if schema supplied and schema import not used
            final Set<Statement> blacklist = Sets.newHashSet();
            if (this.schema != null && schemaPath == null) {
                final Backend backend = allocate();
                try (RepositoryConnection connection = backend.repository.getConnection()) {
                    connection.add(this.schema);
                    connection.exportStatements(null, null, null, true, new AbstractRDFHandler() {

                        @Override
                        public void handleStatement(final Statement stmt)
                                throws RDFHandlerException {
                            blacklist.add(Statements.normalize(stmt));
                        }

                    });
                } finally {
                    release(backend);
                }
            }
            this.blacklist = ImmutableSet.copyOf(blacklist);
        }

        @Override
        RDFHandler doApply(final RDFHandler sink) {

            return new AbstractRDFHandlerWrapper(sink) {

                private Backend backend;

                private RepositoryConnection connection;

                @Override
                public void startRDF() throws RDFHandlerException {
                    super.startRDF();
                    this.backend = allocate();
                    this.connection = this.backend.repository.getConnection();
                    this.connection.begin();
                    if (GraphDBInferencer.this.schema != null
                            && GraphDBInferencer.this.schemaPath == null) {
                        this.connection.add(GraphDBInferencer.this.schema);
                    }
                }

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    this.connection.add(statement);
                }

                @Override
                public void endRDF() throws RDFHandlerException {
                    try {
                        final Set<Statement> blacklist = this.backend.blacklist.isEmpty() ? null
                                : this.backend.blacklist;
                        this.connection.commit();
                        this.connection.exportStatements(null, null, null, true,
                                new AbstractRDFHandler() {

                                    @Override
                                    public void handleStatement(final Statement stmt)
                                            throws RDFHandlerException {
                                        if (blacklist == null || !blacklist.contains(stmt)) {
                                            handler.handleStatement(Statements.normalize(stmt));
                                        }
                                    }

                                });
                    } finally {
                        try {
                            IO.closeQuietly(this.connection);
                            release(this.backend);
                        } finally {
                            this.connection = null;
                            this.backend = null;
                        }
                    }
                    super.endRDF();
                }

                @Override
                public void close() {
                    try {
                        if (this.backend != null) {
                            try {
                                IO.closeQuietly(this.connection);
                                release(this.backend);
                            } finally {
                                this.connection = null;
                                this.backend = null;
                            }
                        }
                    } finally {
                        super.close();
                    }
                }

            };
        }

        @Override
        synchronized void doClose() {

            // Delete all the repositories in the created list
            for (final Backend backend : ImmutableList.copyOf(this.created)) {
                discard(backend);
            }
            this.created.clear();
            this.free.clear();
        }

        private Backend allocate() {

            // Reuse a repository from the pool, if available
            final long ts = System.currentTimeMillis();
            synchronized (this) {
                Preconditions.checkState(!isClosed());
                if (!this.free.isEmpty()) {
                    final Backend backend = this.free.remove(0);
                    if (LOGGER.isDebugEnabled()) {
                        final long size = size(backend.repository);
                        LOGGER.debug("Repository at {} allocated in {} ms, {} tr (reused)",
                                backend.repository.getDataDir(), System.currentTimeMillis() - ts,
                                size);
                    }
                    return backend;
                }
            }

            // Otherwise retrieve factory
            final SailRegistry registry = SailRegistry.getInstance();
            final SailFactory factory = registry.get("graphdb:FreeSail").get();

            // Allocate a temporary directory for the sail
            final File dir = new File(System.getProperty("java.io.tmpdir") + "/graphdb-"
                    + this.counter.incrementAndGet() + "-" + System.currentTimeMillis());

            // Define configuration properties
            final Map<String, String> properties = new HashMap<String, String>();
            properties.put("ruleset", this.ruleset);
            properties.put("noPersist", "true");
            properties.put("storage-folder", dir.getAbsolutePath());
            properties.put("entity-index-size", "100000");
            properties.put("enable-context-index", "true");
            properties.put("in-memory-literal-properties", "true");
            if (this.schemaPath != null) {
                properties.put("defaultNS", "schema:");
                properties.put("imports", this.schemaPath.toString());
            }

            // Store configuration properties in requested RDF graph
            final Model graph = new LinkedHashModel();
            final Resource node = VF.createBNode("graphdb");
            graph.add(node, VF.createIRI("http://www.openrdf.org/config/sail#sailType"),
                    VF.createLiteral("graphdb:FreeSail"));
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                graph.add(node,
                        VF.createIRI("http://www.ontotext.com/trree/owlim#", entry.getKey()),
                        VF.createLiteral(entry.getValue()));
            }

            // Create the temporary directory and create and initialize the repository
            Backend backend = null;
            try {
                final SailImplConfig config = factory.getConfig();
                config.parse(graph, node);
                final Sail sail = factory.getSail(config);
                sail.setDataDir(dir);
                final Repository repository = new SailRepository(sail);
                repository.setDataDir(dir);
                backend = new Backend(repository, this.blacklist);
                synchronized (this) {
                    Preconditions.checkState(!isClosed());
                    Files.createParentDirs(dir);
                    repository.init();
                    if (this.schemaPath != null) {
                        final List<Statement> blacklist = Lists.newArrayList();
                        try (RepositoryConnection connection = repository.getConnection()) {
                            connection.exportStatements(null, null, null, true,
                                    new AbstractRDFHandler() {

                                        @Override
                                        public void handleStatement(final Statement stmt)
                                                throws RDFHandlerException {
                                            blacklist.add(Statements.normalize(stmt));
                                        }

                                    });
                        }
                        backend = new Backend(repository, ImmutableSet.copyOf(blacklist));
                    }
                    if (LOGGER.isDebugEnabled()) {
                        final long size = size(repository);
                        LOGGER.debug("Repository at {} allocated in {} ms, {} tr (new)", dir,
                                System.currentTimeMillis() - ts, size);
                    }
                    this.created.add(backend);
                    return backend;
                }
            } catch (final Throwable ex) {
                if (backend != null) {
                    discard(backend);
                }
                throw new RepositoryException("Could not create GraphDB repository", ex);
            }
        }

        private void release(final Backend backend) {

            try {
                // Clear data in the repository to prepare it for reuse
                final long ts = System.currentTimeMillis();
                try (RepositoryConnection connection = backend.repository.getConnection()) {
                    if (this.schemaPath != null) {
                        connection.prepareUpdate("DROP DEFAULT").execute();
                    } else {
                        connection.clear();
                    }
                }

                // Log result
                if (LOGGER.isDebugEnabled()) {
                    final long size = size(backend.repository);
                    LOGGER.debug("Repository at {} released in {} ms, {} tr",
                            backend.repository.getDataDir(), System.currentTimeMillis() - ts,
                            size);
                }

                // Return the repository to the pool
                synchronized (this) {
                    Preconditions.checkState(!isClosed());
                    this.free.add(backend);
                }

            } catch (final Throwable ex) {
                // If something goes wrong, drop the repository (so a new one will be allocated)
                discard(backend);
                LOGGER.warn("Could not release repository at " + backend.repository.getDataDir()
                        + ": dropped it (error: " + ex.getMessage() + ")");
            }
        }

        private void discard(final Backend backend) {

            // Abort if no repository supplied
            if (backend.repository == null) {
                return;
            }

            // Shutdown repository and delete its data directory
            final File dataDir = backend.repository.getDataDir();
            try {
                final long ts = System.currentTimeMillis();
                backend.repository.shutDown();
                if (dataDir != null) {
                    MoreFiles.deleteRecursively(dataDir.toPath());
                }
                LOGGER.debug("Repository at {} deleted in {} ms", dataDir,
                        System.currentTimeMillis() - ts);
            } catch (final IOException ex) {
                LOGGER.warn("Could not delete repository at " + dataDir + " (error: "
                        + ex.getMessage() + ")");
            }

            // Remove repository from created ones
            synchronized (this) {
                this.created.remove(backend);
                this.free.remove(backend);
            }
        }

        private final class Backend {

            final Repository repository;

            final Set<Statement> blacklist;

            Backend(final Repository repository, Set<Statement> blacklist) {
                this.repository = repository;
                this.blacklist = blacklist;
            }

        }

    }

    private static final class RDFproInferenceEngine extends InferenceEngine {

        private final RuleEngine engine;

        RDFproInferenceEngine(@Nullable final Iterable<Statement> schemaStmts, Ruleset ruleset) {

            Objects.requireNonNull(ruleset);

            if (schemaStmts != null) {
                final QuadModel model = QuadModel.create(schemaStmts);
                ruleset = ruleset.getABoxRuleset(model);
            }

            this.engine = RuleEngine.create(ruleset);
        }

        @Override
        RDFHandler doApply(final RDFHandler sink) {
            return this.engine.eval(sink, false);
        }

    }

}
