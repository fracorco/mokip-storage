package eu.fbk.pdi.promo.triplestore;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pdi.promo.util.Constants;
import eu.fbk.pdi.promo.util.PrettyTurtle;
import eu.fbk.pdi.promo.util.Traces;
import eu.fbk.pdi.promo.vocab.ABPMN;
import eu.fbk.pdi.promo.vocab.BPMN;
import eu.fbk.pdi.promo.vocab.DOMAIN;
import eu.fbk.pdi.promo.vocab.TRACE;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.Ruleset;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.core.IO;

public final class TraceStorer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceStorer.class);

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Gson GSON = new Gson();

    private final String traceNamespace;

    private final String graphNamespace;

    @Nullable
    private final Path dumpFolder; // if supplied, each call to store creates a file there

    @Nullable
    private final RDFHandler dumpHandler; // if supplied, all RDF data is also stored here

    private final UpdateStrategy updateStrategy;

    private final InferenceEngine engine;

    private final Set<Statement> ontoModel;

    private final TraceConverter converter;

    private TraceStorer(final String traceNamespace, final String graphNamespace,
            @Nullable final Path dumpFolder, @Nullable final RDFHandler dumpHandler,
            final UpdateStrategy removeStrategy, final InferenceEngine engine,
            final Set<Statement> ontoModel, // final IRI ontoModelGraphIri,
            final TraceConverter converter) {

        this.traceNamespace = traceNamespace;
        this.graphNamespace = graphNamespace;
        this.dumpFolder = dumpFolder;
        this.dumpHandler = dumpHandler;
        this.updateStrategy = removeStrategy;
        this.engine = engine;
        this.ontoModel = ontoModel;
        this.converter = converter;
    }

    public static TraceStorer create(final Path ontologyFile,
            @Nullable final String traceNamespace, @Nullable final String graphNamespace,
            @Nullable final InferenceStrategy inferenceStrategy,
            @Nullable final UpdateStrategy updateStrategy, @Nullable final Path dumpFolder,
            @Nullable final Path dumpFile) {

        // Read ontology statements
        final long ts = System.currentTimeMillis();
        final List<Statement> ontologyStmts = Lists.newArrayList();
        RDFSources.read(false, true, null, null, null, true, ontologyFile.toString())
                .emit(new StatementCollector(ontologyStmts), 1);
        LOGGER.debug("Ontological model read from {} in {} ms: {} triples", ontologyFile,
                System.currentTimeMillis() - ts, ontologyStmts.size());

        // Delegate
        return create(ontologyStmts, traceNamespace, graphNamespace, inferenceStrategy,
                updateStrategy, dumpFolder, dumpFile);
    }

    public static TraceStorer create(final Iterable<Statement> ontology,
            @Nullable String traceNamespace, @Nullable String graphNamespace,
            @Nullable InferenceStrategy inferenceStrategy, @Nullable UpdateStrategy updateStrategy,
            @Nullable final Path dumpFolder, @Nullable final Path dumpFile) {

        // Normalize parameters and apply defaults
        traceNamespace = MoreObjects.firstNonNull(traceNamespace,
                Constants.DEFAULT_TRACE_NAMESPACE);
        graphNamespace = MoreObjects.firstNonNull(graphNamespace,
                Constants.DEFAULT_GRAPH_NAMESPACE);
        inferenceStrategy = MoreObjects.firstNonNull(inferenceStrategy,
                InferenceStrategy.LOCAL_GRAPHDB_RDFS);
        updateStrategy = MoreObjects.firstNonNull(updateStrategy, UpdateStrategy.APPEND);

        // Create dump folder, if needed
        if (dumpFolder != null) {
            try {
                Files.createDirectories(dumpFolder);
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        // Open RDF dump file, if specified
        RDFHandler dumpHandler = null;
        if (dumpFile != null) {
            try {
                final Path dumpFileParent = dumpFile.getParent();
                if (dumpFileParent != null) {
                    Files.createDirectories(dumpFileParent);
                }
                dumpHandler = RDFHandlers.write(null, 1, dumpFile.toString());
                dumpHandler.startRDF();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        // Prepare ontological model
        final Set<Statement> ontoModel = Sets.newHashSet(ontology);
        try (InferenceEngine modelEngine = inferenceStrategy.newModelEngine()) {

            // Compute axiomatic triples
            // long ts = System.currentTimeMillis();
            // final Set<Statement> axiomaticTriples = ImmutableSet
            // .copyOf(modelEngine.apply(ImmutableList.of()));
            // LOGGER.info("Axiomatic triples identified in {} ms: {} triples",
            // System.currentTimeMillis() - ts, axiomaticTriples.size());

            // Compute inferred ontological model
            final long ts = System.currentTimeMillis();
            final long ontoModelExplicitSize = ontoModel.size();
            for (final Statement stmt : modelEngine.apply(ontoModel)) {
                // if (!axiomaticTriples.contains(stmt)) {
                ontoModel.add(VF.createStatement(stmt.getSubject(), stmt.getPredicate(),
                        stmt.getObject()));
                // }
            }
            LOGGER.info("Ontological model prepared in {} ms: " + "{}/{}/{} triples exp/inf/all",
                    System.currentTimeMillis() - ts, ontoModelExplicitSize,
                    ontoModel.size() - ontoModelExplicitSize, ontoModel.size());
        }

        // Filter onto model for trace inference
        final long ts = System.currentTimeMillis();
        final Set<Statement> ontoModelTbox = extractTraceTbox(ontoModel, inferenceStrategy);
        LOGGER.info("Filtered ontological model prepared in {} ms: {} triples",
                System.currentTimeMillis() - ts, ontoModelTbox.size());

        // Allocate converter
        final TraceConverter converter = new TraceConverter(traceNamespace, ontoModel);

        // Dump ontological model if needed
        if (dumpFolder != null) {
            dump(dumpFolder.resolve("ontomodel.ttl.gz"), traceNamespace, ontoModel);
        }
        // if (dumpHandler != null) {
        // for (final Statement stmt : ontoModel) {
        // dumpHandler.handleStatement(VF.createStatement(stmt.getSubject(),
        // stmt.getPredicate(), stmt.getObject()));
        // }
        // }

        // Allocate inference engine, then build and return TraceStorer
        final InferenceEngine engine = inferenceStrategy.newTraceEngine(ontoModelTbox);
        try {
            return new TraceStorer(traceNamespace, graphNamespace, dumpFolder, dumpHandler,
                    updateStrategy, engine, ontoModel, converter);
        } catch (final Throwable ex) {
            engine.close();
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }
    }

    private static Set<Statement> extractTraceTbox(final Iterable<Statement> stmts,
            final InferenceStrategy strategy) {

        // Identify the namespaces of nodes to drop (BPMN + OWL, if using RDFS)
        final Set<String> droppedNs = Sets.newHashSet();
        droppedNs.add(BPMN.NAMESPACE);
        droppedNs.add("sys:"); // rdfpro ns
        if (strategy == InferenceStrategy.LOCAL_GRAPHDB_RDFS
                || strategy == InferenceStrategy.LOCAL_RDFPRO_RDFS) {
            droppedNs.add(OWL.NAMESPACE);
        }

        // Initialize the set of nodes to drop, including the ones matching namespaces plus any
        // BNode linked to them via (IRI, TBOX_PROP|TYPE, bnode) or (bnode, TBOX_PROP, IRI). Also
        // collect all bnode statements (bnode, *, bnode)
        final Set<Resource> droppedNodes = Sets.newHashSet();
        final List<Statement> bnodeStmts = Lists.newArrayList();
        for (final Statement stmt : stmts) {
            final Resource s = stmt.getSubject();
            final IRI p = stmt.getPredicate();
            final Value o = stmt.getObject();
            if (droppedNs.contains(p.getNamespace())) {
                droppedNodes.add(p);
            }
            if (s instanceof IRI && droppedNs.contains(((IRI) s).getNamespace())) {
                droppedNodes.add(s);
                if (o instanceof BNode && (Statements.TBOX_PROPERTIES.contains(p) //
                        || p.equals(RDF.TYPE))) {
                    droppedNodes.add((BNode) o);
                }
            }
            if (o instanceof IRI && droppedNs.contains(((IRI) o).getNamespace())) {
                droppedNodes.add((IRI) o);
                if (s instanceof BNode && Statements.TBOX_PROPERTIES.contains(p)) {
                    droppedNodes.add(s);
                }
            }
            if (s instanceof BNode && o instanceof BNode) {
                bnodeStmts.add(stmt);
            }
        }

        // Mark as nodes to drop also all BNodes that can be recursively reached starting from
        // other dropped nodes via (bnode, *, bnode) statements (mostly, RDF list statements)
        boolean changed = false;
        do {
            changed = false;
            for (final Iterator<Statement> i = bnodeStmts.iterator(); i.hasNext();) {
                final Statement stmt = i.next();
                final BNode s = (BNode) stmt.getSubject();
                final BNode o = (BNode) stmt.getObject();
                if (droppedNodes.contains(s)) {
                    droppedNodes.add(o);
                    i.remove();
                    changed = true;
                } else if (droppedNodes.contains(o)) {
                    droppedNodes.add(s);
                    i.remove();
                    changed = true;
                }
            }
        } while (changed);

        // Once the nodes to drop are known, filter the statements in one pass
        final Set<Statement> model = Sets.newHashSet();
        for (final Statement stmt : stmts) {
            final Resource s = stmt.getSubject();
            final IRI p = stmt.getPredicate();
            final Value o = stmt.getObject();
            if (!(Statements.TBOX_PROPERTIES.contains(p)
                    || p.equals(RDF.TYPE) && Statements.TBOX_CLASSES.contains(o))
                    || droppedNodes.contains(s) //
                    || droppedNodes.contains(p) //
                    || droppedNodes.contains(o) //
                    || p.equals(OWL.SAMEAS) && s.equals(o) //
                    || o.equals(RDFS.RESOURCE) //
                    || o.equals(RDFS.CLASS) //
                    || o.equals(RDF.LIST) //
                    || o.equals(OWL.THING)) {
                continue;
            }
            model.add(stmt);
        }
        return model;
    }

    public void clear(@Nullable final Repository repository) {

        // Remove data and log statistics
        final long ts = System.currentTimeMillis();
        long size = 0;
        if (repository != null) {
            try (RepositoryConnection conn = repository.getConnection()) {
                conn.begin();
                conn.prepareUpdate("DROP NAMED").execute();
                conn.prepareUpdate("DROP DEFAULT").execute();
                conn.commit();
                size = conn.size();
            }
        }
        final long endTs = System.currentTimeMillis();
        LOGGER.info("Triplestore cleared: {} ms, {} triples", endTs - ts, size);
    }

    public void init(@Nullable final Repository repository, final boolean schemaTx) {

        // Remove data and log statistics
        final long ts = System.currentTimeMillis();
        long size = 0;
        if (repository != null) {
            try (RepositoryConnection conn = repository.getConnection()) {
                conn.begin();
                conn.add(this.ontoModel);
                if (schemaTx) {
                    conn.add(VF.createStatement(VF.createBNode(),
                            VF.createIRI("http://www.ontotext.com/owlim/system#schemaTransaction"),
                            VF.createBNode()));
                }
                conn.commit();
                size = conn.size();
            }
        }
        final long endTs = System.currentTimeMillis();
        LOGGER.info("Triplestore initialized: {} ms, {} triples", endTs - ts, size);
    }

    public void remove(@Nullable final Repository repository, final Iterable<?> idOrIrisOrTraces) {

        // Check parameters
        Objects.requireNonNull(idOrIrisOrTraces);

        // Map id/iri/trace elements in the input to graph IRIs
        final Set<IRI> graphIris = Sets.newHashSet();
        for (final Object element : idOrIrisOrTraces) {
            if (element instanceof IRI) {
                graphIris.add(mintGraphIri((IRI) element));
            } else if (element instanceof String) {
                graphIris.add(mintGraphIri((String) element));
            } else if (element instanceof JsonObject) {
                graphIris.add(mintGraphIri(((JsonObject) element).get("trace_id").getAsString()));
            }
        }

        // Remove data and log statistics if possible
        final long ts = System.currentTimeMillis();
        if (repository != null) {
            this.updateStrategy.update(repository, Maps.asMap(graphIris, iri -> null));
        }
        final long endTs = System.currentTimeMillis();

        // Log statistics
        LOGGER.info("Removed {} traces: {} ms", graphIris.size(), endTs - ts);
    }

    public void warmup(final Repository repository, final int numThreads) {
        IntStream.rangeClosed(0, numThreads - 1).parallel().forEach(i -> {
            this.engine.apply(this.ontoModel);
            if (repository != null) {
                synchronized (repository) {
                    try (RepositoryConnection conn = repository.getConnection()) {
                        conn.begin();
                        conn.prepareUpdate("INSERT DATA {}").execute(); // nop
                        conn.commit();
                    }
                }
            }
        });
    }

    public long store(@Nullable final Repository repository, @Nullable final Listener listener,
            final Stream<JsonObject> traces, final int numThreads, final int batchSize,
            @Nullable final Long timeout, final boolean query) {

        // Check parameters
        Objects.requireNonNull(traces);
        Objects.requireNonNull(batchSize >= 1);

        // Wrap the listener to compute and periodically log aggregate statistics
        final AtomicReference<Statistics> statsHolder = new AtomicReference<>(Statistics.NIL);
        final AtomicLong tsLogHolder = new AtomicLong(System.currentTimeMillis());
        final Listener listenerWrapper = (repo, traceJson, stats, index) -> {
            final Statistics aggregateStats;
            synchronized (statsHolder) {
                aggregateStats = Statistics.aggregate(ImmutableList.of(statsHolder.get(), stats));
                statsHolder.set(aggregateStats);
            }
            final long ts = System.currentTimeMillis();
            if (ts - tsLogHolder.get() > 60000) {
                tsLogHolder.set(ts);
                LOGGER.info("Progress: {}", aggregateStats);
            }
            if (listener != null) {
                listener.onTracesPopulated(repo, traceJson, stats, index);
            }
        };

        // Iterate over traces using multiple threads, storing them in the triplestore
        final long endTime = timeout == null ? Long.MAX_VALUE
                : System.currentTimeMillis() + timeout;
        final AtomicBoolean stopped = new AtomicBoolean(false);
        final AtomicLong counter = new AtomicLong(0);
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try {
            final Iterator<JsonObject> iterator = traces.iterator();
            for (int i = 0; i < numThreads; ++i) {
                executor.submit(() -> {
                    final List<JsonObject> batch = Lists.newArrayList();
                    while (!Thread.interrupted() && !stopped.get()
                            && System.currentTimeMillis() <= endTime) {
                        try {
                            final long index;
                            batch.clear();
                            synchronized (iterator) {
                                index = counter.get();
                                while (iterator.hasNext() && batch.size() < batchSize) {
                                    final JsonObject trace = iterator.next();
                                    if (trace != null) {
                                        batch.add(trace);
                                        counter.incrementAndGet();
                                    }
                                }
                            }
                            if (batch.isEmpty()) {
                                break;
                            }
                            store(repository, listenerWrapper, batch, index, query);
                        } catch (final Throwable ex) {
                            LOGGER.error("Store failed for traces " + batch.stream()
                                    .map(t -> t.get("trace_id")).collect(Collectors.toList()), ex);
                        }
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ex) {
            // Ignore: will send interrupt and mark stopped flag later
        } finally {
            stopped.set(true);
            executor.shutdownNow();
        }

        // Log completion
        LOGGER.info("Completed: {}", statsHolder.get());
        return counter.get();
    }

    public void store(@Nullable final Repository repository, @Nullable final Listener listener,
            final Iterable<JsonObject> traces, final long index, final boolean query) {

        // Check parameters
        Objects.requireNonNull(traces);

        // Abort if there is nothing to do
        if (Iterables.isEmpty(traces)) {
            return;
        }

        // Take start timestamp and allocate time tracking vars
        long ts = System.currentTimeMillis();
        final long tsStart = ts;
        long timeConvert = 0L;
        long timeInfer = 0L;
        long timeStore = 0L;

        // Allocate triple counter vars
        long numExplicitStmts = 0L;
        long numInferredStmts = 0L;

        // Perform conversion and inference over traces
        final List<JsonObject> traceJsons = ImmutableList.copyOf(traces);
        final List<String> traceIds = Lists.newArrayList();
        final List<IRI> traceGraphIris = Lists.newArrayList();
        final Map<IRI, Set<Statement>> traceGraphs = Maps.newHashMap();
        for (final JsonObject traceJson : traceJsons) {

            // Convert trace into RDF
            final long tsConvert = ts;
            final String traceId = traceJson.get("trace_id").getAsString();
            final IRI graphIri = mintGraphIri(traceId);
            final List<Statement> explicitStmts = Lists.newArrayList();
            this.converter.convertTrace(new StatementCollector(explicitStmts), traceJson);

            // Perform inference and extract resulting trace graph
            final long tsInfer = System.currentTimeMillis();
            final Set<Statement> closedStmts = Sets
                    .newLinkedHashSetWithExpectedSize(explicitStmts.size() * 4);
            for (final Statement stmt : this.engine.apply(explicitStmts)) {
                if (this.ontoModel.contains(stmt)) {
                    continue;
                }
                closedStmts.add(stmt);
            }

            // Enqueue trace graph for addition in the repository
            traceIds.add(traceId);
            traceGraphIris.add(graphIri);
            traceGraphs.put(graphIri, closedStmts);

            // Update triple counts
            numExplicitStmts += explicitStmts.size();
            numInferredStmts += closedStmts.size() - explicitStmts.size();

            // Update times
            ts = System.currentTimeMillis();
            timeConvert += tsInfer - tsConvert;
            timeInfer += ts - tsInfer;
        }

        // Update traces in the remote repository (if supplied)
        if (repository != null) {
            this.updateStrategy.update(repository, traceGraphs);
        }

        // Query for data just inserted (one random statement per graph), if enabled
        if (query) {
            try (final RepositoryConnection conn = repository.getConnection()) {
                conn.begin();
                for (final Entry<IRI, ? extends Iterable<Statement>> e : traceGraphs.entrySet()) {
                    final IRI graphIri = e.getKey();
                    final Statement stmt = Iterables.getFirst(e.getValue(), null);
                    if (stmt != null) {
                        final boolean stored = conn.hasStatement(stmt.getSubject(),
                                stmt.getPredicate(), stmt.getObject(), true, graphIri);
                        if (!stored) {
                            LOGGER.warn("Could not retrieve data inserted in graph {}", graphIri);
                        }
                    }
                }
                conn.commit();
            }
        }

        // Dump to file inside the dump folder, if enabled
        if (this.dumpFolder != null) {
            for (int i = 0; i < traceIds.size(); ++i) {
                final String traceId = traceIds.get(i);
                final IRI traceGraphIri = traceGraphIris.get(i);
                final Set<Statement> traceGraph = traceGraphs.get(traceGraphIri);
                final JsonObject traceJson = traceJsons.get(i);
                final Path traceFile = this.dumpFolder.resolve(traceId + "_"
                        + traceJson.get("activities").getAsJsonArray().size() + ".ttl.gz");
                dump(traceFile, this.traceNamespace, traceGraph);
            }
        }

        // Dump to single RDF file, if enabled
        if (this.dumpHandler != null) {
            for (final Entry<IRI, Set<Statement>> e : traceGraphs.entrySet()) {
                final IRI traceGraphIri = e.getKey();
                for (final Statement stmt : e.getValue()) {
                    this.dumpHandler.handleStatement(VF.createStatement(stmt.getSubject(),
                            stmt.getPredicate(), stmt.getObject(), traceGraphIri));
                }
            }
        }

        // Compute and log statistics
        final long tsEnd = System.currentTimeMillis();
        timeStore += tsEnd - ts;
        final Statistics stats = Statistics.create(traceGraphs.size(), tsStart, tsEnd, timeConvert,
                timeInfer, timeStore, numExplicitStmts, numInferredStmts);
        LOGGER.info("Stored {} trace/es ({}): {}", traceGraphs.size(),
                traceGraphs.size() == 1 ? traceIds.get(0)
                        : traceIds.get(0) + ".." + traceIds.get(traceIds.size() - 1),
                stats);

        // Notify listener, if any
        if (listener != null) {
            try {
                listener.onTracesPopulated(repository, ImmutableList.copyOf(traceJsons), stats,
                        index);
            } catch (final Throwable ex) {
                LOGGER.error("Listener notification failed", ex);
            }
        }
    }

    @Override
    public void close() {

        // Deallocate the pool of repositories
        this.engine.close();

        // Close RDFHandler
        if (this.dumpHandler != null) {
            this.dumpHandler.endRDF();
            IO.closeQuietly(this.dumpHandler);
        }
    }

    private IRI mintGraphIri(final String traceId) {
        return mintGraphIri(this.converter.convertTraceId(traceId));
    }

    private IRI mintGraphIri(final IRI traceIri) {
        final String localName = traceIri.stringValue().substring(this.traceNamespace.length());
        return VF.createIRI(this.graphNamespace, localName);
    }

    private static void dump(final Path path, final String traceNamespace,
            final Iterable<Statement> stmts) {

        // Otherwise emit a gzipped TQL file with the name and statements supplied
        try (Writer out = IO.utf8Writer(IO.buffer(IO.write(path.toString())))) {
            final RDFWriter writer = PrettyTurtle.INSTANCE.getWriter(out);
            writer.startRDF();
            writer.handleNamespace(BPMN.PREFIX, BPMN.NAMESPACE);
            writer.handleNamespace(DOMAIN.PREFIX, DOMAIN.NAMESPACE);
            writer.handleNamespace(TRACE.PREFIX, TRACE.NAMESPACE);
            writer.handleNamespace(ABPMN.PREFIX, ABPMN.NAMESPACE);
            writer.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
            writer.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
            writer.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
            writer.handleNamespace(OWL.PREFIX, OWL.NAMESPACE);
            writer.handleNamespace("", traceNamespace);
            for (final Statement stmt : stmts) {
                writer.handleStatement(stmt);
            }
            writer.endRDF();
        } catch (final Throwable ex) {
            LOGGER.warn("Could not dump to " + path, ex);
        }
    }

    public static final class Statistics {

        public static final Statistics NIL = new Statistics(0, -1L, -1L, Stats.of(), Stats.of(),
                Stats.of(), Stats.of(), Stats.of(), Stats.of(), Stats.of());

        private final int numTraces;

        private final long tsStart;

        private final long tsEnd;

        private final Stats timeConvert;

        private final Stats timeInfer;

        private final Stats timeStore;

        private final Stats time;

        private final Stats triplesExplicit;

        private final Stats triplesInferred;

        private final Stats triples;

        private Statistics(final int numTraces, final long tsStart, final long tsEnd,
                final Stats timeConvert, final Stats timeInfer, final Stats timeStore,
                final Stats time, final Stats triplesExplicit, final Stats triplesInferred,
                final Stats triples) {

            this.numTraces = numTraces;
            this.tsStart = tsStart;
            this.tsEnd = tsEnd;
            this.timeConvert = timeConvert;
            this.timeInfer = timeInfer;
            this.timeStore = timeStore;
            this.time = time;
            this.triplesExplicit = triplesExplicit;
            this.triplesInferred = triplesInferred;
            this.triples = triples;
        }

        public int getNumTraces() {
            return this.numTraces;
        }

        public long getTsStart() {
            return this.tsStart;
        }

        public long getTsEnd() {
            return this.tsEnd;
        }

        public Stats getTimeConvert() {
            return this.timeConvert;
        }

        public Stats getTimeInfer() {
            return this.timeInfer;
        }

        public Stats getTimeStore() {
            return this.timeStore;
        }

        public Stats getTime() {
            return this.time;
        }

        public Stats getTriplesExplicit() {
            return this.triplesExplicit;
        }

        public Stats getTriplesInferred() {
            return this.triplesInferred;
        }

        public Stats getTriples() {
            return this.triples;
        }

        public double getThroughput() {
            // traces/min
            return this.numTraces == 0 ? Double.NaN
                    : this.numTraces * 60000.0 / (this.tsEnd - this.tsStart);
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Statistics)) {
                return false;
            }
            final Statistics other = (Statistics) object;
            return this.numTraces == other.numTraces //
                    && this.tsStart == other.tsStart //
                    && this.tsEnd == other.tsEnd //
                    && Objects.equals(this.timeConvert, other.timeConvert)
                    && Objects.equals(this.timeInfer, other.timeInfer)
                    && Objects.equals(this.timeStore, other.timeStore)
                    && Objects.equals(this.time, other.time)
                    && Objects.equals(this.triplesExplicit, other.triplesExplicit)
                    && Objects.equals(this.triplesInferred, other.triplesInferred)
                    && Objects.equals(this.triples, other.triples);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.numTraces, this.tsStart, this.tsEnd, this.timeConvert,
                    this.timeInfer, this.timeStore, this.time, this.triplesExplicit,
                    this.triplesInferred, this.triples);
        }

        @Override
        public String toString() {
            if (this.numTraces == 0) {
                return "-/-/- triples exp/inf/all, -/-/-/- ms con/inf/pop/all";
            }
            final StringBuilder builder = new StringBuilder();
            builder.append((long) this.triplesExplicit.mean()).append("/")
                    .append((long) this.triplesInferred.mean()).append("/")
                    .append((long) this.triples.mean()).append(" triples exp/inf/all, ");
            builder.append((long) this.timeConvert.mean()).append("/")
                    .append((long) this.timeInfer.mean()).append("/")
                    .append((long) this.timeStore.mean()).append("/")
                    .append((long) this.time.mean()).append(" ms con/inf/pop/all");
            if (this.numTraces > 1) {
                builder.append(", ").append(this.numTraces).append(" tr, ")
                        .append(String.format("%.3f", (this.tsEnd - this.tsStart) / 60000.0))
                        .append(" min, ").append(String.format("%.3f", getThroughput()))
                        .append(" tr/min");
            }
            return builder.toString();
        }

        public static Statistics aggregate(@Nullable final Iterable<Statistics> stats) {
            if (stats == null || Iterables.isEmpty(stats)) {
                return NIL;
            } else {
                int numTraces = 0;
                long tsStart = Long.MAX_VALUE;
                long tsEnd = Long.MIN_VALUE;
                final StatsAccumulator timeConvert = new StatsAccumulator();
                final StatsAccumulator timeInfer = new StatsAccumulator();
                final StatsAccumulator timeStore = new StatsAccumulator();
                final StatsAccumulator time = new StatsAccumulator();
                final StatsAccumulator triplesExplicit = new StatsAccumulator();
                final StatsAccumulator triplesInferred = new StatsAccumulator();
                final StatsAccumulator triples = new StatsAccumulator();
                for (final Statistics stat : stats) {
                    if (stat.numTraces > 0) {
                        numTraces += stat.numTraces;
                        tsStart = stat.tsStart < 0 ? tsStart : Math.min(tsStart, stat.tsStart);
                        tsEnd = stat.tsEnd < 0 ? tsEnd : Math.max(tsEnd, stat.tsEnd);
                        timeConvert.addAll(stat.timeConvert);
                        timeInfer.addAll(stat.timeInfer);
                        timeStore.addAll(stat.timeStore);
                        time.addAll(stat.time);
                        triplesExplicit.addAll(stat.triplesExplicit);
                        triplesInferred.addAll(stat.triplesInferred);
                        triples.addAll(stat.triples);
                    }
                }
                return new Statistics(numTraces, tsStart, tsEnd, timeConvert.snapshot(),
                        timeInfer.snapshot(), timeStore.snapshot(), time.snapshot(),
                        triplesExplicit.snapshot(), triplesInferred.snapshot(),
                        triples.snapshot());
            }
        }

        public static Statistics create(final int numTraces, final long tsStart, final long tsEnd,
                final long timeConvert, final long timeInfer, final long timeStore,
                final long triplesExplicit, final long triplesInferred) {

            if (numTraces == 0) {
                return NIL;
            }

            final long time = timeConvert + timeInfer + timeStore;
            final long triples = triplesExplicit + triplesInferred;

            if (numTraces == 1) {
                return new Statistics(1, tsStart, tsEnd, //
                        Stats.of(timeConvert), //
                        Stats.of(timeInfer), //
                        Stats.of(timeStore), //
                        Stats.of(time), //
                        Stats.of(triplesExplicit), //
                        Stats.of(triplesInferred), //
                        Stats.of(triples));
            } else {
                return new Statistics(numTraces, tsStart, tsEnd, //
                        statsFor(timeConvert, numTraces), //
                        statsFor(timeInfer, numTraces), //
                        statsFor(timeStore, numTraces), //
                        statsFor(time, numTraces), //
                        statsFor(triplesExplicit, numTraces), //
                        statsFor(triplesInferred, numTraces), //
                        statsFor(triples, numTraces));
            }
        }

        private static Stats statsFor(final double sum, final int count) {
            final double value = sum / count;
            final StatsAccumulator accumulator = new StatsAccumulator();
            for (int i = 0; i < count; ++i) {
                accumulator.add(value);
            }
            return accumulator.snapshot();
        }

        public static String tsvHeader(final boolean onlyMeans) {
            final StringBuilder sb = new StringBuilder();
            sb.append("count\ttsStart\ttsEnd");
            tsvHeaderHelper(sb, "timeConvert", onlyMeans);
            tsvHeaderHelper(sb, "timeInfer", onlyMeans);
            tsvHeaderHelper(sb, "timeStore", onlyMeans);
            tsvHeaderHelper(sb, "time", onlyMeans);
            tsvHeaderHelper(sb, "triplesExplicit", onlyMeans);
            tsvHeaderHelper(sb, "triplesInferred", onlyMeans);
            tsvHeaderHelper(sb, "triples", onlyMeans);
            return sb.toString();
        }

        public static String tsvLine(final Statistics stats, final boolean onlyMeans) {
            final StringBuilder sb = new StringBuilder();
            sb.append(stats.numTraces).append("\t").append(stats.tsStart).append("\t")
                    .append(stats.tsEnd);
            tsvLineHelper(sb, stats.timeConvert, onlyMeans);
            tsvLineHelper(sb, stats.timeInfer, onlyMeans);
            tsvLineHelper(sb, stats.timeStore, onlyMeans);
            tsvLineHelper(sb, stats.time, onlyMeans);
            tsvLineHelper(sb, stats.triplesExplicit, onlyMeans);
            tsvLineHelper(sb, stats.triplesInferred, onlyMeans);
            tsvLineHelper(sb, stats.triples, onlyMeans);
            return sb.toString();
        }

        private static void tsvHeaderHelper(final StringBuilder sb, final String name,
                final boolean onlyMeans) {
            sb.append("\t").append(name);
            if (!onlyMeans) {
                sb.append("\t").append(name).append("Min");
                sb.append("\t").append(name).append("Max");
                sb.append("\t").append(name).append("Std");
            }
        }

        private static void tsvLineHelper(final StringBuilder sb, final Stats stats,
                final boolean onlyMeans) {
            sb.append("\t").append((long) stats.mean());
            if (!onlyMeans) {
                sb.append("\t").append((long) stats.min());
                sb.append("\t").append((long) stats.max());
                sb.append("\t").append(String.format("%.2f", stats.sampleStandardDeviation()));
            }
        }

    }

    @FunctionalInterface
    public interface Listener {

        final Listener NIL = (repository, traceJson, statistics, index) -> {
        };

        void onTracesPopulated(@Nullable Repository repository, List<JsonObject> traceJsons,
                Statistics statistics, long index) throws Throwable;

        static Listener aggregate(final Iterable<Listener> listeners) {
            final Listener[] array = Iterables.toArray(listeners, Listener.class);
            if (array.length == 0) {
                return NIL;
            } else if (array.length == 1) {
                return array[1];
            } else {
                return (repository, traceJson, statistics, index) -> {
                    Throwable exception = null;
                    for (final Listener listener : array) {
                        try {
                            listener.onTracesPopulated(repository, traceJson, statistics, index);
                        } catch (final Throwable ex) {
                            if (exception == null) {
                                exception = ex;
                            } else {
                                exception.addSuppressed(ex);
                            }
                        }
                    }
                    if (exception != null) {
                        Throwables.throwIfUnchecked(exception);
                        throw new RuntimeException(exception);
                    }
                };
            }
        }
    }

    public enum UpdateStrategy {

        APPEND {

            @Override
            public void update(final Repository repository,
                    final Map<IRI, ? extends Iterable<Statement>> graphs) {

                if (graphs.isEmpty()) {
                    return;
                }

                try (final RepositoryConnection conn = repository.getConnection()) {
                    conn.begin();
                    for (final Entry<IRI, ? extends Iterable<Statement>> e : graphs.entrySet()) {
                        final IRI graphIri = e.getKey();
                        final Iterable<Statement> graphStmts = e.getValue();
                        if (graphStmts == null) {
                            conn.clear(graphIri);
                        } else {
                            conn.add(graphStmts, graphIri);
                        }
                    }
                    conn.commit();
                }
            }

        },

        APPEND_DEFAULT_GRAPH {

            @Override
            public void update(final Repository repository,
                    final Map<IRI, ? extends Iterable<Statement>> graphs) {

                if (graphs.isEmpty()) {
                    return;
                }

                try (final RepositoryConnection conn = repository.getConnection()) {
                    conn.begin();
                    for (final Iterable<Statement> graphStmts : graphs.values()) {
                        if (graphStmts == null) {
                            throw new UnsupportedOperationException(
                                    "Delete unsupported in strategy " + name());
                        } else {
                            conn.add(graphStmts);
                        }
                    }
                    conn.commit();
                }
            }

        },

        REPLACE_RDF4J_CLEAR {

            @Override
            public void update(final Repository repository,
                    final Map<IRI, ? extends Iterable<Statement>> graphs) {

                if (graphs.isEmpty()) {
                    return;
                }

                try (final RepositoryConnection conn = repository.getConnection()) {
                    conn.begin();
                    conn.clear(Iterables.toArray(graphs.keySet(), Resource.class));
                    for (final Entry<IRI, ? extends Iterable<Statement>> e : graphs.entrySet()) {
                        if (e.getValue() != null) {
                            conn.add(e.getValue(), e.getKey());
                        }
                    }
                    conn.commit();
                }
            }

        },

        REPLACE_RDF4J_REMOVE {

            @Override
            public void update(final Repository repository,
                    final Map<IRI, ? extends Iterable<Statement>> graphs) {

                if (graphs.isEmpty()) {
                    return;
                }

                try (final RepositoryConnection conn = repository.getConnection()) {
                    conn.begin();
                    conn.remove((Resource) null, (IRI) null, (Value) null,
                            Iterables.toArray(graphs.keySet(), Resource.class));
                    for (final Entry<IRI, ? extends Iterable<Statement>> e : graphs.entrySet()) {
                        if (e.getValue() != null) {
                            conn.add(e.getValue(), e.getKey());
                        }
                    }
                    conn.commit();
                }
            }

        },

        REPLACE_SPARQL_UPDATE {

            @Override
            public void update(final Repository repository,
                    final Map<IRI, ? extends Iterable<Statement>> graphs) {

                if (graphs.isEmpty()) {
                    return;
                }

                try (final RepositoryConnection conn = repository.getConnection()) {
                    conn.begin();
                    for (final Entry<IRI, ? extends Iterable<Statement>> e : graphs.entrySet()) {
                        final Update update = conn.prepareUpdate(QueryLanguage.SPARQL,
                                "DROP SILENT GRAPH <" + e.getKey().stringValue() + ">");
                        update.execute();
                        if (e.getValue() != null) {
                            conn.add(e.getValue(), e.getKey());
                        }
                    }
                    conn.commit();
                }
            }

        },

        REPLACE_GRAPH_PROTOCOL {

            @Override
            public void update(final Repository repository,
                    final Map<IRI, ? extends Iterable<Statement>> graphs) {

                final HTTPRepository httpRepository = (HTTPRepository) repository;
                final HttpClient httpClient = httpRepository.getHttpClient();

                for (final Entry<IRI, ? extends Iterable<Statement>> e : graphs.entrySet()) {

                    final IRI traceGraphIri = e.getKey();
                    final Iterable<Statement> traceGraph = MoreObjects.firstNonNull(e.getValue(),
                            ImmutableSet.of());

                    final RDFFormat format = RDFFormat.BINARY;
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    Rio.write(traceGraph, baos, format);

                    final HttpEntity entity = new ByteArrayEntity(baos.toByteArray());
                    final HttpPut put = new HttpPut(httpRepository.getRepositoryURL()
                            + "/rdf-graphs/service?graph=" + traceGraphIri.stringValue());
                    put.setHeader("Content-Type", format.getDefaultMIMEType());
                    put.setEntity(entity);

                    try {
                        httpClient.execute(put);
                    } catch (final Throwable ex) {
                        Throwables.throwIfUnchecked(ex);
                        throw new RuntimeException(ex);
                    }
                }
            }

        };

        public abstract void update(final Repository repository,
                final Map<IRI, ? extends Iterable<Statement>> graphs);

    }

    public enum InferenceStrategy {

        CENTRAL {

            @Override
            public InferenceEngine newModelEngine() {
                return InferenceEngine.newIdentityEngine();
            }

            @Override
            public InferenceEngine newTraceEngine(final Iterable<Statement> model) {
                return InferenceEngine.newIdentityEngine();
            }

        },

        LOCAL_GRAPHDB_RDFS {

            @Override
            public InferenceEngine newModelEngine() {
                return InferenceEngine.newGraphDBEngine(null, "rdfs-optimized", false);
            }

            @Override
            public InferenceEngine newTraceEngine(final Iterable<Statement> model) {
                return InferenceEngine.newGraphDBEngine(model, "rdfs-optimized", true);
            }

        },

        LOCAL_GRAPHDB_OWL2 {

            @Override
            public InferenceEngine newModelEngine() {
                return InferenceEngine.newGraphDBEngine(null, "owl2-rl-optimized", false);
            }

            @Override
            public InferenceEngine newTraceEngine(final Iterable<Statement> model) {
                return InferenceEngine.newGraphDBEngine(model, "owl2-rl-optimized", true);
            }

        },

        LOCAL_RDFPRO_RDFS {

            @Override
            public InferenceEngine newModelEngine() {
                return InferenceEngine.newRDFproEngine(null, Ruleset.OWL2RL);
            }

            @Override
            public InferenceEngine newTraceEngine(final Iterable<Statement> model) {
                return InferenceEngine.newRDFproEngine(model, Ruleset.RHODF);
            }

        },

        LOCAL_RDFPRO_OWL2 {

            @Override
            public InferenceEngine newModelEngine() {
                return InferenceEngine.newRDFproEngine(null, Ruleset.OWL2RL);
            }

            @Override
            public InferenceEngine newTraceEngine(final Iterable<Statement> model) {
                return InferenceEngine.newRDFproEngine(model, Ruleset.OWL2RL);
            }

        };

        public abstract InferenceEngine newModelEngine();

        public abstract InferenceEngine newTraceEngine(final Iterable<Statement> model);

    }

    public static Stream<JsonObject> load(final Path path, final int offset, final int limit,
            final boolean partialTraces) {

        // Check parameters
        Objects.requireNonNull(path);
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkArgument(limit >= 0);

        try {
            // Handle different path types
            Stream<JsonObject> str;
            if (Files.isDirectory(path)) {
                // A directory: return a stream that iterates over child directories
                final List<Path> dirs = Files.list(path).collect(Collectors.toList());
                Collections.sort(dirs);
                Collections.shuffle(dirs, new Random(0L));
                str = dirs.stream().skip(offset).limit(limit).map(d -> {
                    final Path f = d.resolve("generated_trace_" + d.getFileName() + ".trace");
                    try (Reader reader = Files.newBufferedReader(f)) {
                        final JsonObject js = GSON.fromJson(reader, JsonObject.class);
                        js.addProperty("trace_process", "Nascita-Astratto3");
                        js.addProperty("trace_id",
                                "Comune2@nomeComuneTest2@protocolloComuneTest_test"
                                        + d.getFileName());
                        return js;
                    } catch (final Throwable ex) {
                        LOGGER.warn("Could not read and parse " + f, ex);
                        return null;
                    }
                });

            } else if ((path + ".").contains(".jsonl.")) {
                // Jsonl file: return a stream that scans the lines in the file
                final BufferedReader reader = new BufferedReader(
                        IO.utf8Reader(IO.buffer(IO.read(path.toString()))));
                str = reader.lines().skip(offset).limit(limit).map(l -> {
                    try {
                        return GSON.fromJson(l, JsonObject.class);
                    } catch (final Throwable ex) {
                        LOGGER.warn("Could not parse: " + l, ex);
                        return null;
                    }
                }).onClose(() -> {
                    IO.closeQuietly(reader);
                });

            } else if ((path + ".").contains(".json.")) {
                // Json file: read it and return a singleton stream
                try (Reader reader = Files.newBufferedReader(path)) {
                    final JsonObject js = GSON.fromJson(reader, JsonObject.class);
                    js.addProperty("trace_process", "Nascita-Astratto3");
                    js.addProperty("trace_id", "Comune2@nomeComuneTest2@protocolloComuneTest_test"
                            + path.getFileName());
                    str = ImmutableList.of(js).stream();
                }

            } else {
                // Unsupported
                throw new IllegalArgumentException("Unsupported path " + path);
            }

            // Explode into partial traces, if required
            if (partialTraces) {
                final Iterator<List<JsonObject>> i = Iterators.partition(str.iterator(),
                        Constants.PARTIAL_TRACES_BATCH);
                final Spliterator<List<JsonObject>> s = Spliterators.spliteratorUnknownSize(i,
                        Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL);
                str = StreamSupport.stream(s, false)
                        .flatMap(b -> Traces.computePartialTraces(b).stream());
            }

            // Return the requested stream
            return str;

        } catch (final IOException ex) {
            // Wrap and propagate
            throw new RuntimeException(ex);
        }
    }

    public static void main(final String[] args) {

        // Define class-specific defaults
        final InferenceStrategy defaultInferenceStrategy = InferenceStrategy.LOCAL_RDFPRO_OWL2;
        final UpdateStrategy defaultUpdateStrategy = UpdateStrategy.APPEND;

        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser()
                    .withName("java -cp ... eu.fbk.pdi.promo.tracegenerator.TraceStorer")
                    .withOption("u", "url", "the repository URL", "URL", CommandLine.Type.STRING,
                            true, false, false)
                    .withOption("i", "inference",
                            "the inference STRATEGY, values: "
                                    + Joiner.on(' ').join(InferenceStrategy.values())
                                    + "(default: " + defaultInferenceStrategy + ")",
                            "STRATEGY", CommandLine.Type.STRING, true, false, false)
                    .withOption("U", "update",
                            "the update STRATEGY, values: "
                                    + Joiner.on(' ').join(UpdateStrategy.values()) + "(default: "
                                    + defaultUpdateStrategy + ")",
                            "STRATEGY", CommandLine.Type.STRING, true, false, false)
                    .withOption("q", "query",
                            "whether to query for data just inserted to check it has been stored (default: false)")
                    .withOption("p", "partial", "whether to simulate insertion of partial traces")
                    .withOption("t", "threads",
                            "the NUMBER of threads to use in the test (default: "
                                    + Constants.DEFAULT_STORE_THREADS + ")",
                            "NUMBER", CommandLine.Type.INTEGER, true, false, false)
                    .withOption("s", "skip-traces",
                            "the NUMBER of traces to skip, if any (default: none)", "NUMBER",
                            CommandLine.Type.NON_NEGATIVE_INTEGER, true, false, false)
                    .withOption("m", "max-traces",
                            "the maximum NUMBER of traces to store, if any (default: none)",
                            "NUMBER", CommandLine.Type.NON_NEGATIVE_INTEGER, true, false, false)
                    .withOption("b", "batch-size",
                            "the NUMBER of traces to store in a single operation (default: 1)",
                            "NUMBER", CommandLine.Type.POSITIVE_INTEGER, true, false, false)
                    .withOption("D", "delete",
                            "whether to delete the repository before populating (default: false)")
                    .withOption("O", "init-ontology",
                            "whether to upload the ontology to the repository before populating (default: false)")
                    .withOption(null, "schema-tx",
                            "whether to upload the ontology using a schema transaction")
                    .withOption("d", "data",
                            "the input FILE with the traces to store (default: "
                                    + Constants.DEFAULT_TRACES_FILE + ")",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("r", "result",
                            "the FILE where to store resulting statistics, if any", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption("o", "ontology",
                            "the ontology FILE (default: " + Constants.DEFAULT_ONTOLOGY_FILE + ")",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption(null, "dump-folder",
                            "the FOLDER where to emit generated RDF triples, if any, split into files (default: none)",
                            "FOLDER", CommandLine.Type.DIRECTORY, true, false, false)
                    .withOption(null, "dump-file",
                            "the FILE where to emit generated RDF quads, if any (default: none)",
                            "FILE", CommandLine.Type.FILE, true, false, false)
                    .withOption(null, "trace-namespace",
                            "the URI to use as namespace for populated trace data (default: "
                                    + Constants.DEFAULT_TRACE_NAMESPACE + ")",
                            "URI", CommandLine.Type.STRING, true, false, false)
                    .withOption(null, "graph-namespace",
                            "the URI to use as namespace for populated named graphs (default: "
                                    + Constants.DEFAULT_GRAPH_NAMESPACE + ")",
                            "URI", CommandLine.Type.STRING, true, false, false)
                    .withHeader(
                            "Populates the central triplestore with trace data, using multiple threads")
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read command line options
            final String repositoryUrl = cmd.getOptionValue("u", String.class);
            final InferenceStrategy inferenceStrategy = cmd.getOptionValue("i",
                    InferenceStrategy.class, defaultInferenceStrategy);
            final UpdateStrategy updateStrategy = cmd.getOptionValue("U", UpdateStrategy.class,
                    defaultUpdateStrategy);
            final boolean partialTraces = cmd.hasOption("p");
            final int numThreads = cmd.getOptionValue("t", Integer.class,
                    Constants.DEFAULT_STORE_THREADS);
            final int skipTraces = cmd.getOptionValue("s", Integer.class, 0);
            final int maxTraces = cmd.getOptionValue("m", Integer.class, Integer.MAX_VALUE);
            final int batchSize = cmd.getOptionValue("b", Integer.class, 1);
            final boolean query = cmd.hasOption("q");
            final boolean delete = cmd.hasOption("D");
            final boolean storeOntology = cmd.hasOption("O");
            final boolean storeOntologySchemaTx = cmd.hasOption("schema-tx");
            final Path tracesFile = cmd.getOptionValue("d", Path.class,
                    Constants.DEFAULT_TRACES_FILE);
            final Path ontologyFile = cmd.getOptionValue("o", Path.class,
                    Constants.DEFAULT_ONTOLOGY_FILE);
            final Path resultFile = cmd.getOptionValue("r", Path.class);
            final Path dumpFolder = cmd.getOptionValue("dump-folder", Path.class);
            final Path dumpFile = cmd.getOptionValue("dump-file", Path.class);
            final String traceNamespace = cmd.getOptionValue("trace-namespace", String.class,
                    Constants.DEFAULT_TRACE_NAMESPACE);
            final String graphNamespace = cmd.getOptionValue("graph-namespace", String.class,
                    Constants.DEFAULT_TRACE_NAMESPACE);

            // Declare resources to release
            HTTPRepository repository = null;
            Writer tsvWriter = null;
            Stream<JsonObject> traces = null;
            TraceStorer storer = null;

            try {
                // Connect to central triplestore if possible
                if (repositoryUrl != null) {
                    repository = new HTTPRepository(repositoryUrl);
                    repository.init();
                }

                // Open output TSV file and allocate listener to record statistics
                Listener listener = null;
                if (resultFile != null) {
                    try {
                        tsvWriter = IO.utf8Writer(IO.buffer(IO.write(resultFile.toString())));
                        tsvWriter.write("index\tids\t");
                        tsvWriter.write(Statistics.tsvHeader(true));
                        tsvWriter.write("\n");
                    } catch (final IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    final Writer w = tsvWriter;
                    listener = (repo, traceJsons, stats, index) -> {
                        final List<String> ids = traceJsons.stream()
                                .map(t -> t.get("trace_id").getAsString())
                                .collect(Collectors.toList());
                        w.write(Long.toString(index));
                        w.write("\t");
                        w.write(Joiner.on(',').join(ids));
                        w.write("\t");
                        w.write(Statistics.tsvLine(stats, true));
                        w.write("\n");
                    };
                }

                // Load traces
                traces = load(tracesFile, skipTraces, maxTraces, partialTraces);

                // Allocate traces storer
                storer = TraceStorer.create(ontologyFile, traceNamespace, graphNamespace,
                        inferenceStrategy, updateStrategy, dumpFolder, dumpFile);

                // Clear remote triplestore and store again ontological model, if requested
                if (delete) {
                    storer.clear(repository);
                }
                if (storeOntology) {
                    storer.init(repository, storeOntologySchemaTx);
                }

                // Perform warmup
                storer.warmup(repository, numThreads);

                // Perform population
                LOGGER.info("Starting population");
                storer.store(repository, listener, traces, numThreads, batchSize, null, query);

            } finally {
                // Release resources
                IO.closeQuietly(storer);
                IO.closeQuietly(traces);
                IO.closeQuietly(tsvWriter);
                if (repository != null && repository.isInitialized()) {
                    repository.shutDown();
                }
            }

        } catch (final Throwable ex) {
            // Handle failure
            CommandLine.fail(ex);
        }
    }

}
