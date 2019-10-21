package eu.fbk.pdi.promo.evaluation;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;

import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.pdi.promo.triplestore.TraceStorer;
import eu.fbk.pdi.promo.triplestore.TraceStorer.InferenceStrategy;
import eu.fbk.pdi.promo.triplestore.TraceStorer.Listener;
import eu.fbk.pdi.promo.triplestore.TraceStorer.Statistics;
import eu.fbk.pdi.promo.triplestore.TraceStorer.UpdateStrategy;
import eu.fbk.pdi.promo.util.Constants;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.utils.core.CommandLine;

public final class StoreEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreEvaluator.class);

    public static void main(final String[] args) throws Throwable {

        // Define class-specific defaults
        final InferenceStrategy defaultInferenceStrategy = InferenceStrategy.LOCAL_GRAPHDB_OWL2;
        final UpdateStrategy defaultUpdateStrategy = UpdateStrategy.REPLACE_GRAPH_PROTOCOL;

        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser()
                    .withOption("u", "url", "the repository URL", "URL", CommandLine.Type.STRING,
                            true, false, true)
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
                            "whether to query for data just inserted, to ensure it has been stored (default: false)")
                    .withOption("p", "partial",
                            "whether to simulate insertion of partial traces (default: false)")
                    .withOption("D", "delete",
                            "whether to delete inserted data after (and before, to be sure) the test (default: false)")
                    .withOption("t", "threads",
                            "the NUMBER of threads to use in the test (default: "
                                    + Constants.DEFAULT_STORE_THREADS + ")",
                            "NUMBER", CommandLine.Type.INTEGER, true, false, false)
                    .withOption("T", "timeout",
                            "the maximum time DURATION (approx) for the test (e.g., PT5M)",
                            "DURATION", CommandLine.Type.STRING, true, false, false)
                    .withOption("w", "warmup-samples",
                            "the NUMBER of warmup samples to discard when computing statistics (default: 0)",
                            "NUMBER", CommandLine.Type.INTEGER, true, false, false)
                    .withOption("d", "data",
                            "the input FILE with the traces to store (default: "
                                    + Constants.DEFAULT_TRACES_FILE + ")",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("r", "result",
                            "the FOLDER where to emit files with resulting statistics", "FILE",
                            CommandLine.Type.DIRECTORY, true, false, false)
                    .withOption("o", "ontology",
                            "the ontology FILE (default: " + Constants.DEFAULT_ONTOLOGY_FILE + ")",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption(null, "trace-namespace",
                            "the URI to use as namespace for populated trace data (default: "
                                    + Constants.DEFAULT_TRACE_NAMESPACE + ")",
                            "URI", CommandLine.Type.STRING, true, false, false)
                    .withOption(null, "graph-namespace",
                            "the URI to use as namespace for populated named graphs (default: "
                                    + Constants.DEFAULT_GRAPH_NAMESPACE + ")",
                            "URI", CommandLine.Type.STRING, true, false, false)
                    .withHeader("Evaluates population of PROMO central triplestore") //
                    .withLogger(LoggerFactory.getLogger("eu.fbk.pdi.promo")) //
                    .parse(args);

            // Read command line options
            final String repositoryUrl = cmd.getOptionValue("u", String.class);
            final Path tracesFile = cmd.getOptionValue("d", Path.class,
                    Constants.DEFAULT_TRACES_FILE);
            final InferenceStrategy inferenceStrategy = cmd.getOptionValue("i",
                    InferenceStrategy.class, defaultInferenceStrategy);
            final UpdateStrategy updateStrategy = cmd.getOptionValue("U", UpdateStrategy.class,
                    defaultUpdateStrategy);
            final boolean query = cmd.hasOption("q");
            final boolean partialTraces = cmd.hasOption("p");
            final boolean delete = cmd.hasOption("p"); // implied by partialTraces
            final int numThreads = cmd.getOptionValue("t", Integer.class,
                    Constants.DEFAULT_STORE_THREADS);
            final int numWarmupSamples = cmd.getOptionValue("w", Integer.class, 0);
            final Long timeout = cmd.hasOption("T")
                    ? Duration.parse(cmd.getOptionValue("T", String.class)).toMillis()
                    : null;
            final Path resultDir = cmd.getOptionValue("r", Path.class);
            final Path ontologyFile = cmd.getOptionValue("o", Path.class,
                    Constants.DEFAULT_ONTOLOGY_FILE);
            final String traceNamespace = cmd.getOptionValue("trace-namespace", String.class,
                    Constants.DEFAULT_TRACE_NAMESPACE);
            final String graphNamespace = cmd.getOptionValue("graph-namespace", String.class,
                    Constants.DEFAULT_TRACE_NAMESPACE);

            // Read traces
            final List<JsonObject> traces = TraceStorer
                    .load(tracesFile, 0, Integer.MAX_VALUE, partialTraces)
                    .collect(Collectors.toList());

            // Create TraceStorer
            final TraceStorer storer = TraceStorer.create(ontologyFile, traceNamespace,
                    graphNamespace, inferenceStrategy, updateStrategy, null, null);

            // Delegate, deallocating storer when done
            try {
                evaluate(storer, repositoryUrl, traces, numThreads, numWarmupSamples, timeout,
                        resultDir, delete, query);
            } finally {
                IO.closeQuietly(storer);
            }

        } catch (final Throwable ex) {
            // Report failure
            CommandLine.fail(ex);
        }
    }

    public static void evaluate(final TraceStorer storer, final String repositoryUrl,
            final Iterable<JsonObject> traces, final int numThreads, int numWarmupSamples,
            @Nullable final Long timeout, @Nullable final Path resultDir, boolean delete,
            boolean query) throws Throwable {

        // Take timestamp for total elapsed time computation
        long ts = System.currentTimeMillis();

        // Create directory for report files, if any
        if (resultDir != null) {
            Files.createDirectories(resultDir);
        }

        // Extract trace IDs (note: for partial test more traces have the same ID)
        int numTraces = Iterables.size(traces);
        final Set<String> traceIds = Sets.newHashSet();
        for (JsonObject trace : traces) {
            traceIds.add(trace.get("trace_id").getAsString());
        }

        // Compute the indexes of samples to aggregate
        final int indexFrom = numThreads == 1 ? numWarmupSamples : numWarmupSamples / 2;
        final int indexTo = numThreads == 1 ? numTraces
                : numTraces - (numWarmupSamples - indexFrom);

        // Resources to deallocate (if initialized)
        Writer traceWriter = null;
        Writer avgWriter = null;
        HTTPRepository repository = null;

        try {
            // Setup trace writers
            if (resultDir != null) {
                LOGGER.info("Writing statistics to {}", resultDir);
                traceWriter = IO.utf8Writer(
                        IO.buffer(IO.write(resultDir.resolve("traces.tsv").toString())));
                traceWriter.write("index\tids\t");
                traceWriter.write(Statistics.tsvHeader(true));
                traceWriter.write("\n");

                avgWriter = IO
                        .utf8Writer(IO.buffer(IO.write(resultDir.resolve("avg.tsv").toString())));
                avgWriter.write("countRaw\tindexFrom\tindexTo\t");
                avgWriter.write(Statistics.tsvHeader(false));
                avgWriter.write("\n");
            }

            // Setup report writer, if needed
            final AtomicReference<Statistics> statistics = new AtomicReference<>(Statistics.NIL);
            final AtomicReference<Writer> traceWriterRef = new AtomicReference<>(traceWriter);
            final Listener listener = (repo, traceJsons, stats, index) -> {
                if (index >= indexFrom && index < indexTo) {
                    synchronized (statistics) {
                        final Statistics statsOld = statistics.get();
                        final Statistics statsNew = Statistics
                                .aggregate(ImmutableList.of(statsOld, stats));
                        statistics.set(statsNew);
                    }
                }
                if (traceWriterRef.get() != null) {
                    final Writer w = traceWriterRef.get();
                    synchronized (w) {
                        final List<String> ids = traceJsons.stream()
                                .map(t -> t.get("trace_id").getAsString())
                                .collect(Collectors.toList());
                        w.write(Long.toString(index));
                        w.write("\t");
                        w.write(Joiner.on(',').join(ids));
                        w.write("\t");
                        w.write(Statistics.tsvLine(stats, true));
                        w.write("\n");
                    }
                }
            };

            // Connect to repository
            repository = new HTTPRepository(repositoryUrl);
            repository.init();
            LOGGER.info("Connected to {}", repositoryUrl);

            // Make sure that the traces to insert are not in the repository
            if (delete) {
                LOGGER.info("Removing test traces...");
                storer.remove(repository, traceIds);
            }

            // Perform warmup
            LOGGER.info("Warming up...");
            storer.warmup(repository, numThreads);

            // Perform test
            LOGGER.info("Store evaluation started: {} traces, {} threads, {} timeout", numTraces,
                    numThreads, timeout == null ? "no" : timeout);
            long tsStore = System.currentTimeMillis();
            long actualNumTraces = storer.store(repository, listener,
                    ImmutableList.copyOf(traces).stream(), numThreads, 1, timeout, query);
            avgWriter.write(Long.toString(numTraces));
            avgWriter.write("\t");
            avgWriter.write(Long.toString(indexFrom));
            avgWriter.write("\t");
            avgWriter.write(Long.toString(indexTo));
            avgWriter.write("\t");
            avgWriter.write(Statistics.tsvLine(statistics.get(), false));
            avgWriter.write("\n");
            LOGGER.info("Store evaluation completed: {} traces, {} ms", actualNumTraces,
                    System.currentTimeMillis() - tsStore);

            // Delete inserted traces
            if (delete) {
                LOGGER.info("Removing test traces...");
                storer.remove(repository, traceIds);
            }

        } finally {
            // Release resources
            IO.closeQuietly(traceWriter);
            IO.closeQuietly(avgWriter);
            if (repository != null) {
                repository.shutDown();
            }
        }

        // Signal completion
        LOGGER.info("Test completed in {} ms", System.currentTimeMillis() - ts);
    }

}
