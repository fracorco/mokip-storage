package eu.fbk.pdi.promo.evaluation;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.IteratingGraphQueryResult;
import org.eclipse.rdf4j.query.impl.IteratingTupleQueryResult;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.utils.core.CommandLine;

public final class QueryEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEvaluator.class);

    public static void main(final String[] args) throws Throwable {
        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser()
                    .withOption("u", "url", "the repository URL", "URL", CommandLine.Type.STRING,
                            true, false, true)
                    .withOption("q", "queries", "the FOLDER with the queries to test", "FOLDER",
                            CommandLine.Type.DIRECTORY_EXISTING, true, false, true)
                    .withOption("r", "result", "the FOLDER where to store resulting statistics",
                            "FOLDER", CommandLine.Type.DIRECTORY, true, false, false)
                    .withOption("v", "verify", "the FOLDER with the expected query results",
                            "FOLDER", CommandLine.Type.DIRECTORY_EXISTING, true, false, false)
                    .withOption("w", "warmup", "whether to perform warmup")
                    .withHeader("Evaluates querying of PROMO central triplestore") //
                    .withLogger(LoggerFactory.getLogger("eu.fbk.pdi.promo")) //
                    .parse(args);

            // Read command line options
            final String repositoryUrl = cmd.getOptionValue("u", String.class);
            final Path queryDir = cmd.getOptionValue("q", Path.class);
            final Path resultDir = cmd.getOptionValue("r", Path.class);
            final Path verifyDir = cmd.getOptionValue("v", Path.class);
            final boolean warmup = cmd.hasOption("w");

            // Delegate
            evaluate(repositoryUrl, queryDir, resultDir, verifyDir, warmup);

        } catch (final Throwable ex) {
            // Report failure
            CommandLine.fail(ex);
        }
    }

    public static void evaluate(final String repositoryUrl, final Path queryDir,
            final Path resultDir, @Nullable final Path verifyDir, final boolean warmup)
            throws Throwable {

        // Check parameters
        Objects.requireNonNull(repositoryUrl);
        Objects.requireNonNull(queryDir);
        Objects.requireNonNull(resultDir);

        // Keep track of start time
        final long start = System.currentTimeMillis();

        // Load, validate, and filter queries
        final Map<String, String> queries = loadQueries(queryDir);
        LOGGER.info("Loaded {} query/ies: {}", queries.size(),
                Joiner.on(", ").join(queries.keySet()));

        // Connect to remote repository
        final Repository repository = new SPARQLRepository(repositoryUrl);
        repository.init();
        LOGGER.info("Connected to repository {}", repositoryUrl);

        // Operate using a single repository connection
        try (RepositoryConnection connection = repository.getConnection()) {

            // Compute and create result directory
            Files.createDirectories(resultDir);

            // Perform warmup, if requested
            if (warmup) {
                warmup(connection, resultDir);
            }

            // Perform testing
            test(connection, queries, resultDir, verifyDir);

        } finally {
            // Ensure to shutdown repository
            repository.shutDown();
        }

        // Compute and show total execution time
        final long end = System.currentTimeMillis();
        LOGGER.info("Completed in {} ms", end - start);
    }

    private static Map<String, String> loadQueries(final Path queryDir)
            throws IOException, MalformedQueryException {

        // List and sort query files for consistency
        final List<Path> files = Files.list(queryDir).collect(Collectors.toList());
        Collections.sort(files);

        // Read and validate each query file, returning the list of queries read
        final Map<String, String> queries = Maps.newHashMap();
        for (final Path file : files) {

            // Skip files in query directory not ending with .sparql
            if (!file.toString().endsWith(".sparql")) {
                continue;
            }

            // Use file name (without extension) as query name
            final String key = file.getFileName().toString().replace(".sparql", "");

            // Use file content as query string (without #comment lines)
            final StringBuilder queryBuilder = new StringBuilder();
            for (final String line : Files.readAllLines(file, Charsets.UTF_8)) {
                if (!line.trim().startsWith("#")) {
                    queryBuilder.append(line).append("\n");
                }
            }
            String query = queryBuilder.toString();

            // Check query is well-formed
            QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null); // check

            // Rewrite query (English -> Italian)
            query = rewriteQuery(query);

            // Check rewritten query is well-formed
            QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null); // check

            // Store query and log result
            queries.put(key, query);
            LOGGER.trace("Rewritten query {}: {}", key, query);
        }
        return queries;
    }

    private static String rewriteQuery(String q) {
        q = q.replace(":ShowInMunicipality", ":Presenta_in_comune");
        q = q.replace(":ShowInHospital", ":Presenta_in_ospedale");
        q = q.replace(":BirthManagement", ":Nascita-Astratto3");
        q = q.replace(":Municipality", ":Comune");
        q = q.replace(":PublicOffice", ":Ufficio_pubblico");
        q = q.replace(":RegistrationRequest", ":Richiesta_iscrizione");
        q = q.replace(":AP5Request.PersonalDetails.CF", ":AP5_richiesta.Generalita.CF");
        q = q.replace("BirthManagement", "Nascita-Astratto3");
        // q = q.replace("trace:execution_of",
        // "(rdf:type / ^<http://dkm.fbk.eu/TTrace_Ontology#associated_to>)");
        return q;
    }

    private static void warmup(final RepositoryConnection connection, final Path resultDir)
            throws RepositoryException, QueryEvaluationException, IOException {

        // Retrieve TBox, logging number of properties and classes
        LOGGER.info("Retrieving TBox...");
        final List<IRI> properties = Lists
                .newArrayList(retrieveIRIs(connection, "SELECT DISTINCT ?p WHERE { ?s ?p ?o }"));
        LOGGER.info("Retrieved {} property/ies", properties.size());
        final List<IRI> types = Lists
                .newArrayList(retrieveIRIs(connection, "SELECT DISTINCT ?t WHERE { ?s a ?t }"));
        LOGGER.info("Retrieved {} type/es", types.size());

        // Shuffle properties and classes
        Collections.shuffle(properties);
        Collections.shuffle(types);

        // Dump warmup statistics to file
        try (Writer out = Files.newBufferedWriter(resultDir.resolve("warmup.tsv"),
                Charsets.UTF_8)) {

            // Write header
            out.write("type\turi\tcount\ttime\n");

            // Fetch property counts
            for (int i = 0; i < properties.size(); ++i) {
                final String p = properties.get(i).toString();
                final long ts = System.currentTimeMillis();
                int count = retrieveCount(connection, "?s <" + p + "> ?o", null);
                long time = System.currentTimeMillis() - ts;
                LOGGER.info("Queried for property {}/{} <{}>: {} triples, {} ms", i + 1,
                        properties.size(), p, count, time);
                out.write(String.format("property\t%s\t%d\t%d\n", p, count, time));
            }

            // Fetch type instances counts
            for (int i = 0; i < types.size(); ++i) {
                final String t = types.get(i).toString();
                final long ts = System.currentTimeMillis();
                int count = retrieveCount(connection, "?s a <" + t + ">", "?s");
                long time = System.currentTimeMillis() - ts;
                LOGGER.info("Queried for type {}/{} <{}>: {} instances, {} ms", i + 1,
                        types.size(), t, count, time);
                out.write(String.format("type\t%s\t%d\t%d\n", t, count, time));
            }
        }
    }

    private static void test(final RepositoryConnection connection,
            final Map<String, String> queries, final Path resultDir, final Path verifyDir)
            throws Throwable {

        // Log beginning
        LOGGER.info("Testing {} query/ies...", queries.size());

        // Dump query statistics to file
        try (final Writer out = Files.newBufferedWriter(resultDir.resolve("result.tsv"),
                Charsets.UTF_8)) {

            // Emit TSV header
            out.write("query\tverified\tcount\telapsed\n");

            int index = 0;
            for (final Map.Entry<String, String> entry : queries.entrySet()) {

                // Retrieve query name and string
                final String name = entry.getKey();
                final String sparql = entry.getValue();

                // Evaluate the query, counting #results and measuring elapsed time
                final long start = System.currentTimeMillis();
                final Object result;
                final int count;
                try {
                    final Query query = connection.prepareQuery(QueryLanguage.SPARQL, sparql);
                    if (query instanceof TupleQuery) {
                        try (final TupleQueryResult cursor = ((TupleQuery) query).evaluate()) {
                            final List<String> vars = cursor.getBindingNames();
                            final List<BindingSet> tuples = Iterations.asList(cursor);
                            count = tuples.size();
                            result = new IteratingTupleQueryResult(vars, tuples);
                        }
                    } else if (query instanceof GraphQuery) {
                        try (final GraphQueryResult cursor = ((GraphQuery) query).evaluate()) {
                            final Map<String, String> ns = cursor.getNamespaces();
                            final List<Statement> stmts = Iterations.asList(cursor);
                            count = stmts.size();
                            result = new IteratingGraphQueryResult(ns, stmts);
                        }
                    } else if (query instanceof BooleanQuery) {
                        count = 1;
                        result = ((BooleanQuery) query).evaluate();
                    } else {
                        throw new Error("Unknown query type (!): " + query.getClass());
                    }
                } catch (final QueryResultHandlerException ex) {
                    throw MoreObjects.firstNonNull(ex.getCause(), ex); // unwrap
                } catch (final MalformedQueryException ex) {
                    throw new Error("Unexpected error (!)", ex); // validated before
                }
                final long elapsed = System.currentTimeMillis() - start;

                // Serialize query results to file (after elapsed time measured)
                final Path resultFile = resultDir.resolve(name + ".result");
                try (OutputStream os = new BufferedOutputStream(
                        Files.newOutputStream(resultFile))) {
                    if (result instanceof TupleQueryResult) {
                        final TupleQueryResult r = (TupleQueryResult) result;
                        QueryResultIO.writeTuple(r, TupleQueryResultFormat.TSV, os);
                    } else if (result instanceof GraphQueryResult) {
                        final GraphQueryResult r = (GraphQueryResult) result;
                        QueryResultIO.writeGraph(r, RDFFormat.NTRIPLES, os);
                    } else if (result instanceof Boolean) {
                        final Boolean r = (Boolean) result;
                        QueryResultIO.writeBoolean(r, BooleanQueryResultFormat.TEXT, os);
                    }
                }

                // Validate query output, if possible
                String verified = "?";
                if (verifyDir != null) {
                    final Path referenceFile = verifyDir
                            .resolve(resultFile.getFileName().toString());
                    if (Files.exists(referenceFile)) {
                        final List<String> resultLines = Files.readAllLines(resultFile,
                                Charsets.UTF_8);
                        final List<String> referenceLines = Files.readAllLines(referenceFile,
                                Charsets.UTF_8);
                        Collections.sort(resultLines);
                        Collections.sort(referenceLines);
                        verified = resultLines.equals(referenceLines) ? "OK" : "MISMATCH";
                    }
                }

                // Store query statistics
                out.write(String.format("%s\t%s\t%d\t%d\n", name, verified, count, elapsed));

                // Log query statistics
                ++index;
                LOGGER.info("Tested query {}/{} '{}': {} results, {} ms, {}", index,
                        queries.size(), name, count, elapsed, verified);
            }
        }
    }

    private static int retrieveCount(RepositoryConnection connection, String pattern,
            @Nullable String var) {

        // Setup the query
        final TupleQuery query = connection.prepareTupleQuery("" //
                + "SELECT (COUNT(" + (var == null ? "*" : "DISTINCT " + var) + ") AS ?n)\n" //
                + "WHERE  { " + pattern + " }");

        // Evaluate the query, returning the number of triples (var = null) or distinct var values
        try (TupleQueryResult cursor = query.evaluate()) {
            final BindingSet result = cursor.next();
            return ((Literal) result.getValue("n")).intValue();
        }
    }

    private static Set<IRI> retrieveIRIs(final RepositoryConnection connection,
            final String queryString) throws RepositoryException, QueryEvaluationException {

        // Setup the query
        final TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        // Evaluate the query, returning all IRIs in its results
        try (final TupleQueryResult cursor = query.evaluate()) {
            final Set<IRI> iris = Sets.newHashSet();
            while (cursor.hasNext()) {
                final BindingSet bindings = cursor.next();
                for (final Binding binding : bindings) {
                    final Value value = binding.getValue();
                    if (value instanceof IRI) {
                        iris.add((IRI) value);
                    }
                }
            }
            return iris;
        }
    }

}
