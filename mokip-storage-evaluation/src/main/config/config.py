#!/usr/bin/env python3
"""
Evaluation configuration

This file is sourced by all the evaluation scripts. You may change it to configure the evaluation process, but editing
is not needed to reproduce the results of the paper.
"""

# Imports
import datetime

#
# Approaches (local=our approach)
#
# id               - identifier used in reports
# inference        - name of inference strategy to be selected in JAVA code
# preload          - whether to use GraphDB "preload" when generating repositories (use only with central approaches)
# supports_partial - whether partial traces and thus data deletion are supported (GraphDB+OWL2RL will timeout)
# supports_graphs  - whether named graphs (needed by partial traces) are supported (GraphDB+OWL2RL will not work)
# label            - the label used in generated plots
#
# Note: do not change variable names 'lr', 'lo', 'cr', 'co' as they are referenced by analyze script
#
lr =  { "id": "lr", "inference": "local_graphdb_rdfs", "preload": False,
        "supports_partial": True, "supports_graphs": True, "label": "MoKiP system" }
lo =  { "id": "lo", "inference": "local_graphdb_owl2", "preload": False,
         "supports_partial": True, "supports_graphs": True, "label": "MoKiP system" }
cr =  { "id": "cr", "inference": "central", "preload": True,
        "supports_partial": True, "supports_graphs": True, "label": "baseline system" }
co =  { "id": "co", "inference": "central", "preload": True,
        "supports_partial": False, "supports_graphs": False, "label": "baseline system" }
approaches = [ lr, lo, cr, co ]  # only the approaches listed here are evaluated

#
# Dataset sizes
#
# id     - identifier used in reports
# traces - the number of traces (used at trace generation time)
#
# Note: the sizes defined below support both ~logarithmic and linear (every 2 months) scales; we started from a
# logarithmic one and then used a linear scale in the paper.
#
d01 = { "id": "d01", "traces": 1370 }
d03 = { "id": "d03", "traces": 4110 }
d07 = { "id": "d07", "traces": 9589 }
d14 = { "id": "d14", "traces": 19178 }
m01 = { "id": "m01", "traces": 41667 }
m02 = { "id": "m02", "traces": 83333 }
m03 = { "id": "m03", "traces": 125000 }
m04 = { "id": "m04", "traces": 166667 }
m06 = { "id": "m06", "traces": 250000 }
m08 = { "id": "m08", "traces": 333333 }
m10 = { "id": "m10", "traces": 416667 }
m12 = { "id": "m12", "traces": 500000 }
sizes = [ d01, m02, m04, m06, m08, m10, m12 ]  # only the approaches listed here are evaluated

#
# Dataset generation parameters (don't need to change them, do not affect evaluation results)
#
namespace_trace = "trace:"  # the namespace to use for the URIs of generated trace individuals
namespace_graph = "promo:"  # the namespace to use for the URIs of trace named graphs
num_threads_store = 4       # the number of threads to use when preparing a repository of a certain data size
server_port = 50082         # the port of GraphDB (must match graphdb.connector.port in graphdb.properties)

#
# CPU configurations
#
# id          - identifier used in reports
# num_threads - the number of CPU threads used in the CPU configuration
# taskset     - the arguments to pass to command 'taskset' to enable those CPU threads
#
# Note: the sizes defined below are for a dual CPU system with 12 cores overall (6 + 6) and hyperthreading, so each
# CPU core = 2x CPU threads.  CPU threads are indexed by (hyperthread index, core ID, physical CPU ID) in that order
# (refer to /proc/cpuinfo), so range 0-11 (the maximum we use) make use of only 1 hyperthread of the 2 hyperthreads of
# each core.  This is because we don't want to use hyper-threading in the evaluation, as the increment in performance
# is definitely not linear and this compromises the analysis of evaluation results (a better solution would be to
# disable hyper-threading in the BIOS of the machine, but we don't have access to it).  Also, note that range 0-1 refer
# to core #1 in CPU #1 and CPU #2, so we are using both CPUs here and in following configurations.
# 
cpu_1t = { "id": "1t", "num_threads": 1, "taskset": "-c 0" }
cpu_2t = { "id": "2t", "num_threads": 2, "taskset": "-c 0-1" }
cpu_4t = { "id": "4t", "num_threads": 4, "taskset": "-c 0-3" }
cpu_6t = { "id": "6t", "num_threads": 6, "taskset": "-c 0-5" }
cpu_8t = { "id": "8t", "num_threads": 8, "taskset": "-c 0-7" }
cpu_10t = { "id": "10t", "num_threads": 10, "taskset": "-c 0-9" }
cpu_12t = { "id": "12t", "num_threads": 12, "taskset": "-c 0-11" } 
cpus = [ cpu_1t, cpu_2t, cpu_4t, cpu_6t, cpu_8t, cpu_10t, cpu_12t ]  # only the configurations listed here are evaluated

#
# Run selection
#
# Note: query and store reports are put in folders "eval-query-{current_run}" and "eval-store-{current_run}" (if
# already there, tests are skipped).  Therefore, variable 'current_run' allows repeat the evaluation multiple times.
# whereas variable 'analysis_runs' controls which run(s) are considered when analyzing results and producing reports.
# At analysis time, in presence of multiple runs each measure (e.g., trace processing time for OWL2RL, our approach,
# 12 months data size) will be aggregated across round by taking the best value.  The motivation for this mechanism is
# that a single run takes several hours and any background activity on the test machine during that period may affect 
# the corresponding test(s) in a way difficult to spot manually.  By having multiple runs and taking the best measures
# across runs we filter out "bad results" due to background activity on the test machine.
#
current_run = "run1"          # the run to consider when running tests
analysis_runs = [ "run1", "run2", "run3" ]    # the run(s) to consider when analyzing test results and generating plots

#
# Queries (PPIs)
#
# id        - identifier used in reports
# label     - label used in generated plots
# selective - whether the query should be put in the zoom-in plot for selective queries
#
ppi1 = { "id": "ppi1", "label": "PPI.1", "selective": False }
ppi2 = { "id": "ppi2", "label": "PPI.2", "selective": False }
ppi3 = { "id": "ppi3", "label": "PPI.3", "selective": False }
ppi4 = { "id": "ppi4", "label": "PPI.4", "selective": False }
ppi5 = { "id": "ppi5", "label": "PPI.5", "selective": False }
ppi6 = { "id": "ppi6", "label": "PPI.6", "selective": True }
ppi7 = { "id": "ppi7", "label": "PPI.7", "selective": True }
ppi8 = { "id": "ppi8", "label": "PPI.8", "selective": True }
queries = [ ppi1, ppi2, ppi3, ppi4, ppi5, ppi6, ppi7, ppi8 ]  # only the queries listed here are evaluated


#
# Other query evaluation settings
#
# Note: regarding 'query_taskset' we enable all cores (no hyper-threading), but actually this doesn't matter much as
# the query engine is not able to parallelize the execution of a query over multiple threads.  Two or more CPU threads
# are OK (do not use a single CPU thread as it will be shared by both the driver submitting the query and GraphDB query
# engine).
#
query_enable = True         # whether to enable query evaluation
query_num_iterations = 10   # the number of times to evaluate each query (evaluation times are averaged)
query_approach_id = "lo"    # the approach whose repository should be used for queries (must include OWL2RL inferences)
query_taskset = "-c 0-11"   # the CPU configuration (command 'taskset' parameters) to use for the queries

#
# Other store evaluation settings
#
# Notes:
# - partial traces are automatically extracted from full traces them by including an increasing number of activities;
#   a full trace in the Birth Management process ~= 10 partial traces
# - the taskset parameters for sequential store evaluation are not much relevant; however, even if traces are processed
#   sequentially, multiple threads are involved in the processing, so DO NOT use a single CPU thread 
# - the 'skip' parameters affect the analysis of results, and discard certain traces at begin of the test when the
#   system is still warming up to achieve a steady state, and at end of test where CPU usage may be lower
#
sf_enable = True       # whether to enable the (s)equential (f)ull store evaluation test
sp_enable = True       # whether to enable the (s)equential (p)artial store evaluation test
pf_enable = True       # whether to enable the (p)arallel (f)ull store evaluation test
pp_enable = True       # whether to enable the (p)arallel (p)artial store evaluation test
num_traces_sf = 300    # (full) traces to use for the (s)equential (f)ull trace evaluation
num_traces_pf = 300    # (full) traces to use for the (p)arallel (f)ull trace evaluation
num_traces_pp = 300    # (full) traces to use for the (s)equential (p)artial evaluation (same as for sp test)
sp_taskset = "-c 0-11" # command 'taskset' parameters to use for (s)equential (p)artial trace evaluation
sf_taskset = "-c 0-11" # command 'taskset' parameters to use for (s)equential (f)ull trace evaluation
sf_skip = [100, 0]     # (full) traces not to analyze at [beginning, end] of (s)equential (f)ull trace evaluation
sp_skip = [100, 0]     # (full) traces not to analyze at [beginning, end] of (s)equential (p)artial trace evaluation
pf_skip = [100, 100]   # (full) traces not to analyze at [beginning, end] of (p)arallel (f)ull trace evaluation
pp_skip = [100, 100]   # (full) traces not to analyze at [beginning, end] of (p)arallel (p)artial trace evaluation

#
# Relative paths for INPUT data used by the evaluation scripts (don't need to change them, data MUST be present)
#
path_diagram = "models/diagram.json"   # location of BPD for the evaluated process
path_ontology = "models/ontology.tql"  # location of ontology (K+P+T layer, trace ABox excluded) for the process
path_queries = "queries"               # location of evaluation queries (the PPIs)
path_templates = "templates"           # location of dumps of preconfigured, empty GraphDB repositories used as templates

#
# Relative paths for OUTPUT data generated by the scripts (don't need to change, if deleted folders are re-generated)
#
path_repos = "repositories"  # where to temporarily store repository data (100-200 GB needed, deleted after use, on SSD)
path_reports = "reports"     # where to write raw test reports (logs, tsv files)
path_analysis = "analysis"   # where to write final evaluation results (plots, aggregated tsv files)
path_data = "data"           # where to store dumps of generated traces and repositories (100 GB needed, may be on HDD)
path_tmp = "tmp"             # where to store temporary data

#
# Location of invoked commands (modify if needed)
#
# In addition to these commands, the evaluation scripts execute via shell the following commands:
# head, tail, tee, tar, mkdir, mv, rm, touch, curl, taskset  (basically, you must be on Linux for the script to work)
#
cmd_promo = f"promo -Djava.io.tmpdir={path_tmp}"  # the 'promo' script running the Java code for repository population and testing
cmd_graphdb = f"graphdb"                          # the 'graphdb' Bash script controlling GraphDB
cmd_rdfpro = f"rdfpro"                            # the 'rdfpro' script running RDFpro
cmd_gzip = f"pigz"                                # the 'pigz' utility (apt install pigz)
cmd_plzip = f"plzip"                              # the 'plzip' utility  (apt install plzip)

# in addition: head, tail, tee
#
# Test timeout (ISO 8601 duration format)
#
# A test is interrupted after the timeout and, for store tests, the result obtained based on the traces processed so far.
# We set here a timeout large enough so not to occur during our tests, but even if it is triggered the throughput/time
# measures obtained during store evaluation are accurate.
#
timeout = "PT60M"  # 60 minutes

#
# Style settings for (s)ystem, (b)aseline, and other plot elements in (q)uery and sto(r)e plots
#
sty_width = 4                                                                   # width of individual plot (note: 2x for heatmap)
sty_height_r = 2.325 #2.75 # 2.325                                              # height of individual store plot
sty_height_r = { "sp": 2.75, "pp": 2.75, "sf": 2.325, "pf": 2.325 }             # height of individual store plot, indexed by test
sty_height_q = 3                                                                # height of individual query plot
sty_max_size_r = 13                                                             # max size axis value for store plots (months)
sty_max_size_q = 13.99                                                          # max size axis value for query plots (months)
sty_max_threads = 13                                                            # max threads axis value
sty_max_time_r = { "sp": 600, "sf": 1000 }                                      # max time axis value [ms] for store plots (sp, sf tests)
sty_max_time_q = { "all": 140, "selective": 0.07 }                              # max time axis value [s] for query plots (all, selective)
sty_max_throughput = { "pp": 1500, "pf": 1000 }                                 # max throughput axis value (pp, pf tests)
sty_ticks_size = [0, 2, 4, 6, 8, 10, 12]                                        # tick location on size axis (optional)
sty_ticks_threads = [0, 2, 4, 6, 8, 10, 12]                                     # tick location on threads axis (optional)
sty_ticks_time_r = {                                                            # tick location on time axis for store plots (optional)
  "sp": [0, 100, 200, 300, 400, 500, 600],
  "sf": [0, 200, 400, 600, 800, 1000]
}
sty_ticks_time_q = {                                                            # tick location on time axis for query plots (optional)
  "all": [0, 20, 40, 60, 80, 100, 120, 140] ,
  "selective": [0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07]
}
sty_ticks_throughput = {                                                        # tick location on throughput axis (optional) 
  "pp": [0, 250, 500, 750, 1000, 1250, 1500],
  "pf": [0, 200, 400, 600, 800, 1000]
}                               
sty_vadjust_heatmap = { 1: -.25, 2: +.25 }                                      # vertical adjustment for heat map numbers, by #threads
sty_vadjust_q = { "ppi4": 2, "ppi7": .001, "ppi8": -0.004 }                     # vertical adjustment for query labels in query plots
sty_color_s = "#1F77B4"                                                         # color associated to system under test
sty_color_b = "#FF7F0E"                                                         # color associated to baseline
sty_line_s = { "marker": "o", "markersize": 2.5, "linestyle": "-" }             # line style for system
sty_line_s_inf = { "marker": "o", "markersize": 2.5, "linestyle": "--" }        # line style for system inference time
sty_line_b = { "marker": "o", "markersize": 2.5, "linestyle": "-" }             # line style for baseline
sty_line_q = { "marker": "o", "markersize": 2.5, "linestyle": "-" }             # line style for queries
sty_grid = { "b": True, "color": "#e8e8e8", "linewidth": .25, "axis": "both" }  # plot grid options
sty_legend = { "fontsize": 10, "frameon": False }                               # plot legend options
sty_legend_oneplot = True                                                       # whether to show legend only for one plot
sty_int_marker = { "marker": "D", "s": 15, "c": ["k"] }                         # intersection markers
sty_label = { "fontsize": 10, "horizontalalignment": "center" }                 # line labels options
sty_title_heatmap = {
  "rdfs": "MoKiP vs baseline",
  "owl2rl": "MoKiP vs baseline"
}
sty_label_loc = {                                                               # loc. of line labels (min_x, min_y, max_x, max_y)
  "pp": { lo["id"]: (6, 525, 8, 200),
          lr["id"]: (8.25, 1350, 4, 125),
          cr["id"]: (1.75, 825, 11, 300) },
  "pf": { lo["id"]: (11, 525, 11, 250),
          co["id"]: (1.5, 250, 11, 75),
          lr["id"]: (10, 840, 4, 100),
          cr["id"]: (1.5, 525, 11, 225) }
}

#
# Customizable callback for logging messages to stdout
#
def log(message):
  """
  Displays a log message using format 'HH:mm:ss.SSS(I) <message>'.
  
  This is the same format used in Java code.
  Example:  08:35:47.875(I) Message
  """
  now = datetime.datetime.now();
  millis = now.microsecond / 1000
  print(now.strftime('%H:%M:%S.') + ("%03d(I) %s" % (millis, message)), flush=True)
