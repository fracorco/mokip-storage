#!/usr/bin/env python3
"""
Evaluation script for running experiments and generating raw results in reports folder.

This script loads configuration in ``config.py`` and employs input data looked up in the paths configured in that file
(see there for list of data used). Traces and populated central repositories are generated as part of the test (unless
their dumps are available in the data folder, in which case thy are reused). At the end, raw results (logs and tsv
files) are stored in the reports folder. Call separate script 'analyze.py' to process these raw results to generate
the plots of the paper. 
"""

# Imports
import os
import sys
import itertools
import subprocess
from config import *


def shell(command):
  """
  Executes a shell command, terminating the script in case it returns a non-zero exit code.
  
  Parameters
  - command (str): the command to execute, as it would be typed on the bash shell
  """
  log("Executing: " + command)
  result = subprocess.call(command, shell=True, executable="/bin/bash")
  if (result != 0):
    log("Execution failed (result=%d)" % result)
    sys.exit()


def prepare_traces():
  """
  Generates the synthetic traces used for the central triplestores and perform store tests.
  
  This function generates a number of traces (stored in traces.jsonl.gz in data folder) consisting of multiple subsets:
  - the traces for populating the central triplestores, up to the largest configured data size
  - the traces for performing the (s)equential (f)ull test, which cannot be deleted so are not reusable for other tests
  - the traces for performing the (p)arallel (p)artial test, which are deleted after the test so can be reused for
    multiple CPU configurations as well as for the (s)equential (p)artial test
  - the traces for performing the (p)arallel (f)ull test, which cannot be deleted, so a subset has to be generated for
    each CPU configuration
    
  All generated trace subsets are stored in the data folder. Generation is omitted if the trace files are already there.
  """
  
  # Identify the number of traces for the largest dataset
  num_traces = max([size["traces"] for size in sizes])
  
  # Create folder (if needed) where to store trace generation logs under report folder
  if not os.path.isdir(f"{path_reports}/traces"):
    shell(f"mkdir -p {path_reports}/traces")

  # Generate synthetic traces to cover largest dataset plus traces for sf, pp, pf tests
  path_traces = path_data + "/traces.jsonl.gz"
  if not os.path.isfile(path_traces):
    shell(f"{cmd_mokip} simulate -d {path_diagram} -o {path_ontology} " +
                f"-n {num_traces + num_traces_sf + num_traces_pp + len(cpus) * num_traces_pf} " +
                f"-r {path_traces} " + 
          f"| tee {path_reports}/traces/simulate.log")

  # Extract the subset of traces for the sf trace store test
  path_traces_sf = path_data + "/traces_sf.jsonl.gz"
  if not os.path.isfile(path_traces_sf):
    shell(f"{cmd_gzip} -dkc {path_traces} 2> /dev/null " + 
          f"| head -n {num_traces + num_traces_sf} " + 
          f"| tail -n {num_traces_sf} " + 
          f"| {cmd_gzip} -9 - > {path_traces_sf}")

  # Extract the subset of traces for the pp trace store test (also used for sp)
  path_traces_pp = path_data + "/traces_pp.jsonl.gz"
  if not os.path.isfile(path_traces_pp):
    shell(f"{cmd_gzip} -dkc {path_traces} 2> /dev/null " +
          f"| head -n {num_traces + num_traces_sf + num_traces_pp} " + 
          f"| tail -n {num_traces_pp} " +
          f"| {cmd_gzip} -9 - > {path_traces_pp}")

  # Extract the subsets of traces for each pf trace store test on a CPU configuration
  for i in range(0, len(cpus)):
    path_traces_pf = path_data + "/traces_pf_" + cpus[i]["id"] + ".jsonl.gz"
    if not os.path.isfile(path_traces_pf):
      shell(f"{cmd_gzip} -dkc {path_traces} 2> /dev/null " +
            f"| head -n {num_traces + num_traces_sf + num_traces_pp + (i + 1) * num_traces_pf} " +
            f"| tail -n {num_traces_pf} " +
            f"| {cmd_gzip} -9 - > {path_traces_pf}")


def prepare_repository(size, approach):
  """
  Generates the central repository for the size and approach specified.
  
  If a dump of the requested repository already exists under the 'data' folder, the function does nothing.
  Otherwise, it creates a temporary repository under the 'repo' folder starting from the template for the approach.
  The repository is populated with the specified (size parameter) number of traces, using either direct population
  or the 'preload' command, depending on the configuration. Statistics of the generated repository are emitted to
  a file 'reports/store/stats_{size}_{approach}.tsv'
  
  Parameters:
  size (dict): the size for the central repository to generate
  approach (dict): the approach for the central repository to generate
  """
  # Create folder (if needed) where to store repository generation logs under report folder
  if not os.path.isdir(f"{path_reports}/store"):
    shell(f"mkdir -p {path_reports}/store")
  
  # Skip repository if we already have both its dump and statistics
  path_dump = path_data + "/" + size["id"] + "_" + approach["id"] + ".tar.lz"
  path_stats = path_reports + "/store/stats_" + size["id"] + "_" + approach["id"] + ".tsv"
  if os.path.isfile(path_dump) and os.path.isfile(path_stats):
    return

  # Initialize the repository directory, either from the template or from the dump if we have it
  path_repo = path_repos + "/" + size["id"] + "_" + approach["id"]
  if not os.path.isdir(path_repo):
    if not os.path.isfile(path_dump):
      template_id = "template_" + approach["id"]
      shell(f"{cmd_plzip} -kdc {path_templates}/{template_id}.tar.lz | tar xf - -C {path_repos}")
      shell(f"mv {path_repos}/{template_id} {path_repo}")
    else:
      shell(f"{cmd_plzip} -kdc {path_dump} | tar xf - -C {path_repos}")

  # Locate the repository URL
  repo_url = f"http://localhost:{server_port}/repositories/promo"
    
  # Populate the repository if needed (i.e., marker file .ready missing)
  if not os.path.isfile(f"{path_repo}/.ready"):

    # Locate the synthetic traces to populate
    path_traces = path_data + "/traces.jsonl.gz"
      
    # Populate either using default approach (i.e., mokip store command) or preload
    if not approach["preload"]:
      # Default population: start GraphDB, store traces, stop GraphDB
      shell(f"{cmd_graphdb} start {path_repo}")
      shell(f"{cmd_mokip} store -D -O --schema-tx -t {os.cpu_count()} -b 100 -u {repo_url} -d {path_traces} " +
                f"-o {path_ontology} -m {size['traces']} -i {approach['inference']} " +
                f"-U {'APPEND' if approach['supports_graphs'] else 'APPEND_DEFAULT_GRAPH'} " +
                f"--trace-namespace '{namespace_trace}' --graph-namespace '{namespace_graph}' " +
                f"-r {path_reports}/store/store_{size['id']}_{approach['id']}.tsv " +
            f"| tee {path_reports}/store/store_{size['id']}_{approach['id']}.log")
      shell(f"{cmd_graphdb} stop {path_repo}")
        
    else:
      # Preload population: generate repository files offline via preload, if needed
      if not os.path.isfile(f"{path_repo}/.preloaded"):
        
        # Populate an RDF file with the triples for all the traces to be put in the repository
        path_rdf = path_tmp + "/traces_" + size["id"] + ".nq.gz"
        shell(f"{cmd_mokip} store -t {os.cpu_count()} -b 100 -U APPEND -i CENTRAL -d {path_traces} " +
                  f"-o {path_ontology} -m {size['traces']} --dump-file {path_rdf} " +
                  f"--trace-namespace '{namespace_trace}' --graph-namespace '{namespace_graph}'" +
                  f"-r {path_reports}/traces/traces_{size['id']}.tsv " + 
              f"| tee {path_reports}/traces/traces_{size['id']}.log")
        
        # Convert fron .nq to .nt (with triple deduplication) in case the repository does not support graphs
        if not approach["supports_graphs"]:
          path_rdf_nq = path_rdf
          path_rdf = path_tmp + "/traces_" + size["id"] + ".nt.gz"
          shell(f"{cmd_rdfpro} @read {path_rdf_nq} @transform '=c sesame:nil' @unique @write {path_rdf}")
          shell(f"rm {path_rdf_nq}")
        
        # Invoke the preload command
        shell(f"{cmd_graphdb} preload -Dgraphdb.home={path_repo} -f -i promo {path_rdf}")
        shell(f"touch {path_repo}/.preloaded")

      # Preload population: start GraphDB, store schema, trigger closure recomputation, and finally stop GraphDB
      update = "INSERT DATA { [] <http://www.ontotext.com/owlim/system#reinfer> [] }"
      shell(f"{cmd_graphdb} start {path_repo}")
      shell(f"{cmd_mokip} store -O --schema-tx -U APPEND -u {repo_url} -d {path_traces} -o {path_ontology} -m 0 -i CENTRAL")
      shell(f"curl -X POST -m 604800 -d 'update={update}' {repo_url}/statements")
      shell(f"{cmd_graphdb} stop {path_repo}")

    # Mark the repository as ready
    shell(f"touch {path_repo}/.ready")

  # Generate dump file if needed
  if not os.path.isfile(path_dump):
    shell(f"tar -C {path_repos} -c -f - {size['id']}_{approach['id']} | plzip -9 > {path_dump}")

  # Collect statistics of generated repository via SPARQL query
  if not os.path.isfile(path_stats):
    query = """
      PREFIX trace: <http://dkm.fbk.eu/Trace_Ontology#>
      PREFIX bpmn:  <http://dkm.fbk.eu/index.php/BPMN_Ontology#>
      PREFIX domain: <https://dkm.fbk.eu/#>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX onto: <http://www.ontotext.com/>
      SELECT ?nt ?na ?nas ?nd ?nds ?nf ?nfs ?ncs ?nq ?nqg ?nqe ?nqs {
        { SELECT (COUNT(DISTINCT ?t) AS ?nt) { ?t trace:compliance ?c } }
        { SELECT (COUNT(DISTINCT ?a) AS ?na) { ?a trace:execution_of / rdf:type bpmn:activity } }
        { SELECT (SUM(?a) AS ?nas) { ?t trace:number_of_activities ?a } }
        { SELECT (COUNT(DISTINCT ?d) AS ?nd) { ?d trace:execution_of / rdf:type domain:Document } }
        { SELECT (SUM(?d) AS ?nds) { ?t trace:number_of_documents ?d } }
        { SELECT (COUNT(DISTINCT ?f) AS ?nf) { ?d domain:hasOutputField ?f} }
        { SELECT (SUM(?f) AS ?nfs) { ?t trace:number_of_simple_fields ?f } }
        { SELECT (SUM(?f) AS ?ncs) { ?t trace:number_of_complex_fields ?f } }
        { SELECT (COUNT(*) AS ?nq) { ?s ?p ?o } }
        { SELECT (COUNT(*) AS ?nqg) { GRAPH ?g { ?s ?p ?o } } }
        { SELECT (COUNT(*) AS ?nqe) { GRAPH onto:explicit { ?s ?p ?o } } }
        { SELECT (COUNT(*) AS ?nqs) { GRAPH onto:readonly { ?s ?p ?o } } }
      }"""
    shell(f"{cmd_graphdb} start {path_repo}")
    shell(f"curl -X POST -m 604800 -d 'query={query}' {repo_url} 2> /dev/null | sed -E 's/[,]/\\t/g' > {path_stats}")
    shell(f"{cmd_graphdb} stop {path_repo}")
   
  # Delete the repository to save space (will extract the dump when needed)
  shell(f"rm -Rf {path_repo}")
  

def run_experiments(size, approach):
  """
  Run the data population and data querying experiments for the size and approach specified.
  
  The function extracts the dump of the central repository corresponding to the size and approach specified.
  Then, it runs in sequence:
  - the query evaluation test (multiple iterations)
  - the (s)equential (p)artial population test, deleting inserted traces at the end
  - the (p)arallel (p)artial population test for each CPU setting, deleting inserted traces at the end
  - the (s)equential (f)ull population test, NOT deleting inserted traces at the traces due to baseline limits
  - the (p)arallel (f)ull population test for each CPU setting, NOT deleting inserted traces due to baseline limits
  If for any of these test the results are already available, the test is skipped (delete the test folder under
  'reports/setting' to force the execution of the test).
  
  The test drivers are implemented in Java and called here ('mokip' command invocations). They save raw results
  in .tsv files under the test directory ('traces.tsv' for store test, 'result.tsv' for query test).
  
  Parameters:
  size (dict): the size to test
  approach (dict): the approach to test
  """

  # Create folder (if needed) where to store query evaluation logs and raw results
  if not os.path.isdir(f"{path_reports}/eval-query-{current_run}"):
    shell(f"mkdir -p {path_reports}/eval-query-{current_run}")

  # Create folder (if needed) where to store store evaluation logs and raw results
  if not os.path.isdir(f"{path_reports}/eval-store-{current_run}"):
    shell(f"mkdir -p {path_reports}/eval-store-{current_run}")

  # Helper function computing the path of a file/folder for the given query evaluation iteration 
  def query_path(iteration, filename=None):
    folder = size["id"] + "_" + str(iteration)
    return path_reports + "/eval-query-" + current_run + "/" + folder + ("/" + filename if filename != None else "") 

  # Helper function computing the path of a file/folder for the given store evaluation test / cpu setting
  def store_path(test, cpu, filename=None):
    folder = size["id"] + "_" + approach["id"] + "_" + test + ("_" + cpu["id"] if cpu != None else "")
    return path_reports + "/eval-store-" + current_run + "/" + folder + ("/" + filename if filename != None else "")
    
  # Determine whether partial traces and named graphs are supported
  partial = approach["supports_partial"]
  graphs = approach["supports_graphs"]
  
  # Skip setting if all data is available (check for presence of log files - delete them to repeat test)
  may_skip = (not sp_enable or not partial or not graphs or os.path.isfile(store_path("sp", None, "eval.log")))
  may_skip = may_skip and (not sf_enable or os.path.isfile(store_path("sf", None, "eval.log")))
  if query_enable and approach["id"] == query_approach_id:
    for i in range(0, query_num_iterations):
      may_skip = may_skip and os.path.isfile(query_path(i, "eval.log"))
  for cpu in cpus:
    may_skip = may_skip and (not pp_enable or not partial or not graphs or os.path.isfile(store_path("pp", cpu, "eval.log")))
    may_skip = may_skip and (not pf_enable or os.path.isfile(store_path("pf", cpu, "eval.log")))
  if may_skip:
    return

  # Delete (if needed) and extract again the repository from its .tar.xz file, so to work on a clean repository (at the end of this test, the repository is no more clean)
  path_dump = path_data + "/" + size["id"] + "_" + approach["id"] + ".tar.lz"
  path_repo = path_repos + "/" + size["id"] + "_" + approach["id"]
  if not os.path.isfile(path_dump):
    log(f"Missing required file {path_dump}")
    sys.exit()
  if os.path.isdir(path_repo):
    shell(f"rm -Rf {path_repo}")
  shell(f"{cmd_plzip} -kdc {path_dump} | tar xf - -C {path_repos}")

  # Locate the repository URL
  repo_url = f"http://localhost:{server_port}/repositories/promo"
    
  # Query test (if enabled)
  if query_enable and approach["id"] == query_approach_id:
    for i in range(0, query_num_iterations):
      if not os.path.isfile(query_path(i, "eval.log")):
        shell(f"mkdir -p {query_path(i)}")
        shell(f"taskset -a {query_taskset} {cmd_graphdb} start {path_repo}")
        shell(f"taskset -a {query_taskset} {cmd_mokip} eval-query -w -u {repo_url} -q {path_queries} -r {query_path(i)} " +
              f"| tee {query_path(i, 'eval.log')}")
        shell(f"taskset -a {query_taskset} {cmd_graphdb} stop {path_repo}")

  # Sequential Partial test (to assess store times per trace and their components)
  if sp_enable and partial and graphs and not os.path.isfile(store_path("sp", None, "eval.log")):
    shell(f"mkdir -p {store_path('sp', None)}")
    shell(f"taskset -a {sp_taskset} {cmd_graphdb} start {path_repo}")
    shell(f"taskset -a {sp_taskset} {cmd_mokip} eval-store -d {path_data}/traces_pp.jsonl.gz " + 
              f"-u {repo_url} -i {approach['inference']} -U REPLACE_GRAPH_PROTOCOL " + 
              f"-o {path_ontology} --trace-namespace '{namespace_trace}' --graph-namespace '{namespace_graph}' " +
              f"-T {timeout} -r {store_path('sp', None)} -t 1 -w 50 -p -D " + 
          f"| tee {store_path('sp', None, 'eval.log')}")
    shell(f"taskset -a {sp_taskset} {cmd_graphdb} stop {path_repo}")

  # Parallel Partial (to assess throughput, varying # of CPU cores)
  for cpu in cpus:
    if pp_enable and partial and graphs and not os.path.isfile(store_path("pp", cpu, "eval.log")):
      shell(f"mkdir -p {store_path('pp', cpu)}")
      shell(f"taskset -a {cpu['taskset']} {cmd_graphdb} start {path_repo}")
      shell(f"taskset -a {cpu['taskset']} {cmd_mokip} eval-store -d {path_data}/traces_pp.jsonl.gz " +
                f"-u {repo_url} -i {approach['inference']} -U REPLACE_GRAPH_PROTOCOL " + 
                f"-o {path_ontology} --trace-namespace '{namespace_trace}' --graph-namespace '{namespace_graph}' " + 
                f"-T {timeout} -r {store_path('pp', cpu)} -t {max(2, cpu['num_threads'])} -w 50 -p -D " + 
            f"| tee {store_path('pp', cpu, 'eval.log')}")
      shell(f"taskset -a {cpu['taskset']} {cmd_graphdb} stop {path_repo}")

  # Sequential Full test (to assess store times per trace and their components)
  if sf_enable and not os.path.isfile(store_path("sf", None, "eval.log")):
    shell(f"mkdir -p {store_path('sf', None)}")
    shell(f"taskset -a {sf_taskset} {cmd_graphdb} start {path_repo}")
    shell(f"taskset -a {sf_taskset} {cmd_mokip} eval-store -d {path_data}/traces_sf.jsonl.gz " + 
              f"-u {repo_url} -i {approach['inference']} -U {'APPEND' if graphs else 'APPEND_DEFAULT_GRAPH'} " +
              f"-o {path_ontology} --trace-namespace '{namespace_trace}' --graph-namespace '{namespace_graph}' " +
              f"-T {timeout} -r {store_path('sf', None)} -t 1 -w 50 " + 
          f"| tee {store_path('sf', None, 'eval.log')}")
    shell(f"taskset -a {sf_taskset} {cmd_graphdb} stop {path_repo}")

  # Parallel Full (to assess throughput where data is also deleted, varying # of CPU cores)
  for cpu in cpus:
    if pf_enable and not os.path.isfile(store_path("pf", cpu, "eval.log")):
      update = "APPEND" if graphs else "APPEND_DEFAULT_GRAPH"
      shell(f"mkdir -p {store_path('pf', cpu)}")
      shell(f"taskset -a {cpu['taskset']} {cmd_graphdb} start {path_repo}")
      shell(f"taskset -a {cpu['taskset']} {cmd_mokip} eval-store -d {path_data}/traces_pf_{cpu['id']}.jsonl.gz " +
                f"-u {repo_url} -i {approach['inference']} -U {'APPEND' if graphs else 'APPEND_DEFAULT_GRAPH'} " + 
                f"-o {path_ontology} --trace-namespace '{namespace_trace}' --graph-namespace '{namespace_graph}' " +
                f"-T {timeout} -r {store_path('pf', cpu)} -t {max(2, cpu['num_threads'])} -w 50 " + 
            f"| tee {store_path('pf', cpu, 'eval.log')}")
      shell(f"taskset -a {cpu['taskset']} {cmd_graphdb} stop {path_repo}")

  # Drop the repository (both to save space and since it is not clean anymore)
  shell(f"rm -Rf {path_repo}")


def main():
  """
  Generates test data (traces, repositories) and perform query and store tests, writing raw results to reports folder.
  """

  # Chdir into script directory so to properly resolve relative paths in configuration
  os.chdir(os.path.dirname(os.path.realpath(__file__)) + "/")

  # Disable proxy as we access localhost, both to avoid overhead and issues with proxy misconfiguration
  os.environ['NO_PROXY'] = '*'

  # Stop any GraphDB server that we previously started and is possibly still around due to script interruption/crash
  shell(f"{cmd_graphdb} stopall")

  # Generate synthetic traces, both for populating the repositories and for the {sf, sp, pf, pp} tests
  prepare_traces()
  
  # Generate central repositories (if needed)
  for size, approach in itertools.product(sizes, approaches):
    prepare_repository(size, approach)
  
  # Run experiments (if needed)
  for size, approach in itertools.product(sizes, approaches):
    run_experiments(size, approach)


if __name__ == "__main__":
  main() # Triggers script execution
