#!/usr/bin/env python3
"""
Script for aggregating raw results in reports folder and producing all the measures and plots reported in the paper.

This script loads configuration in ``config.py`` and looks at files ``reports/eval-store-{run}/{setting}/traces.tsv``
and ``reports/eval-query-{run}/{setting}/result.tsv`` (only files matched by the configuration are loaded). Therefore
these files should be present in order to generate complete plots (missing data will cause 'holes' in the plots).
All generated plots are placed in folder ``analysis`. This script is separated from ``test.py`` as it relies on 
numpy, scipy, pandas and matplotlib which may be unavailable on the machine where the tests are run. 
"""

# Imports
import os
import itertools
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FuncFormatter
from scipy.interpolate import griddata, SmoothBivariateSpline
from config import *


def aggregate_repository_statistics():
  """
  Loads all "stats_{size}_{approach}.tsv" files under reports/store, and aggregates them in a single data frame
  
  These statistics refer to the repository generation process. They provide the total number of triples, traces,
  activities, documents, etc, for reporting in the paper.
  
  Returns:
  DataFrame: a data frame where each line provides the statistics for a generated central repository
  """
  
  # Start with an empty result data frame
  df = None
  
  # Load and add to the result data frames all the rows for the 'stats_{size}_{approach}.tsv' files under 'reports/store'
  for size, approach in itertools.product(sizes, approaches):
    
    # Identify the TSV file for the <size, approach> repository. Skip if it does not exist
    path_tsv = os.path.join(path_reports, "store", f"stats_{size['id']}_{approach['id']}.tsv")
    if not os.path.isfile(path_tsv): continue
    
    # Load the TSV file
    row = pd.read_csv(path_tsv, sep="\t")
    
    # Add extra fields
    row["size"] = size["id"]
    row["sizeTraces"] = size["traces"]
    row["sizeMonths"] = round(size["traces"] / 500000 * 12, 2)
    row["approach"] = approach["id"]
    
    # Append row to result
    df = row if df is None else df.append(row, ignore_index=True)
  
  # Return the collectd data frame
  return df


def aggregate_store_results():
  """
  Loads all 'traces.tsv' files under eval-store-{run} and its sub-folders, and aggregates them in a single data frame.
  
  Paths and settings are taken from the configuration. Data is loaded from multiple runs, and in that case for each
  test, the aggregated row that is produced is the one coming from the run with the best throughput.
  
  Returns:
  DataFrame: a data frame where each line is the aggregated result of a test
  """
  
  # Utility function to load a test TSV and aggregate it into a single row that is appended to result
  def aggregate_tsv(result, run, size, approach, cpu, test, skip):
    
    # Identify the TSV file for the test; return the result unchanged if the file does not exist
    folder = f"{size['id']}_{approach['id']}_{test}" + (f"_{cpu['id']}" if cpu is not None else "")
    path_tsv = os.path.join(path_reports, "eval-store-" + run, folder, "traces.tsv")
    if not os.path.isfile(path_tsv): return result
    
    # Load the TSV file
    df = pd.read_csv(path_tsv, sep="\t")
    
    # Skip first and last rows as specified by parameter 'skip', raising exception if no row remains
    index_max = df["index"].max();
    index_to = index_max - skip[1]
    index_from = skip[0]
    if (index_to < index_from): raise Exception(f"No trace remaining after skipping in file {path_tsv}")
    len_before = len(df)
    df = df[np.logical_and(df["index"] >= index_from, df["index"] <= index_to)]
    
    # Compute #traces and throughput
    count = df["count"].sum()
    throughput = (count - 1) * 60000 / (df["tsEnd"].max() - df["tsEnd"].min())

    # Generate row and append it to result
    row = pd.DataFrame.from_dict({
      "test": [ test ],
      "approach": [ approach["id"] ],
      "size": [ size["id"] ],
      "sizeTraces": [ size["traces"] ],
      "sizeMonths": [ round(size["traces"] / 500000 * 12, 2) ],
      "numThreads": [ cpu["num_threads"] if cpu is not None else 1 ],
      "run": [ run ],
      "count": [ count ],
      "throughput": [ throughput ],
      "time": [ df["time"].mean() ],
      "timeConvert": [ df["timeConvert"].mean() ],
      "timeInfer": [ df["timeInfer"].mean() ],
      "timeStore": [ df["timeStore"].mean() ],
      "triples": [ df["triples"].mean() ],
      "triplesExplicit": [ df["triplesExplicit"].mean() ],
      "triplesInferred": [ df["triplesInferred"].mean() ]
    })
    result = row if result is None else result.append(row, ignore_index=True)
    return result
  
  # Load and aggregate all .tsv files available for all the settings and runs enabled in the configuration
  df = None
  for run in analysis_runs:
    for size in sizes:
      for approach in approaches:
        df = aggregate_tsv(df, run, size, approach, None, "sf", sf_skip)
        for cpu in cpus: df = aggregate_tsv(df, run, size, approach, cpu, "pf", pf_skip)
        if approach["supports_partial"]:
          df = aggregate_tsv(df, run, size, approach, None, "sp", sp_skip)
          for cpu in cpus: df = aggregate_tsv(df, run, size, approach, cpu, "pp", pp_skip)
  
  # Keep only the rows with the best throughput for each run (if same throughput, prefer first runs) 
  df["group"] = df["test"] + "_" + df["size"] + "_" + df["approach"] + "_" + df["numThreads"].astype(str)
  df["group_score"] = df["throughput"] + df["run"].transform(lambda x: analysis_runs.index(x) * -.000001)
  df = df[ df["group_score"] == df.groupby("group")["group_score"].transform("max") ]
  df = df.drop(["group", "group_score"], axis=1).copy()
  df = df.sort_values(["test", "approach", "size", "numThreads"], axis=0).reset_index()
  
  # Log average inference time per approach, for reporting in the paper
  for approach in approaches:
    for test in ["sp", "sf"]:
      timeInfer = df[np.logical_and(df["approach"] == approach["id"], df["test"] == test)]["timeInfer"];
      log(f"Average inference time {approach['id']}/{test}: {timeInfer.mean()} +/-{timeInfer.std()}")
  
  # Return the data frame
  return df


def aggregate_query_results():
  """
  Loads all 'result.tsv' files under eval-query-{run} and its sub-folders, and aggregates them in a single data frame.
  
  Paths and settings are taken from the configuration. Data is loaded from multiple runs and averaged.
  
  Returns:
  DataFrame: a data frame where each line is the aggregated result of a test
  """
  
  # Load and aggregate all result.tsv files available for all the sizes and runs enabled in the configuration
  df = None
  for run in analysis_runs:
    for size in sizes:
      for i in range(query_num_iterations):
        
        # Identify the TSV file for the test; return the result unchanged if the file does not exist
        path_tsv = os.path.join(path_reports, "eval-query-" + run, f"{size['id']}_{i}", "result.tsv")
        if not os.path.isfile(path_tsv): continue
    
        # Load the TSV file
        row = pd.read_csv(path_tsv, sep="\t")
        
        # Add constant-value columns to loaded rows
        row["size"] = size["id"]
        row["sizeTraces"] = size["traces"]
        row["sizeMonths"] = round(size["traces"] / 500000 * 12, 2)
        row["samples"] = 1
    
        # Get rid of 'verified' row (not used)
        row = row.drop(["verified"], axis=1)
    
        # Append row to data frame
        df = row if df is None else df.append(row, ignore_index=True)
  
  # Perform aggregation
  df["elapsed_std"] = df["elapsed"] # hack
  df = df.groupby(["query", "size"]).agg({
    "sizeTraces": "first",
    "sizeMonths": "first",
    "samples": "sum",
    "count": "mean",
    "elapsed": "mean",
    "elapsed_std": "std"
  })
  
  # Get rid of index
  df = df.reset_index().copy()
  
  # Return the data frame
  return df


def find_intersections(x, y1, y2):
  """
  Finds the intersection(s), if any, between two (x, y1) and (x, y2) lines.
  
  Parameters:
  x (array-like of floats): the x coordinates, shared by the two lines
  y1 (array-like of floats): the y1 coordinates (same length as x)
  y2 (array-like of floats): the y2 coordinates (same length as x)
  
  Return:
  (xint, yint): a pair of arrays x, y containing the coordinates of the intersection points
  """
    
  # Map to numpy array
  x = np.asarray(x)
  y1 = np.asarray(y1)
  y2 = np.asarray(y2)
    
  # Compute a, b parameters of equation y = a*x + b for the two lines
  l = len(x)
  a1 = (y1[1:l] - y1[0:l-1]) / (x[1:l] - x[0:l-1])
  b1 = y1[0:l-1] - a1 * x[0:l-1]
  a2 = (y2[1:l] - y2[0:l-1]) / (x[1:l] - x[0:l-1])
  b2 = y2[0:l-1] - a2 * x[0:l-1]

  # Find intersections, not considering they must lie in [x_i, x_i+1] intervals
  xint = - (b1 - b2) / (a1 - a2)
  yint = a1 * xint + b1
  
  # Filter intersections ensuring they occur in the proper x intervals, and return result
  mask = np.logical_and(xint > x[0:l-1], xint < x[1:l])
  return (xint[mask], yint[mask])


def plot_store_time_by_data_size(df, test, system, baseline=None):
  """
  Plots the trace store time of system and baseline, showing also inference time and as a function of data size.
  
  Parameters:
  df (DataFrame): the data frame with the aggregated results to plot
  test (str): the test name ('sp', 'sf')
  system (dict): the system object ('lo', 'lr')
  baseline (dict): the baseline object, optional ('co', 'cr')
  """
  
  # Plot system lines (2x): avg. time and avg. inference time
  dft = df[df["test"] == test]
  dfs = dft[dft["approach"] == system["id"]]
  if dfs.size > 0:
    plt.plot(dfs["sizeMonths"], dfs["time"], sty_color_s, label=system["label"], **sty_line_s)
    plt.plot(dfs["sizeMonths"], dfs["timeInfer"], sty_color_s, label=system["label"] + " (inference)", **sty_line_s_inf)

  # Plot baseline line, if a baseline was supplied
  if baseline != None:
    dfb = dft[dft["approach"] == baseline["id"]]
    if dfb.size > 0:
      plt.plot(dfb["sizeMonths"], dfb["time"], sty_color_b, label=baseline["label"], **sty_line_b)

  # Set axis labels and limits
  plt.gca().set_xlabel("# months data")
  plt.gca().set_xlim([0, sty_max_size_r])
  plt.gca().set_ylabel("update ms")
  plt.gca().set_ylim([0, sty_max_time_r[test]])
  if sty_ticks_size != None:
    plt.gca().set_xticks(sty_ticks_size)
  if sty_ticks_time_r.get(test) != None:
    plt.gca().set_yticks(sty_ticks_time_r[test])

  # Emit grid and legend
  plt.grid(**sty_grid)
  if sty_legend_oneplot and system != lr:
    plt.gca().legend().set_visible(False)
  else:
    plt.legend(loc="lower left" if system == lo else "upper right", **sty_legend)


def plot_store_throughput_by_data_size(df, test, system, baseline=None, num_threads=None):
  """
  Plots the throughput of system and baseline as a function of data size.
  
  Parameters:
  df (DataFrame): the data frame with the aggregated results to plot
  test (str): the test name ('pp', 'pf')
  system (dict): the system object ('lo', 'lr')
  baseline (dict): the baseline object, optional ('co', 'cr')
  num_threads (int): the number of threads for which to plot throughput; if omitted, the max number is used
  """
  
  # Use maximum number of threads, if not specified
  if num_threads == None: num_threads = df["numThreads"].max()
  
  # Plot system line
  dft = df[np.logical_and(df["test"] == test, df["numThreads"] == num_threads)]
  dfs = dft[dft["approach"] == system["id"]]
  if dfs.size > 0:
    plt.plot(dfs["sizeMonths"], dfs["throughput"], sty_color_s, label=system["label"], **sty_line_s)
  
  # Plot baseline line, if a baseline was supplied
  if (baseline != None):
    dfb = dft[dft["approach"] == baseline["id"]]
    if dfb.size > 0:
      plt.plot(dfb["sizeMonths"], dfb["throughput"], sty_color_b, label=baseline["label"], **sty_line_b)
  
  # Set axis labels and limits
  plt.gca().set_xlabel("# months data")
  plt.gca().set_xlim([0, sty_max_size_r])
  plt.gca().set_ylabel("updates / min")
  plt.gca().set_ylim([0, sty_max_throughput[test]])
  if sty_ticks_size != None:
    plt.gca().set_xticks(sty_ticks_size)
  if sty_ticks_throughput.get(test) != None:
    plt.gca().set_yticks(sty_ticks_throughput[test])

  # Emit grid and legend
  plt.grid(**sty_grid)
  if sty_legend_oneplot:
    plt.gca().legend().set_visible(False)
  else:
    plt.legend(loc="upper left" if system == lo else "upper right", **sty_legend)


def plot_store_throughput_by_num_threads(df, test, system, baseline=None):
  """
  Plots the througput of system and baseline for min and max data sizes, as function of number of CPU threads.
  
  Parameters:
  df (DataFrame): the data frame with the aggregated results to plot
  test (str): the test name ('pp', 'pf')
  system (dict): the system object ('lo', 'lr')
  baseline (dict): the baseline object, optional ('co', 'cr')
  """
  
  # Identify min and max data sizes in the data frame
  size_min = df["sizeMonths"].min()
  size_max = df["sizeMonths"].max()
  
  # Plot system lines for min and max data sizes, and fill the area between them
  dft = df[df["test"] == test]
  dfs = dft[dft["approach"] == system["id"]]
  if dfs.size > 0:
    dfs_min = dfs[dfs["sizeMonths"] == size_min]
    dfs_max = dfs[dfs["sizeMonths"] == size_max]
    plt.plot(dfs_min["numThreads"], dfs_min["throughput"], sty_color_s, label=system["label"], **sty_line_s, zorder=10)
    plt.plot(dfs_max["numThreads"], dfs_max["throughput"], sty_color_s, label="_nolegend_", **sty_line_s, zorder=10)
    plt.fill_between(dfs_min["numThreads"], dfs_min["throughput"], dfs_max["throughput"], color=sty_color_s, alpha=.3, zorder=10)
  
  # Plot baseline lines for min and max data sizes, and fill the area between them
  if baseline != None:
    dfb = dft[dft["approach"] == baseline["id"]]
    if dfb.size > 0:
      dfb_min = dfb[dfb["sizeMonths"] == size_min]
      dfb_max = dfb[dfb["sizeMonths"] == size_max]
      plt.plot(dfb_min["numThreads"], dfb_min["throughput"], sty_color_b, label=baseline["label"], **sty_line_b)
      plt.plot(dfb_max["numThreads"], dfb_max["throughput"], sty_color_b, label="_nolegend_", **sty_line_b)
      plt.fill_between(dfb_min["numThreads"], dfb_min["throughput"], dfb_max["throughput"], color=sty_color_b, alpha=.15, zorder=0)

  # Plot system line labels for min and max data sizes, if label coordinates are configured
  loc_s = sty_label_loc.get(test).get(system["id"])
  if loc_s != None:
    plt.text(loc_s[0], loc_s[1], f"{int(size_min)} months", color=sty_color_s, **sty_label)
    plt.text(loc_s[2], loc_s[3], f"{int(size_max)} months", color=sty_color_s, **sty_label)
  
  # Plot baseline line labels for min and max data sizes, if label coordinates are configured
  loc_b = sty_label_loc.get(test).get(baseline["id"]) if baseline != None else None
  if loc_b != None:
    plt.text(loc_b[0], loc_b[1], f"{int(size_min)} months", color=sty_color_b, **sty_label)
    plt.text(loc_b[2], loc_b[3], f"{int(size_max)} months", color=sty_color_b, **sty_label)

  # Plot intersection markers  
  if baseline != None and dfb.size > 0:
    xint_min, yint_min = find_intersections(dfs_min["numThreads"], dfs_min["throughput"], dfb_min["throughput"])
    xint_max, yint_max = find_intersections(dfs_max["numThreads"], dfs_max["throughput"], dfb_max["throughput"])
    xint, yint = np.concatenate([xint_min, xint_max]), np.concatenate([yint_min, yint_max])
    plt.scatter(xint, yint, **sty_int_marker, zorder=20)
  
  # Set axis labels and limits
  plt.gca().set_xlabel("# cores")
  plt.gca().set_xlim([0, sty_max_threads])
  plt.gca().set_ylabel("updates / min")
  plt.gca().set_ylim([0, sty_max_throughput[test]])
  if sty_ticks_threads != None:
    plt.gca().set_xticks(sty_ticks_threads)
  if sty_ticks_throughput.get(test) != None:
    plt.gca().set_yticks(sty_ticks_throughput[test])
  
  # Emit grid and legend
  plt.grid(**sty_grid)
  if sty_legend_oneplot:
    plt.gca().legend().set_visible(False)
  else:
    plt.legend(loc="upper left", **sty_legend)


def plot_store_comparison_heatmap(df, test, system, baseline):
  """
  Plots the delta between system and baseline throughput using a heat map.
  
  Parameters:
  df (DataFrame): the data frame with the aggregated results to plot
  test (str): the test name ('pp', 'pf')
  system (dict): the system object ('lo', 'lr')
  baseline (dict): the baseline object, optional ('co', 'cr')   
  """
  
  # Helper function that truncates a color map (i.e., uses only a subset of its color range) - taken from the Web
  def truncate_colormap(cmap, minval=0.0, maxval=1.0, n=100):
    new_cmap = LinearSegmentedColormap.from_list(
      'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
      cmap(np.linspace(minval, maxval, n)))
    return new_cmap

  # Select system and baseline data, aborting if they are missing or not comparable
  dft = df[df["test"] == test]
  dfs = dft[dft["approach"] == system["id"]]
  dfb = dft[dft["approach"] == baseline["id"]]
  if dfs.size == 0 or dfb.size != dfs.size: return
  
  # Reindex system and baseline data by <size, #threads>, to enable computing delta
  dfs = dfs.reset_index().set_index(["sizeMonths", "numThreads"])
  dfb = dfb.reset_index().set_index(["sizeMonths", "numThreads"])
  
  # Compute delta and store it in system data frame, then drop index
  dfs["delta"] = (dfs["throughput"] - dfb["throughput"]) / dfb["throughput"]
  dfs = dfs.reset_index()
  
  # Compute X (= data size), Y (= #threads), Z (= delta) coordinates of the heatmap  
  x = dfs["sizeMonths"].to_numpy()
  y = dfs["numThreads"].to_numpy()
  z = dfs["delta"].to_numpy()
  
  # Matplotlib / scipy magic to interpolate a surface given the <x, y, z> points
  xi, yi = np.mgrid[0:(sty_max_size_r + 0.75):50j, 0:sty_max_threads:50j]
  zi = griddata((x, y), z, (xi, yi), method='cubic', rescale=True)
  f = SmoothBivariateSpline(x, y, z)
  zii = f(xi, yi, grid=False)

  # Generate heat map (i.e., color surface based on interpolated zii values)
  cmap = truncate_colormap(plt.cm.RdBu, .3, .7)
  plt.contourf(xi, yi, zii, 100, cmap=cmap, alpha=1, vmin=-1, vmax=1)
  
  # Display color bar
  label = sty_title_heatmap["rdfs" if system["inference"] == "local_graphdb_rdfs" else "owl2rl"]
  cbar = plt.colorbar(fraction=.13, format=FuncFormatter(lambda x, pos: "{:+.0%}".format(x)))
  cbar.set_label(label)
   
  # Display scatter plot with grid of delta values
  for i in range(len(x)):
    vadjust = sty_vadjust_heatmap.get(y[i])
    color=plt.cm.RdBu(.01) if z[i] < 0 else plt.cm.RdBu(.99)
    plt.scatter(x[i], y[i], marker='o', c=[color], s=5, zorder=10)
    plt.text(x[i]+.15, y[i]-.15 + (vadjust if vadjust != None else 0), "%+.1f%%" % (z[i] * 100), fontsize=10, color=color)

  # Set axis labels
  plt.gca().set_xlabel("# months data")
  plt.gca().set_xlim([0, sty_max_size_r + 0.75])
  plt.gca().set_ylabel("# cores")
  if sty_ticks_size != None:
    plt.gca().set_xticks(sty_ticks_size)
  if sty_ticks_threads != None:
    plt.gca().set_yticks(sty_ticks_threads)
  
  # Emit grid
  plt.grid(**sty_grid)


def plot_query_times(df, selective=False):
  """
  Plots the query evaluation time as a function of data size.
  
  Parameters:
  df (DataFrame): the data frame with the aggregated query results to plot
  selective (bool): whether to plot only selective queries vs all queries
  """
  
  # Identify max data size
  size_max = df["sizeMonths"].max()
  
  # Plot a line for each query
  for query in queries:
    
    # Skip query if it is not selective and we are plotting selective queries only  
    if selective and query["selective"] != selective:
      plt.plot([],[]) # to skip color
      continue
    
    # Plot query line
    dfq = df[df["query"] == query["id"]]
    line, = plt.plot(dfq["sizeMonths"], dfq["elapsed"] / 1000, label="_nolegend_", **sty_line_q, zorder=10)
    
    # Display query label on the right
    vadjust = sty_vadjust_q.get(query["id"])
    time_max = dfq[dfq["sizeMonths"] == size_max]["elapsed"] / 1000 + (vadjust if vadjust != None else 0)
    plt.text(size_max + .2, time_max, query["label"], color=plt.getp(line, "color"), fontsize=10)
  
  # Set axis labels
  key = "selective" if selective else "all"
  plt.gca().set_xlabel("# months data")
  plt.gca().set_xlim([0, sty_max_size_q])
  plt.gca().set_ylabel("time [s]")
  plt.gca().set_ylim([0, sty_max_time_q[key]])
  if sty_ticks_size != None:
    plt.gca().set_xticks(sty_ticks_size)
  if sty_ticks_time_q.get(key) != None:
    plt.gca().set_yticks(sty_ticks_time_q[key])
  
  # Emit legend and grid
  plt.grid(**sty_grid)
  

def main():
  """
  Aggregate and emit store and query results, and generate plots out of them. 
  """

  # Chdir into script directory so to properly resolve relative paths in configuration
  os.chdir(os.path.dirname(os.path.realpath(__file__)) + "/")

  # Aggregate repository statistics and save resulting TSV
  dfr = aggregate_repository_statistics()
  dfr.to_csv(os.path.join(path_analysis, "repository.tsv"), sep="\t")
  
  # Aggregate store evaluation results and save resulting TSV  
  dfs = aggregate_store_results()
  dfs.to_csv(os.path.join(path_analysis, "eval_store.tsv"), sep="\t")

  # Aggregate query evaluation results and save resulting TSV  
  dfq = aggregate_query_results()
  dfq.to_csv(os.path.join(path_analysis, "eval_query.tsv"), sep="\t")

  # Utility function to create a figure of given size, invoke plot logic, and save the figure to file
  def save_plot(filename, width, height, plot_callback):
    path = os.path.join(path_analysis, filename)
    plt.figure(figsize=(width, height))
    plot_callback()
    plt.tight_layout()
    plt.savefig(path, bbox_inches='tight', pad_inches=0)
    log(f"Generated {filename} ({width} x {height})")
  
  # Emit individual query plots (these are the plots reported in the paper)
  save_plot("eval_query_all.pdf", sty_width, sty_height_q, lambda: plot_query_times(dfq, False))
  save_plot("eval_query_selective.pdf", sty_width, sty_height_q, lambda: plot_query_times(dfq, True))
  
  # Emit aggregated query plots (for authors' convenience)
  save_plot("eval_query.pdf", 2 * sty_width, sty_height_q, lambda: [
    plt.subplot2grid((1, 2), (0, 0)), plot_query_times(dfq, False),
    plt.subplot2grid((1, 2), (0, 1)), plot_query_times(dfq, True) ])
  
  # Emit individual plots for the partial traces scenario (these are the plots reported in the paper)
  save_plot("eval_store_time_by_size_owl.pdf", sty_width, sty_height_r["sp"], lambda : plot_store_time_by_data_size(dfs, "sp", lo, co))
  save_plot("eval_store_time_by_size_rdfs.pdf", sty_width, sty_height_r["sp"], lambda : plot_store_time_by_data_size(dfs, "sp", lr, cr))
  save_plot("eval_store_throughput_by_size_owl.pdf", sty_width, sty_height_r["pp"], lambda: plot_store_throughput_by_data_size(dfs, "pp", lo, co))
  save_plot("eval_store_throughput_by_size_rdfs.pdf", sty_width, sty_height_r["pp"], lambda: plot_store_throughput_by_data_size(dfs, "pp", lr, cr))
  save_plot("eval_store_throughput_by_cores_owl.pdf", sty_width, sty_height_r["pp"], lambda: plot_store_throughput_by_num_threads(dfs, "pp", lo, co))
  save_plot("eval_store_throughput_by_cores_rdfs.pdf", sty_width, sty_height_r["pp"], lambda: plot_store_throughput_by_num_threads(dfs, "pp", lr, cr))
  save_plot("eval_store_throughput_heatmap_rdfs.pdf", 2 * sty_width, sty_height_r["pp"], lambda: plot_store_comparison_heatmap(dfs, "pp", lr, cr))

  # Emit aggregated plot for the partial traces scenario (for authors' convenience)
  save_plot("eval_store.pdf", 2 * sty_width, sty_height_r["sp"] + 3 * sty_height_r["pp"], lambda: [
    plt.subplot2grid((4, 2), (0, 0)), plot_store_time_by_data_size(dfs, "sp", lo, co),
    plt.subplot2grid((4, 2), (0, 1)), plot_store_time_by_data_size(dfs, "sp", lr, cr),
    plt.subplot2grid((4, 2), (1, 0)), plot_store_throughput_by_data_size(dfs, "pp", lo, co),
    plt.subplot2grid((4, 2), (1, 1)), plot_store_throughput_by_data_size(dfs, "pp", lr, cr),
    plt.subplot2grid((4, 2), (2, 0)), plot_store_throughput_by_num_threads(dfs, "pp", lo, co),
    plt.subplot2grid((4, 2), (2, 1)), plot_store_throughput_by_num_threads(dfs, "pp", lr, cr),
    plt.subplot2grid((4, 2), (3, 0), colspan=2), plot_store_comparison_heatmap(dfs, "pp", lr, cr) ])
  
  # Emit individual plots for the full traces scenario (these are the plots reported in the supplemental material)
  save_plot("eval_store_time_by_size_owl_f.pdf", sty_width, sty_height_r["sf"], lambda : plot_store_time_by_data_size(dfs, "sf", lo, co))
  save_plot("eval_store_time_by_size_rdfs_f.pdf", sty_width, sty_height_r["sf"], lambda : plot_store_time_by_data_size(dfs, "sf", lr, cr))
  save_plot("eval_store_throughput_by_size_owl_f.pdf", sty_width, sty_height_r["pf"], lambda: plot_store_throughput_by_data_size(dfs, "pf", lo, co))
  save_plot("eval_store_throughput_by_size_rdfs_f.pdf", sty_width, sty_height_r["pf"], lambda: plot_store_throughput_by_data_size(dfs, "pf", lr, cr))
  save_plot("eval_store_throughput_by_cores_owl_f.pdf", sty_width, sty_height_r["pf"], lambda: plot_store_throughput_by_num_threads(dfs, "pf", lo, co))
  save_plot("eval_store_throughput_by_cores_rdfs_f.pdf", sty_width, sty_height_r["pf"], lambda: plot_store_throughput_by_num_threads(dfs, "pf", lr, cr))
  save_plot("eval_store_throughput_heatmap_owl_f.pdf", 2 * sty_width, sty_height_r["pf"], lambda: plot_store_comparison_heatmap(dfs, "pf", lo, co))
  save_plot("eval_store_throughput_heatmap_rdfs_f.pdf", 2 * sty_width, sty_height_r["pf"], lambda: plot_store_comparison_heatmap(dfs, "pf", lr, cr))

  # Emit aggregated plot for the full traces scenario (for authors' convenience)
  save_plot("eval_store_f.pdf", 2 * sty_width, sty_height_r["sf"] + 4 * sty_height_r["sp"], lambda: [
    plt.subplot2grid((5, 2), (0, 0)), plot_store_time_by_data_size(dfs, "sf", lo, co),
    plt.subplot2grid((5, 2), (0, 1)), plot_store_time_by_data_size(dfs, "sf", lr, cr),
    plt.subplot2grid((5, 2), (1, 0)), plot_store_throughput_by_data_size(dfs, "pf", lo, co),
    plt.subplot2grid((5, 2), (1, 1)), plot_store_throughput_by_data_size(dfs, "pf", lr, cr),
    plt.subplot2grid((5, 2), (2, 0)), plot_store_throughput_by_num_threads(dfs, "pf", lo, co),
    plt.subplot2grid((5, 2), (2, 1)), plot_store_throughput_by_num_threads(dfs, "pf", lr, cr),
    plt.subplot2grid((5, 2), (3, 0), colspan=2), plot_store_comparison_heatmap(dfs, "pf", lo, co),
    plt.subplot2grid((5, 2), (4, 0), colspan=2), plot_store_comparison_heatmap(dfs, "pf", lr, cr) ])


if __name__ == "__main__":
  main() # Triggers script execution
