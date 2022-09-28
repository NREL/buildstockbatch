# python version of run_sampling.rb
# Author: Rajendra.Adhikari@nrel.gov

import pathlib
import pandas as pd
import networkx as nx
import numpy as np
import time
from joblib import Parallel, delayed
import itertools as it
import multiprocessing
import random
from collections import Counter, defaultdict
random.seed(42)
import math
import pandas as pd
import statistics
from functools import cache


TSVTuple = tuple[dict[tuple[str, ...], list[float]], list[str], list[str]]
def read_char_tsv(file_path: pathlib.Path) -> TSVTuple:
    dep_cols = []
    opt_cols = []
    sampling_col = None
    group2probs = {}
    with open(file_path) as file:
        for line_num, line in enumerate(file):
            if line[0] == '#':
                continue
            if line_num==0:
                for col_num, header in enumerate(line.split("\t")):
                    if header.startswith("Dependency="):
                        dep_cols.append(header.removeprefix('Dependency=').strip())
                    elif header.startswith("Option="):
                        opt_cols.append(header.removeprefix("Option=").strip())
                    elif header.strip().lower() == 'sampling_probability':
                        sampling_col = col_num
            else:
                line_array = line.split("\t")
                dep_val = tuple(line_array[:len(dep_cols)])
                opt_val = [float(v) for v in line_array[len(dep_cols) : len(dep_cols) + len(opt_cols)]]
                sampling_prob = float(line_array[sampling_col]) if sampling_col else 1
                group2probs[dep_val] = opt_val + [sampling_prob]  # append sampling probability at the end

    return group2probs, dep_cols, opt_cols

@cache
def get_param2tsv(project_dir: pathlib.Path) -> dict[str, TSVTuple]:
    characteristics_dir = project_dir / "housing_characteristics"
    s_time = time.time()
    param2tsv_path = {tsv_path.name.removesuffix('.tsv'): tsv_path for tsv_path in characteristics_dir.glob('*.tsv')}
    param2tsv = {}
    with Parallel(n_jobs=-2) as parallel:
        def read_tsv(param, tsv_path):
            return (param, read_char_tsv(tsv_path))
        res = parallel(map(delayed(read_tsv), *zip(*param2tsv_path.items())))
    param2tsv = dict(res)
    print(f"Got Param2tsv in {time.time()-s_time:.2f} seconds")
    return param2tsv

def get_samples(probs: list[float], options: list[str], num_samples: int) -> list[str]:
    """Returns a list of samples chosen from the options list as per the probability distribution in probs using quota
       sampling algorithm.
    Args:
        probs (list[float]): The probabilities for the options.
        options (list[str]): The options to sample from.
        num_samples (int): Number of samples to return.

    Returns:
        list[str]: The list of samples.
    """
    prob_options = list(sorted(zip(probs, options), key=lambda x: x[0], reverse=True))
    if num_samples < len(prob_options):
        prob_options = prob_options[:num_samples]
    new_probs, new_options = zip(*prob_options)
    probs_arr = np.array(new_probs)
    sample_dist = probs_arr * num_samples / sum(probs_arr)
    allocations = np.floor(sample_dist).astype(int)  # Assign integer number of samples at first
    remaining_samples = num_samples - int(sum(allocations))
    remaining_allocation = sample_dist - allocations
    extra_opts = sorted(enumerate(remaining_allocation), key=lambda x: (x[1], x[0]), reverse=True)[:remaining_samples]

    for indx, _ in extra_opts:
       allocations[indx] += 1

    samples = []
    for (indx, count) in enumerate(allocations):
        samples.extend([new_options[indx]] *count)
    random.shuffle(samples)
    return samples

def get_marginal_prob(initial_prob: float, count: int) -> float:
    """Returns the relative 'marginal' probability for an option with a initial probability and total counts.
       Marginal probability is defined as the probability of the option getting selected out of all other options at
       as the last sample.
       For example, Options=["Yes", "No"], Probabilities=[0.75, 0.25] and nsamples=2
       The first sample is chosen as "Yes". After that, the relative marginal probabilities for chosing "Yes" or "No"
       as the last sample are [0.5, 0.5].
    Args:
        initial_prob (float): Probability assigned to the option
        count (int): Total samples

    Returns:
        float: The marginal probability
    """
    return initial_prob * count - math.floor(initial_prob * count)


def get_issues(samples: list[str], probs: list[float], opts: list[str]) -> list[str]:
    """Find if the actual samples and reference samples are equivalent. For them to be equivalent:
    1. there are no more than 1 sample difference for any options and sum of differences (extra and deficit) is zero.
    2. The options for which there is one sample extra, and corresponding options for which there is one sample
    deficit have exact same marginal probability.
    Example: options = ["Yes", "No"]. Probabilities = [0.75, 0.25]. For n=2, both [Yes, Yes] and [Yes, No] are valid.

    Args:
        samples (list[str]): The samples to be tested
        probs (list[float]): The probabilities for the options as defined in the housing characteristics TSV
        opts (list[str]): The options corresponding to the probabilities.

    Returns:
        list[str]: List of issues found. For valid samples, it should be an empty list.
    """

    nsamples = len(samples)
    ref_samples = get_samples(probs, opts, nsamples)
    prob_options = list(sorted(zip(probs, opts), key=lambda x: x[0], reverse=True))
    if nsamples < len(prob_options):
        prob_options = prob_options[:nsamples]
    prob_sum = sum(prob_option[0] for prob_option in prob_options)
    probs = [p/prob_sum for p in probs]  # Renormalize the probabilities
    opt2probs = dict(zip(opts, probs))
    ref_sample2count = Counter(ref_samples)
    act_sample2count = Counter(samples)
    all_opts = set(ref_sample2count.keys()).union(act_sample2count.keys())
    plus_one_opts = []
    minus_one_opts = []
    issues = []
    for opt in all_opts:
        ref_count = ref_sample2count.get(opt, 0)
        act_count = act_sample2count.get(opt, 0)
        diff = act_count - ref_count
        if diff == 1:
            plus_one_opts.append(opt)
        elif diff == -1:
            minus_one_opts.append(opt)
        elif diff != 0:
            issues.append(f"Difference of more than 1 samples for option {opt}")

    if len(plus_one_opts) != len(minus_one_opts):
        issues.append("Differences in options don't sum to zero.")
    pos_diff_probs = sorted([(get_marginal_prob(opt2probs[opt], nsamples), opt) for opt in plus_one_opts])
    neg_diff_probs = sorted([(get_marginal_prob(opt2probs[opt], nsamples), opt) for opt in minus_one_opts])
    while pos_diff_probs and neg_diff_probs:
        pos_diff_prob, pos_opt = pos_diff_probs.pop()
        neg_diff_prob, neg_opt = neg_diff_probs.pop()
        if pos_opt not in ref_sample2count and neg_opt not in act_sample2count\
           and opt2probs[pos_opt] == opt2probs[neg_opt] and nsamples < len(opts):
            # The options are simply exchanged between reference and actual.
            # Example: If options=['A', 'B'] with equal probabilities and nsamples = 1, both ['A'] and ['B'] is valid.
            continue
        if pos_diff_prob == 0:
            issue = f"{pos_opt} has one sample more ({act_sample2count[pos_opt]}) than reference, and {neg_opt} "\
            f"has one sample less ({act_sample2count[pos_opt]}), but the marginal probability is zero for {pos_opt} "\
            f"so it can't have an extra sample in expense of {neg_opt} when nsamples is {nsamples}"
            issues.append(issue)
        elif abs(pos_diff_prob - neg_diff_prob) >= 1e-9:
            issue = f"{pos_opt} has one sample more than reference, and {neg_opt} "\
            f"has one sample less, but their marginal probabilities are not equal."
            issues.append(issue)

    return issues


def get_tsv_issues(param: str, tsv_tuple: TSVTuple, sample_df: pd.DataFrame)->list[str]:
    group2probs, dep_cols, opt_cols = tsv_tuple
    issues: list[str] = []
    if not dep_cols:
        probs = group2probs[()]
        samples=sample_df[param].values
        issues.extend(get_issues(samples=samples, probs=probs[:-1], opts=opt_cols))
    else:
        convert_dict = {dep_col: str for dep_col in dep_cols}
        sample_df = sample_df.astype(convert_dict)
        grp_cols = dep_cols[0] if len(dep_cols) == 1 else dep_cols
        grouped_df = sample_df.groupby(grp_cols, sort=False)
        for group_key, sub_df in grouped_df:
            group_key = group_key if isinstance(group_key, tuple) else (group_key,)
            probs = group2probs[group_key]
            samples = sub_df[param].values
            current_issues = [f"In {group_key} {issue}" for issue in get_issues(samples=samples,probs=probs[:-1], opts=opt_cols)]
            issues.extend(current_issues)
    if issues:
        print(f"{param} has {len(issues)} issues.")
    else:
        print(f"{param} passed.")
    return issues


def get_all_tsv_issues(sample_df: pd.DataFrame, project_dir: pathlib.Path) -> dict[str, list[str]]:
    param2tsv = get_param2tsv(project_dir)
    all_params = list(param2tsv.keys())
    results = []
    with multiprocessing.Pool(processes=max(multiprocessing.cpu_count() - 2, 1)) as pool:
        for param in all_params:
            _, dep_cols, _ = param2tsv[param]
            res = pool.apply_async(get_tsv_issues, (param, param2tsv[param], sample_df[dep_cols + [param]]))
            results.append(res)
        all_issues = {param:res_val.get() for param, res_val in zip(all_params, results)}
    return all_issues


def get_tsv_max_errors(param: str, tsv_tuple: TSVTuple, sample_df: pd.DataFrame) -> dict[str, tuple[float, str]]:
    sample_df = sample_df.astype(str)
    group2probs, dep_cols, opt_cols = tsv_tuple
    nsamples = len(sample_df)
    diff_counts = defaultdict(list)
    total_diff_counts: dict[str, int] = defaultdict(int)

    def get_stats(param, vals):
        return {'max_diff': max(vals)[0], 'max_diff_grp': max(vals)[1], 'total_diff': total_diff_counts[param]}

    def gather_diffs(df, probs, grp):
        samples_count = Counter(df[param].values)
        current_nsamples = len(df)
        sampling_probability = probs[-1]
        probs_sum = sum(probs[:-1])
        probs = [p/probs_sum for p in probs[:-1]]  # Normalize the probabilities
        true_nsamples_for_grp = nsamples * sampling_probability
        expected_samples = Counter(dict(zip(opt_cols, np.array(probs) * current_nsamples)))
        true_expected_samples = Counter(dict(zip(opt_cols, np.array(probs) * true_nsamples_for_grp)))
        for opt, expected_count in expected_samples.items():
            diff_counts[opt].append((expected_count - samples_count.get(opt, 0), grp))
            total_diff_counts[opt] += true_expected_samples[opt] - samples_count.get(opt, 0)

    if not dep_cols:
        probs = group2probs[()]
        gather_diffs(sample_df, probs, ())
    else:
        grp_cols = dep_cols[0] if len(dep_cols) == 1 else dep_cols
        grouped_df = sample_df.groupby(grp_cols, sort=False)
        for group_key, sub_df in grouped_df:
            group_key = group_key if isinstance(group_key, tuple) else (group_key,)
            probs = group2probs[group_key]
            gather_diffs(sub_df, probs, group_key)

    diff_stats = {param: get_stats(param, vals) for param, vals in diff_counts.items()}
    max_total_diff = max((stats['total_diff'], param) for param, stats in diff_stats.items())
    max_row_diff = max((stats['max_diff'], param, stats['max_diff_grp']) for param, stats in diff_stats.items())
    return {'max_row_error': max_row_diff[0], 'max_row_error_param': max_row_diff[1],
            'max_row_error_group': max_row_diff[2],
            'max_total_error': max_total_diff[0], 'max_total_error_param': max_total_diff[1]}

def get_all_tsv_max_errors(sample_df: pd.DataFrame, project_dir: pathlib.Path) -> pd.DataFrame:
    param2tsv = get_param2tsv(project_dir)
    all_params = list(param2tsv.keys())
    results = []
    with multiprocessing.Pool(processes=max(multiprocessing.cpu_count() - 2, 1)) as pool:
        for param in all_params:
            _, dep_cols, opt_cols = param2tsv[param]
            res = pool.apply_async(get_tsv_max_errors, (param, param2tsv[param], sample_df[dep_cols + [param]]))
            # res = test_sample(param, param2tsv[param], sample_df[dep_cols + [param]])
            results.append(res)
        errors_dict = {param:res_val.get() for param, res_val in zip(all_params, results)}
    error_df = pd.DataFrame(errors_dict).transpose()
    return error_df