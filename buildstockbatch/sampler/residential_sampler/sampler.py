import pandas as pd
import networkx as nx
import time
import itertools as it
import multiprocessing
import click
import pathlib
from .sampling_utils import get_param2tsv, get_samples, TSVTuple, get_all_tsv_issues, get_all_tsv_max_errors


def get_param_graph(param2dep: dict[str, list[str]]) -> nx.DiGraph:
    param2dep_graph = nx.DiGraph()
    for param, dep_list in param2dep.items():
        param2dep_graph.add_node(param)
        for dep in dep_list:
            param2dep_graph.add_edge(dep, param)
    return param2dep_graph


def get_topological_param_list(param2dep: dict[str, list[str]]) -> list[str]:
    param2dep_graph = get_param_graph(param2dep)
    topo_params = list(nx.topological_sort(param2dep_graph))
    return topo_params


def get_topological_generations(param2dep: dict[str, list[str]]) -> list[tuple[int, list[str]]]:
    param2dep_graph = get_param_graph(param2dep)
    return list(enumerate(nx.topological_generations(param2dep_graph)))


def sample_param(param_tuple: TSVTuple, sample_df: pd.DataFrame, param: str, num_samples: int) -> list[str]:
    start_time = time.time()
    group2values, dep_cols, opt_cols = param_tuple
    if not dep_cols:
        probs = group2values[()]
        samples = get_samples(probs, opt_cols, num_samples)
    else:
        grouped_df = sample_df.groupby(dep_cols, sort=False)
        prob_list = []
        sample_size_list = []
        index_list = []
        for group_key, indexes in grouped_df.groups.items():
            group_key = group_key if isinstance(group_key, tuple) else (group_key,)
            index_list.append(indexes)
            probs = group2values[group_key]
            prob_list.append(probs)
            sample_size_list.append(len(indexes))

        samples_list = map(get_samples, prob_list, it.cycle([opt_cols]), sample_size_list)
        flat_samples = []
        for indexes, samples in zip(index_list, samples_list):
            flat_samples.extend(list(zip(indexes, samples)))
        samples = [s[1] for s in sorted(flat_samples)]
    print(f"Returning samples for {param} in {time.time() - start_time:.2f}s")
    return samples


def sample_all(project_path, num_samples) -> pd.DataFrame:
    param2tsv = get_param2tsv(project_path)
    param2dep = {param: tsv_tuple[1] for (param, tsv_tuple) in param2tsv.items()}
    sample_df = pd.DataFrame()
    sample_df.loc[:, "Building"] = list(range(1, num_samples+1))
    s_time = time.time()
    with multiprocessing.Pool(processes=max(multiprocessing.cpu_count() - 2, 1)) as pool:
        for level, params in get_topological_generations(param2dep):
            print(f"Sampling {len(params)} params in a batch at level {level}")
            results = []
            for param in params:
                _, dep_cols, _ = param2tsv[param]
                res = pool.apply_async(sample_param, (param2tsv[param], sample_df[dep_cols], param, num_samples))
                results.append(res)
            st = time.time()
            samples_dict = {param: res_val.get() for param, res_val in zip(params, results)}
            print(f"Got results for {len(samples_dict)} params in {time.time()-st:.2f}s")
            assert len(samples_dict) == len(params)
            new_df = pd.DataFrame(samples_dict)
            sample_df = pd.concat([sample_df, new_df], axis=1)
    print(f"Sampled in {time.time()-s_time:.2f} seconds")
    print(f"Done sampling {len(param2tsv)} TSVs with {num_samples} samples.")
    return sample_df


@click.group()
def cli():
    """Perform sampling or verify existing samples (in buildstock.csv).
       Type `resstock_sampler sample --help` or `resstock_sampler verify --help` to know more.
    """
    pass


@cli.command()
@click.option("-p", "--project", type=str, required=True,
              help="The path to the project (most have housing_characteristics folder inside)")
@click.option("-n", "--num_datapoints", type=int, required=True,
              help="The number of datapoints to sample.")
@click.option("-o", "--output", type=str, required=True,
              help="The output filename for samples.")
def sample(project: str, num_datapoints: int, output: str) -> None:
    """Performs sampling for project and writes output csv file.
    """
    start_time = time.time()
    print(project, num_datapoints, output)
    sample_df = sample_all(pathlib.Path(project), num_datapoints)
    click.echo("Writing CSV")
    sample_df.to_csv(output, index=False)
    click.echo(f"Completed sampling in {time.time() - start_time:.2f} seconds")


@cli.command()
@click.argument("buildstock_file", type=str, required=True)
@click.option("-p", "--project", type=str, required=True,
              help="The path to the project (most have housing_characteristics folder inside)")
@click.option("-o", "--output", type=str, default='errors.csv',
              help="The output filename for error report.")
def verify(buildstock_file: str, project: str, output: str):
    """
       \b
       Checks the buildstock.csv file (BUILDSTOCK_FILE) for correctness. BUILDSTOCK_FILE is considered correct if
       the probability distribution in project TSVs can result in the BUILDSTOCK_FILE using quota sampling.

       \b
       In addition to correctness verification, it also calculates the maximum number of sample error for any
       options in each TSVs between the BUILDSTOCK_FILE and what one would expect purely based on the probabilities if
       fractional samples were possible. It also calculates smapling errors for each group in the TSV. An example is
       provided below to explain the error calculation futher.
       Consider a project with three TSVs.
       Bedrooms.tsv
       ----------
       Option=1    Option=2    Option=3    Option=4    Option=5    sampling_probability
            0.2         0.2         0.2         0.2         0.2                     1.0
       \b
       Fan.tsv
       ----------
       Dependency=param1    Option=None    Option=Standard    Option=Premium    sampling_probability
                      1            0.35              0.35                0.3                     0.2
                      2            0.35              0.35                0.3                     0.2
                      3            0.35              0.35                0.3                     0.2
                      4            0.35              0.35                0.3                     0.2
                      5            0.35              0.35                0.3                     0.2
       \b
       AC.tsv
       ----------
       Dependency=param2    Option=Yes    Option=No    sampling_probability
                    None           0.9          0.1                    0.35
                Standard           0.8          0.2                    0.35
                 Premium           0.1          0.9                    0.3
       Quota sampling in the above project for 10 samples can generate a buildstock.csv that looks like this:
       buildstock.csv
       -------------
       Building  Bedrooms         Fan            AC
              *         1         None          Yes
              *         1         Standard      Yes
              *         2         None          Yes
              *         2         Standard      Yes
              *         3         None          Yes
              *         3         Standard      Yes
              *         4         None          Yes
              *         4         Standard      Yes
              *         5         None          Yes
              *         5         Standard       No
       \b
       For nsamples=10, the error calculation for each of the TSV will be as follows.
       For Bedrooms.tsv, expected sampels for each option is 2, and there are exactly two samples for each option in the
       buildstock.csv. Hence, the max_option_error is 0 for all options.
       There are no dependencies, so, the max_group_error is also 0.
       \b
       For Fan.tsv, based on the sampling_probability and the options probabilities for each row
       the expected number of samples for None and Standard is 3.5 each, and expected number samples for Premium is 3
       Since the buildstock.csv has 5 samples each for None and Standard and no samples for Premium, there is an error
       of +1.5 samples for None and Standard, and error of -3 samples for Premium. Hence, max_option_error is -3 and
       max_option_error_option is Premium. There are 5 dependency groups [(1,), (2,), (3,), (4,), (5,)] in Fan.tsv with
       sampling_probability of 0.2 for all. Thus we expect 2 samples for each group in buildstock.csv. Since this
       matches exactly with what we have in buildstock.csv, max_group_error is 0.
       \b
       For AC.tsv, we expect 6.25 samples for Yes since 0.9 * 0.35 + 0.8 * 0.35 + 0.1 * 0.3 = 6.25. We also expect 3.75
       samples for No. Since there are 9 samples for Yes, and 1 sample for No in buildstock.csv, option=Yes has sampling
       error of +2.75 and option=No has error of -2.75. Hence max_option_error is either -2.75 or +2.75 (only absolute
       value is compared to determine the maximum, though the sign is preserved in the output). There are 3 dependency
       groups [(None,), (Standard,), (Premium,)] with sampling probability of 0.35, 0.35 and 0.3. Hence we expect 3.5,
       3.5 and 3.0 samples for each group. Since we have 5 samples each for the first two groups and no samples for the
       last, the max_group_error is -3.0 and max_group_error_group is (Premium,).
    """
    buildstock_df = pd.read_csv(buildstock_file)
    buildstock_df = buildstock_df.astype(str)
    issues_dict = get_all_tsv_issues(buildstock_df, pathlib.Path(project))
    issues_found = False
    for param, issues in issues_dict.items():
        if issues:
            click.echo(click.style(f"Following issues found for {param}", fg='red'))
            click.echo(click.style('\n'.join(issues), fg='red'))
            issues_found = True
    if not issues_found:
        click.echo(click.style("Buildstock.csv is correct.", fg='green'))
    click.echo("Now calculating max sampling error.")
    error_df = get_all_tsv_max_errors(sample_df=buildstock_df, project_dir=pathlib.Path(project))
    error_df = error_df.sort_values(by=['max_option_error'], key=lambda x: abs(x), ascending=False)
    click.echo("Top 10 TSVs with maximum option sampling errors")
    click.echo(error_df.head(10))
    error_df.to_csv(output)
    error_df = error_df.sort_values(by=['max_group_error'], key=lambda x: abs(x), ascending=False)
    click.echo("Top 10 TSVs with maximum group sampling errors")
    click.echo(error_df.head(10))


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
