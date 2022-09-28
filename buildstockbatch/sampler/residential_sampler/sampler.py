import pandas as pd
import networkx as nx
import time
import itertools as it
import multiprocessing
import random
import click
import pathlib
from .sampling_utils import get_param2tsv, get_samples, TSVTuple, get_all_tsv_issues, get_all_tsv_max_errors

random.seed(42)

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


def sample_param(param_tuple: TSVTuple, sample_df: pd.DataFrame, param:str, num_samples:int) -> list[str]:
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
    param2dep = {param:tsv_tuple[1] for (param, tsv_tuple) in param2tsv.items()}
    sample_df = pd.DataFrame()
    sample_df.loc[:, "Building"] = list(range(1,num_samples+1))
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
            samples_dict = {param:res_val.get() for param, res_val in zip(params, results)}
            print(f"Got results for {len(samples_dict)} params in {time.time()-st:.2f}s")
            assert len(samples_dict) == len(params)
            new_df = pd.DataFrame(samples_dict)
            sample_df = pd.concat([sample_df, new_df], axis=1)
    print(f"Sampled in {time.time()-s_time:.2f} seconds")
    print(f"Done sampling {len(param2tsv)} TSVs with {num_samples} samples.")
    return sample_df

@click.group()
def cli():
    pass

@cli.command()
@click.option("-p", "--project", type=str, required=True)
@click.option("-n", "--num_datapoints", type=int, required=True)
@click.option("-o", "--output", type=str, required=True)
def sample(project, num_datapoints, output):
    start_time = time.time()
    print(project, num_datapoints, output)
    sample_df = sample_all(pathlib.Path(project), num_datapoints)
    click.echo("Writing CSV")
    sample_df.to_csv(output, index=False)
    click.echo(f"Completed sampling in {time.time() - start_time:.2f} seconds")

@cli.command()
@click.argument("buildstock", type=str, required=True)
@click.option("-p", "--project", type=str, required=True)
@click.option("-o", "--output", type=str, default='errors.csv')
def verify(buildstock, project, output):
    buildstock_df = pd.read_csv(buildstock)
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
    click.echo("Now checking for max sampling inaccuracy")
    error_df = get_all_tsv_max_errors(sample_df=buildstock_df, project_dir= pathlib.Path(project))
    if (error_df['max_row_error'] < 1).all():
        click.echo(click.style("All rows are accurate to within 1 samples.", fg='green'))
        error_df = error_df.sort_values(by=['max_total_error'], key=lambda x: abs(x), ascending=False)
        error_df = error_df.drop(columns = ['max_row_error', 'max_row_error_param', 'max_row_error_group'])
    else:
        click.echo(click.style("Some rows have sampling error of more than 1 samples", fg='red'))
        error_df = error_df.sort_values(by=['max_row_error'], key=lambda x: abs(x), ascending=False)
    click.echo("Top 10 TSVs with maximum number of sample difference between total expected counts and true counts:")
    click.echo(error_df.head(10))
    error_df.to_csv(output)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
