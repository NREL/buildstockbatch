# python version of run_sampling.rb
# Author: Rajendra.Adhikari@nrel.gov

import argparse
import pathlib
from re import sub
import time
import itertools as it
import multiprocessing
import random
from .sampling_utils import read_char_tsv
from .sampling_utils import get_param2tsv, get_issues, get_samples, get_marginal_prob, get_tsv_issues
from .sampling_utils import get_all_tsv_issues, get_tsv_max_errors, get_all_tsv_max_errors
from .sampler import sample_param, sample_all
from collections import Counter
import pandas as pd
import tempfile
random.seed(42)


def test_get_samples() -> None:
    samples = get_samples(probs = [1], options= ["Yes"], num_samples = 10)
    assert samples == ["Yes"]*10
    samples = get_samples(probs = [0.5, 0.5], options= ["Yes", "No"], num_samples = 4)
    assert sorted(samples) == ['No', 'No', 'Yes', 'Yes']

    samples = get_samples(probs = [0.5, 0.5], options= ["Yes", "No"], num_samples = 1)
    assert samples in [['No'], ['Yes']]

    # probabilities may not exactly sum to 1
    samples = get_samples(probs = [0.49, 0.49], options= ["Yes", "No"], num_samples = 4)
    assert sorted(samples) == ['No', 'No', 'Yes', 'Yes']
    samples = get_samples(probs = [0.5, 0.5], options= ["Yes", "No"], num_samples = 3)
    assert sorted(samples) in [['No', 'No', 'Yes'], ['No', 'Yes', 'Yes']]
    samples = get_samples(probs = [0.75, 0.25], options=["Yes", "No"], num_samples = 2)
    assert sorted(samples) in [['Yes', 'Yes'], ['No', 'Yes']]
    samples = get_samples(probs = [0.6, 0.15, 0.15, 0.10], options=["A", "B", "C", "D"], num_samples = 2)
    assert sorted(samples) == ['A', 'A']
    samples = get_samples(probs = [0.6, 0.15, 0.15, 0.10], options=["A", "B", "C", "D"], num_samples = 198)
    assert Counter(samples) in [Counter({'A': 119, 'B': 30, 'C': 29, 'D': 20}),
                                Counter({'A': 119, 'B': 29, 'C': 30, 'D': 20})]

def test_get_marginal_prob()->None:
    assert get_marginal_prob(0.75, 2) == 0.5
    assert get_marginal_prob(0.25, 2) == 0.5
    assert get_marginal_prob(0.75, 4) == 0
    assert get_marginal_prob(0.75, 0) == 0
    assert get_marginal_prob(0.75, 1) == 0.75


def test_get_issues()->None:
    issues = get_issues(["Yes"]*10, [1], ["Yes"])
    assert len(issues) == 0
    issues = get_issues(["Yes"], [0.5, 0.5], ["Yes", "No"])
    assert len(issues) == 0
    issues = get_issues(["No"], [0.5, 0.5], ["Yes", "No"])
    assert len(issues) == 0
    for sample, issue_count in [("A", 0), ("B", 0), ("C", 0), ("D", 1)]:
        issues = get_issues([sample], [0.3, 0.3, 0.3, 0.1], ["A", "B", "C", "D"])
        assert len(issues) == issue_count

    for samples, issue_count in [(["A", "B"], 0), (["A", "C"], 0), (["B", "C"], 0), (["A", "D"], 1)]:
        issues = get_issues(samples, [0.3, 0.3, 0.3, 0.1], ["A", "B", "C", "D"])
        assert len(issues) == issue_count

    issues = get_issues(["Yes", "No"], [1, 0], ["Yes", "No"])
    assert len(issues) == 1
    assert "marginal probability is zero" in issues[0]
    assert len(get_issues(["Yes", "No"], [0.75, 0.25], ["Yes", "No"])) == 0
    assert len(get_issues(["Yes", "Yes"], [0.75, 0.25], ["Yes", "No"])) == 0
    assert len(get_issues(["Yes", "Yes", "No"], [0.75, 0.25], ["Yes", "No"])) == 0

    issues = get_issues(["Yes", "Yes", "Yes"], [0.75, 0.25], ["Yes", "No"])
    assert len(issues) == 1
    assert "marginal probabilities are not equal" in issues[0]
    issues = get_issues(["No", "No", "No"], [0.75, 0.25], ["Yes", "No"])
    assert len(issues) == 2
    assert "Difference of more than 1 samples for option Yes" in issues
    assert "Difference of more than 1 samples for option No" in issues

    issues = get_issues(["A", "A", "A", "B", "C"], [0.2] * 5, ["A", "B", "C", "D", "E"])
    assert len(issues) > 1
    assert "Difference of more than 1 samples for option A" in issues
    assert "Differences in options don't sum to zero." in issues

    issues = get_issues(["A", "A", "B", "B", "B"], [0.2] * 5, ["A", "B", "C", "D", "E"])
    assert len(issues) > 1
    assert "Difference of more than 1 samples for option B" in issues
    assert "Differences in options don't sum to zero." in issues

    issues = get_issues(["A", "A", "B", "C", "D"], [0.2] * 5, ["A", "B", "C", "D", "E"])
    assert len(issues) == 1
    assert "marginal probability is zero" in issues[0]

    assert len(get_issues(["A", "E", "B", "C", "D"], [0.2] * 5, ["A", "B", "C", "D", "E"])) == 0

    # probabilities may not exactly sum to 1
    assert len(get_issues(["A", "E", "B", "C", "D"], [0.1] * 5, ["A", "B", "C", "D", "E"])) == 0


def test_get_tsv_issues():
    fan_tsv = pd.DataFrame({'Dependency=Bedrooms': [1, 2, 3, 4, 5],
                            'Option=None': [0.4] * 5,
                            'Option=Standard': [0.4] * 5,
                            'Option=Premium': [0.2] * 5})
    sample_df = pd.DataFrame({'Bedrooms': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        'Fan': ['None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard']
    })
    with tempfile.TemporaryDirectory() as tmp_dir:
        tsv_file = tmp_dir + "/Fan.tsv"
        fan_tsv.to_csv(tsv_file, sep='\t', index=False)
        tsv_tuple = read_char_tsv(tsv_file)
        issues = get_tsv_issues(param='Fan', tsv_tuple=tsv_tuple, sample_df=sample_df)
        assert len(issues) == 0

    fan_tsv = pd.DataFrame({'Option=None': [0.4],
                            'Option=Standard': [0.4],
                            'Option=Premium': [0.2]})
    sample_df = pd.DataFrame({'Bedrooms': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        'Fan': ['None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard']
    })
    with tempfile.TemporaryDirectory() as tmp_dir:
        tsv_file = tmp_dir + "/Fan.tsv"
        fan_tsv.to_csv(tsv_file, sep='\t', index=False)
        tsv_tuple = read_char_tsv(tsv_file)
        issues = get_tsv_issues(param='Fan', tsv_tuple=tsv_tuple, sample_df=sample_df)
        assert len(issues) > 0
        assert "Differences in options don't sum to zero." in issues
        assert "Difference of more than 1 samples for option Premium" in issues

    fan_tsv = pd.DataFrame({'Dependency=Bedrooms': [1, 2, 3, 4, 5],
                            'Option=None': [0.4] * 5,
                            'Option=Standard': [0.4] * 5,
                            'Option=Premium': [0.2] * 5})
    sample_df = pd.DataFrame({'Bedrooms': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        'Fan': ['None', 'Standard', 'None', 'Standard', 'Premium', 'Standard', 'None', 'Standard', 'None', 'Premium']
    })
    with tempfile.TemporaryDirectory() as tmp_dir:
        tsv_file = tmp_dir + "/Fan.tsv"
        fan_tsv.to_csv(tsv_file, sep='\t', index=False)
        tsv_tuple = read_char_tsv(tsv_file)
        issues = get_tsv_issues(param='Fan', tsv_tuple=tsv_tuple, sample_df=sample_df)
        assert len(issues) == 2
        assert "Premium has one sample more than reference" in issues[0]
        assert "Premium has one sample more than reference" in issues[1]


def test_sample_param():
    bedrooms_tsv = pd.DataFrame({'Option=1': [0.2], 'Option=2': [0.2], 'Option=3': [0.2], 'Option=4': [0.2],
                                 'Option=5': [0.2]})
    fan_tsv = pd.DataFrame({'Dependency=Bedrooms': [1, 2, 3, 4, 5],
                            'Option=None': [0.4] * 5,
                            'Option=Standard': [0.4] * 5,
                            'Option=Premium': [0.2] * 5})
    sample_df = pd.DataFrame({'Building': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    with tempfile.TemporaryDirectory() as tmp_dir:
        tsv_file = tmp_dir + "/Bedrooms.tsv"
        bedrooms_tsv.to_csv(tsv_file, sep='\t', index=False)
        tsv_tuple = read_char_tsv(tsv_file)
        samples = sample_param(param_tuple=tsv_tuple, sample_df=sample_df, param='Bedrooms', num_samples=10)
        assert len(samples) == 10
        assert sorted(samples) == ['1', '1', '2', '2', '3', '3', '4', '4', '5', '5']
        sample_df['Bedrooms'] = samples
        assert get_tsv_issues('Bedrooms', tsv_tuple, sample_df) == []
        tsv_file = tmp_dir + "/Ceiling Fan.tsv"
        fan_tsv.to_csv(tsv_file, sep='\t', index=False)
        tsv_tuple = read_char_tsv(tsv_file)
        samples = sample_param(param_tuple=tsv_tuple, sample_df=sample_df, param='Bedrooms', num_samples=10)
        assert len(samples) == 10
        assert sorted(samples) == ['None'] * 5 + ['Standard'] * 5
        sample_df['Fan'] = samples
        assert get_tsv_issues('Fan', tsv_tuple, sample_df) == []


def test_get_param2tsv():
    project_dir = pathlib.Path(__file__).parent / 'project_sampling_test'
    param2tsv = get_param2tsv(project_dir)
    assert len(param2tsv) == 2
    assert 'Bedrooms' in param2tsv
    assert 'Ceiling Fan' in param2tsv
    group2probs, dep_cols, opt_cols = param2tsv['Bedrooms']
    assert dep_cols == []
    assert opt_cols == ['1', '2', '3', '4', '5']
    assert len(group2probs) == 1
    assert group2probs[()] == [0.2] * 5 + [1.0]
    group2probs, dep_cols, opt_cols = param2tsv['Ceiling Fan']
    assert dep_cols == ['Bedrooms']
    assert opt_cols == ['None', 'Standard', 'Premium']
    assert len(group2probs) == 5
    for group in  ['1', '2', '3', '4', '5']:
        assert group2probs[(group,)] == [0.4, 0.4, 0.2, 0.2]


def test_sample_all():
    project_dir = pathlib.Path(__file__).parent / 'project_sampling_test'
    sample_df = sample_all(project_dir, 10)
    assert len(sample_df) == 10
    tsv_tuple = read_char_tsv(project_dir / 'housing_characteristics' / 'Bedrooms.tsv')
    assert get_tsv_issues(param='Bedrooms', tsv_tuple=tsv_tuple, sample_df=sample_df) == []
    tsv_tuple = read_char_tsv(project_dir / 'housing_characteristics' / 'Ceiling Fan.tsv')
    assert get_tsv_issues(param='Ceiling Fan', tsv_tuple=tsv_tuple, sample_df=sample_df) == []


def test_get_all_tsv_issues()->None:
    project_dir = pathlib.Path(__file__).parent / 'project_sampling_test'
    sample_df = pd.DataFrame({'Bedrooms': ['1', '1', '2', '2', '3', '3', '4', '4', '5', '5'],
        'Ceiling Fan': ['None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None',
                        'Standard']
    })
    issues_dict = get_all_tsv_issues(sample_df=sample_df, project_dir=project_dir)
    assert len(issues_dict) == 2
    for param, issues in issues_dict.items():
        assert issues == []


def test_get_tsv_errors()->None:
    project_dir = pathlib.Path(__file__).parent / 'project_sampling_test'
    sample_df = pd.DataFrame({'Bedrooms': ['1', '1', '2', '2', '3', '3', '4', '4', '5', '5'],
        'Ceiling Fan': ['None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard']
    })
    assert len(sample_df) == 10
    tsv_tuple = read_char_tsv(project_dir / 'housing_characteristics' / 'Bedrooms.tsv')
    error_dict = get_tsv_max_errors(param='Bedrooms', tsv_tuple=tsv_tuple, sample_df=sample_df)
    assert error_dict['max_row_error'] == 0
    assert error_dict['max_row_error_group'] == ()
    assert error_dict['max_total_error'] == 0
    tsv_tuple = read_char_tsv(project_dir / 'housing_characteristics' / 'Ceiling Fan.tsv')
    error_dict = get_tsv_max_errors(param='Ceiling Fan', tsv_tuple=tsv_tuple, sample_df=sample_df)
    assert error_dict['max_row_error'] == 0.4
    assert error_dict['max_row_error_param'] == 'Premium'
    assert error_dict['max_row_error_group'] in [(str(i),) for i in range(1, 6)]
    assert error_dict['max_total_error'] == 2.0
    assert error_dict['max_total_error_param'] == 'Premium'


def test_get_all_tsv_errors()->None:
    project_dir = pathlib.Path(__file__).parent / 'project_sampling_test'
    sample_df = pd.DataFrame({'Bedrooms': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        'Ceiling Fan': ['None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard', 'None', 'Standard']
    })
    error_df = get_all_tsv_max_errors(sample_df=sample_df, project_dir=project_dir)
    assert len(error_df) == 2
    assert (error_df['max_row_error'] < 1).all()  # Individual row error less than 1 sample
    assert (error_df['max_total_error'] <= 0.2 * len(sample_df)).all()  # total error less than 20%
    assert error_df.loc['Bedrooms'].max_row_error == 0
    assert error_df.loc['Bedrooms'].max_row_error_group == ()
    assert error_df.loc['Bedrooms'].max_total_error == 0
    assert error_df.loc['Ceiling Fan'].max_row_error == 0.4
    assert error_df.loc['Ceiling Fan'].max_row_error_group in [(str(i),) for i in range(1, 6)]
    assert error_df.loc['Ceiling Fan'].max_row_error_param == 'Premium'
    assert error_df.loc['Ceiling Fan'].max_total_error == 2.0
    assert error_df.loc['Ceiling Fan'].max_total_error_param == 'Premium'