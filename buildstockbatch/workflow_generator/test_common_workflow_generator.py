from buildstockbatch.workflow_generator.base import WorkflowGeneratorBase


def test_apply_logic_recursion():
    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg(["one", "two", "three"])
    assert apply_logic == "(one&&two&&three)"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({"and": ["one", "two", "three"]})
    assert apply_logic == "(one&&two&&three)"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({"or": ["four", "five", "six"]})
    assert apply_logic == "(four||five||six)"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({"not": "seven"})
    assert apply_logic == "!seven"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg(
        {"and": [{"not": "abc"}, {"or": ["def", "ghi"]}, "jkl", "mno"]}
    )
    assert apply_logic == "(!abc&&(def||ghi)&&jkl&&mno)"
