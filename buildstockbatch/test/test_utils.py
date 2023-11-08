import tempfile
from buildstockbatch.utils import log_error_details, _str_repr
import pytest
import os


def test_str_repr():
    test_obj = [
        {1, 2, 3, 4, 5, 6},
        {
            "List1": ["Item1", ("a", "b", "c", "d"), "item3"],
            "long_name_list": ["long_name_one_two_three", "long_name"],
            "dict": {
                "key1": ["List_item1", "List_item2", "List_item3"],
                "Key2": "value2",
                "key3": "value3",
                "key4": "val4",
            },
        },
    ]

    gen_repr = _str_repr(test_obj, list_max=2, dict_max=3, string_max=10)
    true_repr = (
        "[{'1','2','3' ...6},{'List1': ['Item1',('a','b' ...4) ...3],'long_...14..._list': ['long_...23..."
        "three','long_name'],'dict': {'key1': ['List_item1','List_item2' ...3],'Key2': 'value2',"
        "'key3': 'value3' ...4}}]"
    )

    assert true_repr == gen_repr


def test_get_error_details():
    tf = tempfile.NamedTemporaryFile("w+", delete=False)
    tf.close()

    @log_error_details(tf.name)
    def failing_function1(arg1):
        level_1_string = f"string1_{arg1}"
        level_1_dict = {"key1": "value1"}

        def failing_function2(arg2):
            level_2_string = f"string2_{arg2}"
            level_2_list = ["level_2_str1", "level_2_str2"]
            if level_2_string and level_2_list:
                raise KeyError("actual dummy exception")

        if level_1_dict and level_1_string:
            failing_function2("my_arg2")

    with pytest.raises(KeyError) as ex_info:
        failing_function1("my_arg1")

    assert "actual dummy exception" in str(ex_info.value)
    with open(tf.name, "r") as f:
        error_log = f.read()
    assert "'arg1':'my_arg1'" in error_log
    assert "'level_1_string':'string1_my_arg1'" in error_log
    assert "'level_1_dict':{'key1': 'value1'}" in error_log
    assert "'arg2':'my_arg2'" in error_log
    assert "'level_2_string':'string2_my_arg2'" in error_log
    assert "'level_2_list':['level_2_str1','level_2_str2']" in error_log
    os.remove(tf.name)
