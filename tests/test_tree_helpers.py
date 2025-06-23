import utils.tree_helpers as th


def test_flatten_list_basic():
    assert th.flatten_list(["a", ["b"], "c"]) == ["a", "b", "c"]


def test_flatten_list_single():
    assert th.flatten_list([["a"]]) == "a"


def test_flatten_with_parens():
    data = ["(", ["foo", ["(", "bar", ")"], "baz"], ")"]
    assert th.flatten_with_parens(data) == ["(", "foo", "(", "bar", ")", "baz", ")"]


def test_ctx_flatten_normalizes():
    def dummy_extract(ctx):
        return ["lower ( userRole )"]

    assert th.ctx_flatten(None, dummy_extract) == ["lower( userRole )"]
