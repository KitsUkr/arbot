from arbot.normalize import normalize_title


def test_lowercases_and_drops_punct():
    assert normalize_title("Will Bitcoin Hit $100k?") == "bitcoin hit 100k"


def test_strips_diacritics():
    assert normalize_title("Élection française?") == "election francaise"


def test_drops_stopwords():
    assert "the" not in normalize_title("Will the President resign in 2025").split()


def test_empty_inputs():
    assert normalize_title("") == ""
    assert normalize_title("   ") == ""
    assert normalize_title("???") == ""


def test_collapses_whitespace():
    assert normalize_title("Bitcoin    100k") == "bitcoin 100k"
