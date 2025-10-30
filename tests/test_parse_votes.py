import pathlib

from scraper import parse_votes


def test_parse_votes_real_fixture():
    fixture = pathlib.Path(__file__).parent / "fixtures" / "sample_minutes.html"
    html = fixture.read_text(encoding="utf-8")
    motions = parse_votes(html)
    assert isinstance(motions, list)
    assert len(motions) == 1
    m = motions[0]
    assert m["title"] == "Lansdowne – Council Change of Date"
    assert m["result"].startswith("Lost")

    expected_for = [
        "T. Kavanagh",
        "R. King",
        "J. Leiper",
        "R. Brockington",
        "S. Menard",
        "L. Johnson",
        "S. Devine",
        "J. Bradley",
        "S. Plante",
        "A. Troster",
        "M. Carr",
        "W. Lo",
    ]

    expected_against = [
        "M. Luloff",
        "L. Dudas",
        "G. Gower",
        "T. Tierney",
        "A. Hubley",
        "C. Curry",
        "D. Hill",
        "C. Kelly",
        "D. Brown",
        "M. Sutcliffe",
        "I. Skalski",
    ]

    assert [n.strip() for n in m["for_names"]] == expected_for
    assert [n.strip() for n in m["against_names"]] == expected_against

