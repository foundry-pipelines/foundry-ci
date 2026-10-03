import pytest
from conftest import load
from pipeline import ConfigError

typecheck = load("validators/processors/typecheck.py")

POM = """<project xmlns="http://maven.apache.org/POM/4.0.0"><build><plugins>
  <plugin><artifactId>maven-shade-plugin</artifactId></plugin>
  <plugin><artifactId>jsonschema-maven-plugin</artifactId>
    <configuration><classNames>{}</classNames></configuration></plugin>
</plugins></build></project>"""


def pom(tmp_path, *classes):
    path = tmp_path / "pom.xml"
    path.write_text(POM.format("".join(f"<className>{c}</className>" for c in classes)))
    return path


def test_out_types_must_be_own_classes():
    decl = {"in": ["Packets"], "out": ["DecodeEnrichment"]}
    assert typecheck.check(decl, {"DecodeEnrichment"}, {"Packets"}) == []
    assert typecheck.check(decl, set(), {"Packets", "DecodeEnrichment"}) == [
        "operator.yaml out: 'DecodeEnrichment' is not a class of this project"]


def test_in_types_own_or_registered():
    decl = {"in": ["XmlPackets", "Packets"], "out": ["Packets"]}
    assert typecheck.check(decl, {"XmlPackets", "Packets"}, set()) == []
    decl = {"in": ["Mystery"], "out": ["Packets"]}
    assert typecheck.check(decl, {"Packets"}, {"Packets"}) == [
        "operator.yaml in: 'Mystery' is neither a class of this project nor in schemaRegistry"]


def test_pom_matches_out_types(tmp_path):
    decl = {"out": ["EnrichedPackets"]}
    assert typecheck.check_pom(decl, pom(tmp_path, "foundry.join.EnrichedPackets")) == []


def test_pom_missing_and_extra_classes(tmp_path):
    decl = {"out": ["EnrichedPackets"]}
    assert typecheck.check_pom(decl, pom(tmp_path, "foundry.join.Piece")) == [
        "pom.xml: jsonschema-maven-plugin classNames lack out type 'EnrichedPackets'",
        "pom.xml: jsonschema-maven-plugin classNames has 'Piece', not an out type in operator.yaml"]


def test_pom_without_plugin(tmp_path):
    (tmp_path / "pom.xml").write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"/>')
    assert typecheck.check_pom({"out": ["X"]}, tmp_path / "pom.xml") == [
        "pom.xml: no jsonschema-maven-plugin, so out types get no schemas"]


def test_no_pom_is_fine(tmp_path):
    assert typecheck.check_pom({"out": ["X"]}, tmp_path / "pom.xml") == []


def test_bad_pom(tmp_path):
    (tmp_path / "pom.xml").write_text("<project>")
    with pytest.raises(ConfigError):
        typecheck.check_pom({"out": ["X"]}, tmp_path / "pom.xml")


def test_declared_classes_reads_java_and_skips_target(tmp_path):
    from pipeline import declared_classes
    (tmp_path / "src").mkdir()
    (tmp_path / "src/EnrichedPackets.java").write_text("public record EnrichedPackets(String guid) {}")
    (tmp_path / "target").mkdir()
    (tmp_path / "target/Gen.java").write_text("public class Gen {}")
    assert declared_classes(tmp_path) == {"EnrichedPackets"}
