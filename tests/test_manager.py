from distill import Manager

from .conftest import COMPONENTS_DIR


class TestManagerLoad:
    def test_load_components(self):
        m = Manager(COMPONENTS_DIR)
        infos = m.load_components()
        assert len(infos) == 2
        names = {info.name for info in infos}
        assert "filter-pushdown" in names
        assert "predicate-simplification" in names

    def test_load_empty_dir(self, tmp_path):
        m = Manager(tmp_path)
        infos = m.load_components()
        assert infos == []
