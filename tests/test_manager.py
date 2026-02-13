from distill import Manager

from .conftest import COMPONENTS_DIR


class TestManagerLoad:
    def test_load_components(self):
        m = Manager(COMPONENTS_DIR)
        infos = m.load_components()
        assert len(infos) == 1
        assert infos[0].name == "filter-pushdown"

    def test_load_empty_dir(self, tmp_path):
        m = Manager(tmp_path)
        infos = m.load_components()
        assert infos == []
