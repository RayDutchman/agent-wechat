#!/usr/bin/env python3
"""Pure tests for a11y-dump's derived text states.

These tests never import the real ``gi``/Atspi stack and never need a display:
they install a fake ``gi`` module and exercise ``get_states`` with synthetic
accessibility nodes. The only thing under test is the derived
``TEXT_EMPTY``/``TEXT_NONEMPTY`` state that lets a caller tell an editable
field's *contents* apart from its label (``name``).

Run: python3 docker/tools/test_a11y_dump.py
"""

import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _install_fake_gi():
    """A minimal stand-in so the helper can be imported without Atspi."""
    gi = types.ModuleType("gi")
    gi.require_version = lambda *args, **kwargs: None
    repository = types.ModuleType("gi.repository")
    atspi = types.ModuleType("gi.repository.Atspi")
    atspi.CoordType = types.SimpleNamespace(SCREEN=0)
    repository.Atspi = atspi
    gi.repository = repository
    sys.modules["gi"] = gi
    sys.modules["gi.repository"] = repository
    sys.modules["gi.repository.Atspi"] = atspi


def _load_helper():
    _install_fake_gi()
    path = Path(__file__).with_name("a11y-dump")
    import importlib.machinery

    loader = importlib.machinery.SourceFileLoader("a11y_dump_under_test", str(path))
    spec = importlib.util.spec_from_loader("a11y_dump_under_test", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


class _FakeState:
    def __init__(self, value_name):
        self.value_name = value_name


def _atspi_state(name):
    return _FakeState(f"ATSPI_STATE_{name}")


class _FakeStateSet:
    def __init__(self, names):
        self._states = [_atspi_state(name) for name in names]

    def get_states(self):
        return list(self._states)


class _FakeText:
    def __init__(self, count):
        self._count = count

    def get_character_count(self):
        if isinstance(self._count, BaseException):
            raise self._count
        return self._count


class _FakeNode:
    def __init__(self, role, name, state_names, text):
        self._role = role
        self._name = name
        self._states = _FakeStateSet(state_names)
        self._text = text

    def get_role_name(self):
        return self._role

    def get_name(self):
        return self._name

    def get_state_set(self):
        return self._states

    def get_text_iface(self):
        if isinstance(self._text, BaseException):
            raise self._text
        return self._text


class DerivedTextStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = _load_helper()

    def states(self, role, name, state_names, text):
        return self.mod.get_states(_FakeNode(role, name, state_names, text))

    def assertNoTextState(self, states):
        states = states or []
        self.assertNotIn("TEXT_EMPTY", states)
        self.assertNotIn("TEXT_NONEMPTY", states)

    # ── The decision the fenced file baseline needs ─────────────────────────

    def test_a_label_with_truly_empty_text_is_text_empty(self):
        # The accessible name is the field's label, not its contents: a
        # non-empty label must not hide an empty field.
        states = self.states("text", "Chats", ["EDITABLE"], _FakeText(0))
        self.assertIn("TEXT_EMPTY", states)

    def test_a_label_with_nonempty_text_is_text_nonempty(self):
        states = self.states("text", "", ["EDITABLE"], _FakeText(5))
        self.assertIn("TEXT_NONEMPTY", states)
        self.assertNotIn("TEXT_EMPTY", states)

    # ── Fail closed: no answer means no derived state ───────────────────────

    def test_a_missing_text_interface_is_not_marked(self):
        self.assertNoTextState(self.states("text", "", ["EDITABLE"], None))

    def test_a_raising_text_interface_is_not_marked(self):
        self.assertNoTextState(
            self.states("text", "", ["EDITABLE"], RuntimeError("no text iface"))
        )

    def test_a_negative_character_count_is_not_marked(self):
        self.assertNoTextState(self.states("text", "", ["EDITABLE"], _FakeText(-1)))

    def test_a_non_editable_text_is_not_marked(self):
        self.assertNoTextState(self.states("text", "", [], _FakeText(0)))

    def test_a_non_text_role_is_not_marked(self):
        self.assertNoTextState(self.states("label", "", ["EDITABLE"], _FakeText(0)))

    # ── The bare function, so the contract is pinned directly ───────────────

    def test_get_text_state_reports_only_the_derived_names(self):
        get_text_state = self.mod.get_text_state
        self.assertEqual(get_text_state(_FakeNode("text", "", ["EDITABLE"], _FakeText(0))), "TEXT_EMPTY")
        self.assertEqual(get_text_state(_FakeNode("text", "", ["EDITABLE"], _FakeText(3))), "TEXT_NONEMPTY")
        self.assertIsNone(get_text_state(_FakeNode("text", "", ["EDITABLE"], None)))
        self.assertIsNone(get_text_state(_FakeNode("text", "", ["EDITABLE"], RuntimeError("x"))))
        self.assertIsNone(get_text_state(_FakeNode("text", "", ["EDITABLE"], _FakeText(-2))))


if __name__ == "__main__":
    unittest.main(verbosity=2)
