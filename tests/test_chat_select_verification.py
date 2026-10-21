"""Selection must return, read back identity, and remain bounded on failure."""
import contextlib
import fcntl
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    'chat_select', Path(__file__).resolve().parents[1] / 'docker/tools/chat-select.py')
select = importlib.util.module_from_spec(spec)
spec.loader.exec_module(select)
PROFILE = select.BUILD_PROFILES['ce28c347']

# The vocabulary the server-side reader agreed to accept. A diagnostic that
# falls outside it is a contract break, not a detail.
AGREED_STAGES = ('startup', 'profile', 'enumeration', 'target', 'ui',
                 'selection', 'verification', 'lock', 'unknown')
AGREED_REASONS = ('not_running', 'build_id_unavailable', 'unsupported_build',
                  'no_sessions', 'unsupported_target', 'target_missing',
                  'ui_unavailable', 'click_failed', 'hook_not_ready',
                  'hook_error', 'selection_unverified', 'pane_unverified',
                  'vector_unavailable', 'selection_busy', 'helper_exception',
                  'helper_timeout', 'unknown')


class SelectionTests(unittest.TestCase):
    def invoke(self, lines, click=None):
        with patch.object(select, 'run_frida_bg', return_value=Mock()) as start, \
             patch.object(select, 'read_lines_until', return_value=lines), \
             patch.object(select, 'kill_frida') as cleanup, \
             patch.object(select.subprocess, 'run', return_value=click or Mock(returncode=0)), \
             patch.object(select, 'log'):
            result = select.select_chat('42', PROFILE, 'wxid_fixture', (100, 100))
            cleanup.assert_called_once_with(start.return_value)
            return result

    def test_redirect_alone_is_not_success(self):
        self.assertIsNone(self.invoke(['REDIRECT']))

    def test_silent_hook_is_not_success(self):
        self.assertIsNone(self.invoke([]))

    def test_verified_index_zero_is_success(self):
        self.assertEqual(self.invoke(['REDIRECT', 'SELECTED_INDEX 0', 'SELECTION_VERIFIED']), 0)

    def test_click_failure_is_not_success(self):
        self.assertIsNone(self.invoke(['SELECTION_VERIFIED'], Mock(returncode=1)))

    def test_click_timeout_still_cleans_up(self):
        with patch.object(select, 'run_frida_bg', return_value=Mock()) as start, \
             patch.object(select, 'kill_frida') as cleanup, \
             patch.object(select.subprocess, 'run', side_effect=subprocess.TimeoutExpired('click', 5)):
            with self.assertRaises(subprocess.TimeoutExpired):
                select.select_chat('42', PROFILE, 'wxid_fixture', (100, 100))
            cleanup.assert_called_once_with(start.return_value)

    def test_script_resolves_live_identity_and_waits_for_native_return(self):
        source = select.selection_script(PROFILE, 'wxid_fixture')
        self.assertIn('manager = args[0]', source)
        self.assertIn('var hmInner = ctrl.add(0xf8).readPointer()', source)
        self.assertIn('selectedIndex = i;', source)
        self.assertIn('if (!returned || controller === null) return', source)
        self.assertIn('current.add(0x98)', source)
        self.assertIn('=== target', source)
        self.assertNotIn('hook.detach()', source)
        self.assertNotIn('this.context.', source)
        self.assertLess(source.index('Interceptor.flush()'), source.index("console.log('READY')"))

    def test_native_map_indices_are_not_renumbered(self):
        lines = ['VECTOR 0x10000 count=4', 'SESSION 0 wxid_first',
                 'SESSION 1 gh_abc123', 'SESSION 3 wxid_last', 'CURRENT_SEL wxid_first']
        with patch.object(select, 'run_frida_script', return_value=lines), patch.object(select, 'log'):
            sessions, _, _, _ = select.enumerate_sessions('42', PROFILE)
        self.assertEqual(sessions, {'wxid_first': 0, 'wxid_last': 3})

    def test_legacy_arm_retains_filtered_indexing(self):
        source = select.selection_script(select.BUILD_PROFILES['3eda8254'], 'wxid_fixture')
        self.assertIn('selectedIndex = filteredIndex;', source)
        self.assertIn('var vectorBegin = ctrl.add(0x0)', source)

    def test_unique_scripts_are_removed(self):
        with select.script_path() as first, select.script_path() as second:
            self.assertNotEqual(first, second)
            self.assertEqual(Path(first).stat().st_mode & 0o777, 0o600)
        self.assertFalse(Path(first).exists())
        self.assertFalse(Path(second).exists())

    def test_silent_process_deadline(self):
        proc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                stdout=subprocess.PIPE, stdin=subprocess.PIPE, text=True)
        try:
            start = time.monotonic()
            self.assertEqual(select.read_lines_until(proc, .1, 'READY'), [])
            self.assertLess(time.monotonic() - start, 1)
        finally:
            select.kill_frida(proc)
            proc.stdout.close()

    def test_missing_ready_cleans_up(self):
        with patch.object(select.subprocess, 'Popen', return_value=Mock()) as start, \
             patch.object(select, 'read_lines_until', return_value=['script error']), \
             patch.object(select, 'kill_frida') as cleanup:
            with self.assertRaises(RuntimeError):
                select.run_frida_bg('42', 'unused.js')
            cleanup.assert_called_once_with(start.return_value)

    def test_already_selected_but_unresponsive_ui_is_failure(self):
        with patch.object(sys, 'argv', ['chat-select', 'wxid_fixture']), \
             patch.object(select, 'get_pid', return_value='42'), \
             patch.object(select, 'get_profile', return_value=(PROFILE, None)), \
             patch.object(select, 'enumerate_sessions', return_value=(
                 {'wxid_fixture': 0}, '0x10000', 1, 'wxid_fixture')), \
             patch.object(select, 'find_chat_item_from_a11y', return_value=None), \
             patch.object(select, 'result_json', side_effect=SystemExit) as result, \
             patch.object(select, 'log'):
            with self.assertRaises(SystemExit):
                select.main()
            self.assertFalse(result.call_args.args[0])

    def test_forced_repeat_is_idempotent_only_with_visible_selection(self):
        for visible in (True, False):
            with self.subTest(visible=visible), \
                 patch.object(sys, 'argv', ['chat-select', '--force', 'wxid_fixture']), \
                 patch.object(select, 'get_pid', return_value='42'), \
                 patch.object(select, 'get_profile', return_value=(PROFILE, None)), \
                 patch.object(select, 'enumerate_sessions', return_value=(
                     {'wxid_fixture': 0}, '0x10000', 1, 'wxid_fixture')), \
                 patch.object(select, 'find_chat_item_from_a11y',
                              side_effect=lambda require_open_chat=False, **_kw:
                                  (100, 100) if not require_open_chat or visible else None), \
                 patch.object(select, 'select_chat', return_value=None) as click, \
                 patch.object(select, 'result_json', side_effect=SystemExit) as result, \
                 patch.object(select, 'log'):
                with self.assertRaises(SystemExit):
                    select.main()
                if visible:
                    click.assert_not_called()
                    self.assertTrue(result.call_args.kwargs['skipped'])
                    self.assertTrue(result.call_args.args[0])
                else:
                    click.assert_called_once()
                    self.assertFalse(result.call_args.args[0])

    def test_selected_item_can_be_after_first_row(self):
        items = []
        tree = {'role': 'list', 'name': 'Chats',
                'bounds': {'x': 61, 'y': 81, 'width': 240, 'height': 696},
                'children': [
            {'role': 'list-item', 'bounds': {'x': 61, 'y': 81, 'width': 240, 'height': 65}},
            {'role': 'list-item', 'bounds': {'x': 61, 'y': 146, 'width': 240, 'height': 65},
             'states': ['SELECTED']}]}
        select._find_chat_list_items(tree, items, False)
        self.assertEqual(len(items), 2)
        self.assertIn('SELECTED', items[1]['states'])

    def test_offscreen_chat_has_open_pane_without_selected_row(self):
        tree = {'children': [
            {'role': 'list', 'name': 'Messages', 'bounds': {'width': 500, 'height': 400}},
            {'role': 'push-button', 'name': 'Send', 'bounds': {'width': 50, 'height': 25}}]}
        self.assertTrue(select.has_open_chat_pane(tree))
        tree['children'].pop()
        self.assertFalse(select.has_open_chat_pane(tree))


class ViewportGeometryTests(unittest.TestCase):
    """Clicks must land inside the visible part of the Chats list.

    The live read-only tree measured Chats at x61 y81 w240 h696 while its first
    row reported x61 y16 w240 h65: the row's own bounds lie entirely above the
    viewport, so its centre is not a reachable pixel. The old code returned that
    centre, (181, 48), which is inside the Search field, not the chat list.
    """

    CHATS = {'x': 61, 'y': 81, 'width': 240, 'height': 696}

    # The live tree also showed an open search dropdown at x73 y72 w320 h64, in a
    # sibling subtree of the frame holding Chats. This reader does not resolve
    # overlays: it only refuses points outside the visible list.
    SEARCH_DROPDOWN = {'role': 'list', 'name': 'Suggestions',
                       'bounds': {'x': 73, 'y': 72, 'width': 320, 'height': 64}}

    def tree(self, rows, chats_bounds='default', dropdown=False, pane=False):
        bounds = self.CHATS if chats_bounds == 'default' else chats_bounds
        chats = {'role': 'list', 'name': 'Chats', 'children': [
            {'role': 'list-item', 'bounds': dict(r)} for r in rows]}
        if bounds is not None:
            chats['bounds'] = dict(bounds)
        children = [dict(self.SEARCH_DROPDOWN)] if dropdown else []
        if pane:
            children += [
                {'role': 'list', 'name': 'Messages',
                 'bounds': {'x': 301, 'y': 81, 'width': 500, 'height': 400}},
                {'role': 'push-button', 'name': 'Send',
                 'bounds': {'x': 700, 'y': 700, 'width': 50, 'height': 25}}]
        return {'role': 'application', 'children': children + [chats]}

    SCROLLED_ROWS = (
        {'x': 61, 'y': 16, 'width': 240, 'height': 65},   # fully above y81
        {'x': 61, 'y': 146, 'width': 240, 'height': 65},  # visible
        {'x': 61, 'y': 211, 'width': 240, 'height': 65},  # visible
    )

    # The first visible row spans pixels x61..300, y146..210: x301 and y211 are
    # the far edges, which no pixel occupies. The fallback centre is therefore
    # (180, 178), not (181, 178).
    FALLBACK = (180, 178)

    def visible_center(self, rows, chats_bounds='default', preferred=None, **kwargs):
        """Run the a11y reader against a tree and return its click point."""
        dump = Mock(returncode=0, stderr='',
                    stdout=json.dumps(self.tree(rows, chats_bounds, **kwargs)))
        with patch.object(select.subprocess, 'run', return_value=dump), \
             patch.object(select, 'log'):
            return select.find_chat_item_from_a11y(preferred_click=preferred)

    def test_a_row_entirely_above_the_viewport_is_not_offered(self):
        # The old centre of the first row, (181, 48), is inside the Search field.
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS), self.FALLBACK)

    def test_the_click_point_is_inside_the_visible_chats_rectangle(self):
        x, y = self.visible_center(self.SCROLLED_ROWS)
        chats = self.CHATS
        self.assertTrue(chats['x'] <= x < chats['x'] + chats['width'])
        self.assertTrue(chats['y'] <= y < chats['y'] + chats['height'])

    def test_a_row_partially_visible_clicks_inside_the_intersection(self):
        rows = ({'x': 61, 'y': 146, 'width': 240, 'height': 65},
                {'x': 61, 'y': 720, 'width': 240, 'height': 130})  # clipped at 777
        x, y = self.visible_center(rows)
        self.assertTrue(81 <= y < 777, 'a clipped row must not click below the viewport')
        self.assertTrue(146 <= y < 211)

    def test_rows_entirely_outside_the_viewport_are_rejected(self):
        rows = ({'x': 61, 'y': 900, 'width': 240, 'height': 65},   # below
                {'x': 61, 'y': -200, 'width': 240, 'height': 65})  # above
        self.assertIsNone(self.visible_center(rows))

    def test_zero_negative_and_non_finite_rows_are_rejected(self):
        for bad in ({'x': 61, 'y': 146, 'width': 0, 'height': 65},
                    {'x': 61, 'y': 146, 'width': 240, 'height': -65},
                    {'x': 61, 'y': float('nan'), 'width': 240, 'height': 65},
                    {'x': 61, 'y': float('inf'), 'width': 240, 'height': 65},
                    {'x': 61, 'y': 146, 'width': float('inf'), 'height': 65}):
            with self.subTest(bounds=bad):
                self.assertIsNone(self.visible_center([bad]))

    def test_a_chats_list_without_bounds_fails_closed(self):
        for bounds in (None, {}, {'x': 61, 'y': 81},
                       {'x': 61, 'y': 81, 'width': 0, 'height': 696}):
            with self.subTest(bounds=bounds):
                self.assertIsNone(self.visible_center(self.SCROLLED_ROWS, bounds))

    def test_a_stale_click_point_outside_the_viewport_is_not_used(self):
        # (181, 48) is what the old code computed for the scrolled first row.
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS, preferred=(181, 48)),
                         self.FALLBACK)

    def test_a_supplied_click_point_inside_a_visible_row_is_honoured(self):
        # A point off the centre proves the request, not the centre, was used.
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS, preferred=(181, 178)),
                         (181, 178))
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS, preferred=(200, 200)),
                         (200, 200))

    def test_a_supplied_point_in_a_gap_or_outside_every_row_falls_back(self):
        # 110 is the empty band between the first visible row (146..211) and the
        # viewport top (81); the rest are outside the list horizontally.
        for point in ((60, 178), (302, 178), (181, 80), (181, 110)):
            with self.subTest(point=point):
                self.assertEqual(
                    self.visible_center(self.SCROLLED_ROWS, preferred=point),
                    self.FALLBACK)

    def test_a_point_below_the_viewport_or_above_it_is_refused(self):
        for point in ((181, 800), (181, -5), (181, float('nan')), (float('inf'), 178)):
            with self.subTest(point=point):
                self.assertEqual(
                    self.visible_center(self.SCROLLED_ROWS, preferred=point),
                    self.FALLBACK)

    def test_a_fractional_row_without_an_whole_pixel_is_rejected(self):
        # x=61.8..61.9 spans no whole pixel, so it offers nothing to click.
        rows = ({'x': 61.8, 'y': 146, 'width': 0.1, 'height': 65},
                {'x': 61, 'y': 146, 'width': 240, 'height': 65})
        x, y = self.visible_center(rows)
        self.assertEqual((x, y), self.FALLBACK)

    def test_a_row_whose_only_pixel_is_outside_the_viewport_is_rejected(self):
        # The row's whole area sits inside the viewport mathematically, but its
        # fractional edges leave no pixel to click.
        rows = ({'x': 61.5, 'y': 146, 'width': 0.4, 'height': 65},)
        self.assertIsNone(self.visible_center(rows))

    def test_the_click_point_is_always_a_pixel_inside_the_row(self):
        for rows in (
                ({'x': 61.5, 'y': 146, 'width': 3, 'height': 65},),
                ({'x': 61, 'y': 146.6, 'width': 240, 'height': 65},),
                ({'x': 60, 'y': 146, 'width': 242.5, 'height': 65},),
                ({'x': 61, 'y': 146, 'width': 0.5, 'height': 65},)):
            with self.subTest(rows=rows):
                point = self.visible_center(rows)
                self.assertIsNotNone(point)
                x, y = point
                rect = self.CHATS
                self.assertTrue(rect['x'] <= x < rect['x'] + rect['width'])
                self.assertTrue(rect['y'] <= y < rect['y'] + rect['height'])
                row = rows[0]
                self.assertTrue(row['x'] <= x <= row['x'] + row['width'], (x, row))
                self.assertTrue(row['y'] <= y <= row['y'] + row['height'], (y, row))

    def test_a_narrow_row_clips_to_the_pixel_it_still_covers(self):
        rows = ({'x': 61.5, 'y': 146, 'width': 2, 'height': 65},)
        x, y = self.visible_center(rows)
        self.assertTrue(61.5 <= x <= 63.5, x)
        self.assertTrue(146 <= y < 211, y)

    def test_bounds_that_overflow_to_infinity_are_rejected(self):
        for bad in ({'x': 1e308, 'y': 146, 'width': 1e308, 'height': 65},
                    {'x': 61, 'y': 1e308, 'width': 240, 'height': 1e308}):
            with self.subTest(bounds=bad):
                self.assertIsNone(self.visible_center([bad]))
        for bad in ({'x': 1e308, 'y': 81, 'width': 1e308, 'height': 696},):
            with self.subTest(chats=bad):
                self.assertIsNone(self.visible_center(self.SCROLLED_ROWS, bad))

    def test_a_preferred_point_is_rechecked_after_being_quantised(self):
        # 61.9 truncates to 61, which is left of this row's only pixel.
        rows = ({'x': 61.5, 'y': 146, 'width': 2, 'height': 65},)
        self.assertEqual(self.visible_center(rows, preferred=(61.9, 178)),
                         self.visible_center(rows))
        # 62.5 truncates to 62, which is a pixel this row covers.
        self.assertEqual(self.visible_center(rows, preferred=(62.5, 178)), (62, 178))

    def test_a_fractional_preferred_point_inside_a_row_is_honoured(self):
        # Truncation keeps these inside the row they were asked for, and the
        # point is returned as given rather than replaced by the centre.
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS, preferred=(181.6, 178.4)),
                         (181, 178))
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS, preferred=(200.9, 200.2)),
                         (200, 200))

    def test_a_search_dropdown_over_a_row_does_not_move_the_click_point(self):
        # The dropdown covers y72..136 and would cover the first visible row's
        # centre. Resolving that overlay is out of scope, so this test records
        # the boundary rather than claiming the point is reachable.
        self.assertEqual(self.visible_center(self.SCROLLED_ROWS, dropdown=True),
                         self.FALLBACK)
        in_dropdown = (181, 113)
        self.assertTrue(73 <= in_dropdown[0] < 393 and 72 <= in_dropdown[1] < 136,
                        'the dropdown may still cover a visible row centre')

    def run_main_with_tree(self, argv, **kwargs):
        """Run main() against the fixture tree, returning (coords, ok result)."""
        seen = {}

        def capture(pid, profile, target, click_coords):
            seen['coords'] = click_coords
            return 0

        dump = Mock(returncode=0, stderr='',
                    stdout=json.dumps(self.tree(self.SCROLLED_ROWS, pane=True, **kwargs)))
        results = []
        with patch.object(sys, 'argv', ['chat-select'] + list(argv)), \
             patch.object(select, 'get_pid', return_value='42'), \
             patch.object(select, 'get_profile', return_value=(PROFILE, None)), \
             patch.object(select, 'enumerate_sessions', return_value=(
                 {'wxid_fixture': 0}, '0x10000', 1, None)), \
             patch.object(select.subprocess, 'run', return_value=dump), \
             patch.object(select, 'select_chat', side_effect=capture), \
             patch.object(select, 'result_json', side_effect=SystemExit) as result, \
             patch.object(select, 'log'):
            with self.assertRaises(SystemExit):
                select.main()
            results.append(result.call_args)
        return seen['coords'], results[0]

    def test_main_does_not_trust_a_stale_click_point_over_the_live_tree(self):
        coords, result = self.run_main_with_tree(
            ['--click-xy', '181', '48', 'wxid_fixture'])
        self.assertEqual(coords, self.FALLBACK)
        self.assertTrue(result.args[0])

    def test_main_honours_a_click_point_that_the_current_tree_confirms(self):
        coords, result = self.run_main_with_tree(
            ['--click-xy', '181', '178', 'wxid_fixture'])
        # Honoured as asked, which is not the fallback centre.
        self.assertEqual(coords, (181, 178))
        self.assertTrue(result.args[0])

    def test_main_reads_the_tree_once_per_run(self):
        dump = Mock(returncode=0, stderr='',
                    stdout=json.dumps(self.tree(self.SCROLLED_ROWS, pane=True)))
        with patch.object(select.subprocess, 'run', return_value=dump) as run, \
             patch.object(select, 'log'):
            select.find_chat_item_from_a11y(preferred_click=self.FALLBACK)
        self.assertEqual(run.call_count, 1)

    def test_a_tree_without_a_visible_row_reports_no_click_point(self):
        dump = Mock(returncode=0, stdout=json.dumps(
            {'role': 'application', 'children': [
                {'role': 'list', 'name': 'Messages',
                 'bounds': {'x': 301, 'y': 81, 'width': 500, 'height': 400}},
                {'role': 'push-button', 'name': 'Send',
                 'bounds': {'x': 700, 'y': 700, 'width': 50, 'height': 25}}]}), stderr='')
        with patch.object(select.subprocess, 'run', return_value=dump), \
             patch.object(select, 'log'):
            self.assertIsNone(select.find_chat_item_from_a11y())
            self.assertIsNone(select.find_chat_item_from_a11y(preferred_click=(10, 10)))

    def test_a_dump_failure_still_reports_no_click_point(self):
        for dump in (Mock(returncode=1, stdout='', stderr='boom'),
                     Mock(returncode=0, stdout='not json', stderr='')):
            with self.subTest(dump=dump.returncode), \
                 patch.object(select.subprocess, 'run', return_value=dump), \
                 patch.object(select, 'log'):
                self.assertIsNone(select.find_chat_item_from_a11y(
                    preferred_click=self.FALLBACK))


class DiagnosticTests(unittest.TestCase):
    """Failures must name a stage and reason from the agreed vocabulary only."""

    SCRIPT = Path(__file__).resolve().parents[1] / 'docker/tools/chat-select.py'

    def setUp(self):
        select.clear_diagnostic()

    def logged_in(self, sessions=None, vector_base='0x10000', current_sel=None):
        sessions = {'wxid_fixture': 0} if sessions is None else sessions
        return (
            patch.object(select, 'get_pid', return_value='42'),
            patch.object(select, 'get_profile', return_value=(PROFILE, None)),
            patch.object(select, 'enumerate_sessions', return_value=(
                sessions, vector_base, len(sessions), current_sel)),
        )

    def run_main(self, argv, *patches):
        """Drive main() with fakes, returning (parsed stdout, exit code)."""
        out = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(sys, 'argv', ['chat-select'] + list(argv)))
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(patch.object(select, 'log'))
            for item in patches:
                stack.enter_context(item)
            with self.assertRaises(SystemExit) as exited:
                select.main()
        return json.loads(out.getvalue()), exited.exception.code

    def assert_diagnostic(self, expected):
        self.assertEqual(select.diagnostic(),
                         {'stage': expected[0], 'reason': expected[1]})

    def test_agrees_with_the_server_side_vocabulary(self):
        self.assertEqual(select.DIAGNOSTIC_STAGES, set(AGREED_STAGES))
        self.assertEqual(select.DIAGNOSTIC_REASONS, set(AGREED_REASONS))

    def test_a_failure_reports_stage_and_reason_and_exits_nonzero(self):
        payload, code = self.run_main(
            ['--current'], patch.object(select, 'get_pid', return_value=None))
        self.assertFalse(payload['ok'])
        self.assertEqual(code, 1)
        self.assertEqual(payload['diagnostic'],
                         {'stage': 'startup', 'reason': 'not_running'})

    def test_a_success_carries_no_diagnostic(self):
        payload, code = self.run_main(['--current'], *self.logged_in(current_sel='wxid_fixture'))
        self.assertTrue(payload['ok'])
        self.assertEqual(code, 0)
        self.assertNotIn('diagnostic', payload)

    def test_a_used_diagnostic_is_never_echoed_on_success(self):
        select.set_diagnostic('ui', 'ui_unavailable')
        payload, code = self.run_main(['--current'], *self.logged_in(current_sel='wxid_fixture'))
        self.assertTrue(payload['ok'])
        self.assertNotIn('diagnostic', payload)
        self.assertEqual(code, 0)

    def test_a_stale_diagnostic_does_not_leak_into_a_later_failure(self):
        select.set_diagnostic('ui', 'ui_unavailable')
        payload, _ = self.run_main(
            ['--current'], patch.object(select, 'get_pid', return_value=None))
        self.assertEqual(payload['diagnostic'],
                         {'stage': 'startup', 'reason': 'not_running'})

    def test_argument_misuse_is_reported_as_an_unclassified_startup_failure(self):
        for argv in ([], ['--current', 'wxid_fixture'], ['--click-xy', '5']):
            with self.subTest(argv=argv):
                payload, code = self.run_main(argv, *self.logged_in())
                self.assertEqual(payload['diagnostic'],
                                 {'stage': 'startup', 'reason': 'unknown'})
                self.assertEqual(code, 1)

    def test_build_profile_failures_distinguish_absent_from_unsupported(self):
        for build_id, reason in ((None, 'build_id_unavailable'),
                                 ('deadbeef' + '0' * 56, 'unsupported_build')):
            with self.subTest(build_id=build_id):
                payload, code = self.run_main(
                    ['--current'], patch.object(select, 'get_pid', return_value='42'),
                    patch.object(select, 'get_build_id', return_value=build_id))
                self.assertEqual(payload['diagnostic'],
                                 {'stage': 'profile', 'reason': reason})
                self.assertEqual(code, 1)

    def test_main_stage_table(self):
        cases = (
            ('no_sessions', ['--current'], self.logged_in(sessions={}),
             {'stage': 'enumeration', 'reason': 'no_sessions'}),
            ('official_account', ['gh_abc123'], self.logged_in(),
             {'stage': 'target', 'reason': 'unsupported_target'}),
            ('unknown_target', ['wxid_absent'], self.logged_in(),
             {'stage': 'target', 'reason': 'target_missing'}),
            ('no_chat_list', ['wxid_fixture'], self.logged_in() + (
                patch.object(select, 'find_chat_item_from_a11y', return_value=None),),
             {'stage': 'ui', 'reason': 'ui_unavailable'}),
            ('vector_base', ['wxid_fixture'], self.logged_in(vector_base=None) + (
                patch.object(select, 'find_chat_item_from_a11y', return_value=(100, 100)),),
             {'stage': 'enumeration', 'reason': 'vector_unavailable'}),
        )
        for name, argv, patches, expected in cases:
            with self.subTest(case=name):
                payload, code = self.run_main(argv, *patches)
                self.assertFalse(payload['ok'])
                self.assertEqual(payload['diagnostic'], expected)
                self.assertEqual(code, 1)

    def select_chat_failure(self, lines=(), click=None, side_effect=None, bg=None):
        """Run select_chat with fakes, returning its return value or the exception."""
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(
                select, 'run_frida_bg', **({'side_effect': bg} if bg else {'return_value': Mock()})))
            stack.enter_context(patch.object(select, 'read_lines_until', return_value=list(lines)))
            stack.enter_context(patch.object(select, 'kill_frida'))
            stack.enter_context(patch.object(select, 'log'))
            stack.enter_context(patch.object(
                select.subprocess, 'run', side_effect=side_effect,
                return_value=click if click is not None else Mock(returncode=0)))
            try:
                return select.select_chat('42', PROFILE, 'wxid_fixture', (100, 100))
            except Exception as exc:  # the helper classifies, it does not swallow
                return exc

    def test_select_chat_failure_table(self):
        cases = (
            ('click_failed', {'click': Mock(returncode=1)},
             ('selection', 'click_failed')),
            ('silent_after_ready', {'lines': ['READY']},
             ('selection', 'selection_unverified')),
            ('silent_hook', {'lines': []},
             ('selection', 'selection_unverified')),
            ('hook_error', {'lines': ['READY', 'REDIRECT', 'SELECTION_ERROR invalid live vector']},
             ('selection', 'hook_error')),
            ('hook_error_before_ready', {'lines': ['SELECTION_ERROR invalid live vector']},
             ('selection', 'hook_error')),
            ('selection_unverified', {'lines': ['READY', 'REDIRECT']},
             ('selection', 'selection_unverified')),
            ('hook_not_ready_error', {'bg': RuntimeError('Selection hook did not become ready')},
             ('selection', 'hook_not_ready')),
        )
        for name, kwargs, expected in cases:
            with self.subTest(case=name):
                outcome = self.select_chat_failure(**kwargs)
                if name == 'hook_not_ready_error':
                    self.assertIsInstance(outcome, RuntimeError)
                else:
                    self.assertIsNone(outcome)
                self.assert_diagnostic(expected)

    def test_a_silent_hook_after_ready_is_not_reported_as_not_ready(self):
        # run_frida_bg only returns once READY arrived, so an empty line list
        # proves no event happened, never that the hook was unready.
        outcome = self.select_chat_failure(lines=['READY'])
        self.assertIsNone(outcome)
        self.assert_diagnostic(('selection', 'selection_unverified'))

    def test_a_click_timeout_is_classified_and_still_propagates(self):
        outcome = self.select_chat_failure(
            side_effect=subprocess.TimeoutExpired('click', 5))
        self.assertIsInstance(outcome, subprocess.TimeoutExpired)
        self.assert_diagnostic(('selection', 'helper_timeout'))

    def test_a_verified_selection_with_a_closed_pane_is_a_verification_failure(self):
        def visible(require_open_chat=False, **_kw):
            return None if require_open_chat else (100, 100)

        payload, code = self.run_main(
            ['wxid_fixture'], *self.logged_in(), patch.object(
                select, 'find_chat_item_from_a11y', side_effect=visible),
            patch.object(select, 'select_chat', return_value=0))
        self.assertEqual(payload['diagnostic'],
                         {'stage': 'verification', 'reason': 'pane_unverified'})
        self.assertEqual(code, 1)

    def test_an_unclassified_selection_failure_is_not_reported_as_success(self):
        payload, code = self.run_main(
            ['wxid_fixture'], *self.logged_in(), patch.object(
                select, 'find_chat_item_from_a11y', return_value=(100, 100)),
            patch.object(select, 'select_chat', return_value=None))
        self.assertEqual(payload['diagnostic'],
                         {'stage': 'selection', 'reason': 'selection_unverified'})
        self.assertEqual(code, 1)

    def test_helper_failures_are_classified_by_type_never_by_message(self):
        cases = (
            (subprocess.TimeoutExpired('frida', 45), 'helper_timeout'),
            (RuntimeError('boom'), 'helper_exception'),
            (ValueError('bad target\nwxid_fixture'), 'helper_exception'),
            (BlockingIOError(11, 'Resource temporarily unavailable'), 'helper_exception'),
        )
        for exc, reason in cases:
            with self.subTest(exc=type(exc).__name__):
                self.assertEqual(select.failure_diagnostic(exc),
                                 ('unknown', reason))

    def test_a_diagnostic_carries_no_chat_identity(self):
        payload, _ = self.run_main(
            ['wxid_secret_fixture'], *self.logged_in())
        self.assertIn('wxid_secret_fixture', payload['error'])  # error keeps its text
        self.assertEqual(set(payload['diagnostic']), {'stage', 'reason'})
        for value in payload['diagnostic'].values():
            self.assertTrue(value.isascii() and value.replace('_', '').isalnum())
            self.assertNotIn('wxid', value)

    def test_an_off_vocabulary_stage_or_reason_collapses_to_unknown(self):
        for stage, reason in (('selection ', 'click_failed'), ('click', 'not running'),
                              (None, None), ('UI', 'UI_UNAVAILABLE')):
            with self.subTest(stage=stage, reason=reason):
                select.set_diagnostic(stage, reason)
                self.assert_diagnostic(('unknown', 'unknown'))

    def test_contending_for_the_selection_lock_is_reported_as_busy(self):
        lock_path = Path(tempfile.gettempdir()) / 'wechat-chat-select.lock'
        with open(lock_path, 'a') as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                done = subprocess.run([sys.executable, str(self.SCRIPT), '--current'],
                                      capture_output=True, text=True, timeout=30)
            finally:
                fcntl.flock(held, fcntl.LOCK_UN)
        payload = json.loads(done.stdout)
        self.assertEqual(done.returncode, 1)
        self.assertEqual(payload['diagnostic'],
                         {'stage': 'lock', 'reason': 'selection_busy'})
        self.assertEqual(payload['error'], 'Another chat selection is in progress')


if __name__ == '__main__':
    unittest.main()
