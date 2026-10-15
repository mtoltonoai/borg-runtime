"""Relocation must not turn byte-identical historical receipts into current evidence."""
from pathlib import Path
import shutil
import tempfile
import unittest

import spec_set


class MigrationRoutingTests(unittest.TestCase):
    def bundle(self, directory):
        directory.mkdir(parents=True)
        (directory / 'implementation.md').write_text(
            '[Implementation Capability Specification](cap.md)\n'
            '[Phased Implementation Specification](phase.md)\n')
        (directory / 'cap.md').write_text('No approved declarations.\n')
        (directory / 'phase.md').write_text('Phase source.\n')
        return spec_set.load(directory / 'implementation.md')

    def test_byte_identical_relocated_bundle_is_not_the_report_subject(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = self.bundle(root / 'old/spec')
            new = self.bundle(root / 'v1/spec')
            self.assertEqual(old['digest'], new['digest'])
            metrics = {'spec_set': old, 'source': old['paths']['capabilities'],
                       'planning_tools': {'tool': 'hash'}}
            with self.assertRaisesRegex(ValueError, 'different specification/source paths'):
                spec_set.verify_report_context(metrics, new, Path(new['paths']['capabilities']), {'tool': 'hash'})

    def test_source_relocation_and_changed_tool_bytes_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            bundle = self.bundle(Path(tmp) / 'v1/spec')
            source = Path(bundle['paths']['capabilities'])
            moved = source.with_name('proposal.md')
            shutil.copyfile(source, moved)
            metrics = {'spec_set': bundle, 'source': str(source), 'planning_tools': {'tool': 'old'}}
            with self.assertRaisesRegex(ValueError, 'different specification/source paths'):
                spec_set.verify_report_context(metrics, bundle, moved, {'tool': 'old'})
            with self.assertRaisesRegex(ValueError, 'tooling paths and bytes'):
                spec_set.verify_report_context(metrics, bundle, source, {'tool': 'new'})
            spec_set.verify_report_context(metrics, bundle, source, {'tool': 'old'})

    def test_default_main_resolves_sibling_specs_without_legacy_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            legacy = self.bundle(Path(tmp) / 'borg/spec')
            runtime = Path(tmp) / 'borg-runtime'
            runtime.mkdir()
            chosen = spec_set.implementation_main(runtime)
            self.assertEqual(chosen, Path(tmp) / 'borg/spec/impl/v1/implementation.md')
            with self.assertRaises(FileNotFoundError):
                spec_set.load(chosen)
            current = self.bundle(chosen.parent)
            self.assertEqual(spec_set.load(chosen), current)
            self.assertEqual(spec_set.implementation_main(runtime, Path(legacy['paths']['main'])),
                             Path(legacy['paths']['main']))

    def test_typed_roles_cannot_escape_into_sibling_implementation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'borg/spec/impl'
            one = self.bundle(root / 'v1')
            self.bundle(root / 'v2')
            main = Path(one['paths']['main'])
            original = main.read_text()
            for sibling in ['../v2/cap.md', '../v2/phase.md']:
                label = 'cap.md' if sibling.endswith('cap.md') else 'phase.md'
                main.write_text(original.replace('(' + label + ')', '(' + sibling + ')'))
                with self.assertRaisesRegex(ValueError, 'outside implementation specification root'):
                    spec_set.load(main)
            main.write_text(original)
            self.assertEqual(spec_set.load(spec_set.implementation_main(Path(tmp) / 'runtime', main)), one)


if __name__ == '__main__':
    unittest.main()
