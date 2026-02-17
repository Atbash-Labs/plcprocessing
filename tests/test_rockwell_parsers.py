#!/usr/bin/env python3
"""
Automated tests for Rockwell PLC file parsers.

Tests L5X, L5K, and unified rockwell_export parsers against both
synthetic sample files and real-world L5X exports from GitHub.
"""

import os
import sys
import unittest
from pathlib import Path

# Add scripts/ to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from sc_parser import SCFile, Tag, LogicRung
from l5x_export import L5XParser
from l5k_parser import L5KParser, _write_sc_file, _sanitize_comment
from rockwell_export import (
    detect_rockwell_format, is_rockwell_file,
    find_rockwell_files, parse_rockwell_file,
)

SAMPLES_DIR = Path(__file__).parent / 'samples'
REAL_SAMPLES_DIR = Path(__file__).parent / 'real_samples'


class TestFormatDetection(unittest.TestCase):
    """Test Rockwell file format detection."""

    def test_detect_l5x(self):
        for f in SAMPLES_DIR.glob('*.L5X'):
            self.assertEqual(detect_rockwell_format(str(f)), 'L5X', f.name)

    def test_detect_l5k(self):
        for f in SAMPLES_DIR.glob('*.L5K'):
            self.assertEqual(detect_rockwell_format(str(f)), 'L5K', f.name)

    def test_detect_real_l5x(self):
        if not REAL_SAMPLES_DIR.exists():
            self.skipTest("No real samples directory")
        for f in REAL_SAMPLES_DIR.glob('*.L5X'):
            self.assertEqual(detect_rockwell_format(str(f)), 'L5X', f.name)

    def test_is_rockwell_file(self):
        for f in SAMPLES_DIR.glob('*.L5X'):
            self.assertTrue(is_rockwell_file(str(f)), f.name)
        for f in SAMPLES_DIR.glob('*.L5K'):
            self.assertTrue(is_rockwell_file(str(f)), f.name)

    def test_non_rockwell_file(self):
        self.assertIsNone(detect_rockwell_format(__file__))
        self.assertFalse(is_rockwell_file(__file__))

    def test_find_rockwell_files(self):
        files = find_rockwell_files(str(SAMPLES_DIR))
        self.assertGreater(len(files), 0)
        for fp, fmt in files:
            self.assertIn(fmt, ('L5X', 'L5K', 'ACD', 'RSS'))


class TestL5XParser(unittest.TestCase):
    """Test L5X XML parser."""

    def setUp(self):
        self.parser = L5XParser()

    def test_parse_sample_project(self):
        fpath = SAMPLES_DIR / 'sample_project.L5X'
        if not fpath.exists():
            self.skipTest("sample_project.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertGreater(len(results), 0)

        # Check we got expected component types
        types = {sc.type for sc in results}
        self.assertIn('UDT', types)
        self.assertIn('AOI', types)

        # Verify SCFile structure
        for sc in results:
            self.assertIsInstance(sc, SCFile)
            self.assertTrue(sc.name)
            self.assertIn(sc.type, ('AOI', 'UDT', 'PROGRAM', 'CONTROLLER'))

    def test_parse_aoi_with_tags(self):
        """Test AOI parsing extracts input/output/local tags."""
        fpath = SAMPLES_DIR / 'sample_project.L5X'
        if not fpath.exists():
            self.skipTest("sample_project.L5X not found")

        results = self.parser.parse_file(str(fpath))
        aois = [sc for sc in results if sc.type == 'AOI']
        self.assertGreater(len(aois), 0)

        for aoi in aois:
            # AOIs should have at least some tags
            total_tags = (len(aoi.input_tags) + len(aoi.output_tags) +
                          len(aoi.inout_tags) + len(aoi.local_tags))
            self.assertGreater(total_tags, 0, f"AOI {aoi.name} has no tags")
            # AOIs should have at least one routine
            self.assertGreater(len(aoi.routines), 0,
                               f"AOI {aoi.name} has no routines")

    def test_parse_udt_with_members(self):
        """Test UDT parsing extracts members as local tags."""
        fpath = SAMPLES_DIR / 'sample_project.L5X'
        if not fpath.exists():
            self.skipTest("sample_project.L5X not found")

        results = self.parser.parse_file(str(fpath))
        udts = [sc for sc in results if sc.type == 'UDT']
        self.assertGreater(len(udts), 0)

        for udt in udts:
            self.assertGreater(len(udt.local_tags), 0,
                               f"UDT {udt.name} has no members")


class TestL5XParserRealSamples(unittest.TestCase):
    """Test L5X parser against real-world L5X files from GitHub."""

    def setUp(self):
        self.parser = L5XParser()
        if not REAL_SAMPLES_DIR.exists():
            self.skipTest("No real samples directory")

    def test_parse_aoi_debouncer(self):
        fpath = REAL_SAMPLES_DIR / 'AOI_DEBOUNCER.L5X'
        if not fpath.exists():
            self.skipTest("AOI_DEBOUNCER.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertEqual(len(results), 1)

        aoi = results[0]
        self.assertEqual(aoi.name, 'AOI_DEBOUNCER')
        self.assertEqual(aoi.type, 'AOI')
        self.assertEqual(aoi.revision, '1.0')
        self.assertEqual(len(aoi.input_tags), 5)
        self.assertEqual(len(aoi.output_tags), 1)
        self.assertEqual(len(aoi.local_tags), 5)
        self.assertEqual(len(aoi.routines), 1)
        self.assertEqual(aoi.routines[0]['name'], 'Logic')
        self.assertEqual(aoi.routines[0]['type'], 'RLL')
        self.assertEqual(len(aoi.routines[0]['rungs']), 7)

    def test_parse_aoi_cv_control(self):
        fpath = REAL_SAMPLES_DIR / 'AOI_CV_CONTROL.L5X'
        if not fpath.exists():
            self.skipTest("AOI_CV_CONTROL.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertEqual(len(results), 1)

        aoi = results[0]
        self.assertEqual(aoi.name, 'AOI_CV_CONTROL')
        self.assertEqual(aoi.type, 'AOI')
        self.assertEqual(len(aoi.input_tags), 14)
        self.assertEqual(len(aoi.output_tags), 1)
        self.assertEqual(len(aoi.local_tags), 18)
        self.assertEqual(len(aoi.routines[0]['rungs']), 19)

    def test_parse_aoi_ramp_basic(self):
        fpath = REAL_SAMPLES_DIR / 'AOI_RAMP_BASIC.L5X'
        if not fpath.exists():
            self.skipTest("AOI_RAMP_BASIC.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertEqual(len(results), 1)

        aoi = results[0]
        self.assertEqual(aoi.name, 'AOI_RAMP_BASIC')
        self.assertEqual(aoi.type, 'AOI')
        self.assertEqual(len(aoi.routines[0]['rungs']), 15)

    def test_parse_meter_orifice_with_udts(self):
        """Test multi-component L5X file with AOI + dependent UDTs."""
        fpath = REAL_SAMPLES_DIR / 'AOI_METER_ORIFICE.L5X'
        if not fpath.exists():
            self.skipTest("AOI_METER_ORIFICE.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertEqual(len(results), 3)

        types = {sc.type for sc in results}
        self.assertIn('AOI', types)
        self.assertIn('UDT', types)

        udts = [sc for sc in results if sc.type == 'UDT']
        aois = [sc for sc in results if sc.type == 'AOI']
        self.assertEqual(len(udts), 2)
        self.assertEqual(len(aois), 1)
        self.assertEqual(aois[0].name, 'AOI_METER_ORIFICE')
        self.assertIn(aois[0].inout_tags[0].data_type, ('METER_DPCALC',))

    def test_parse_op_interlock_structured_text(self):
        """Test production-grade structured text AOI with dependencies."""
        fpath = REAL_SAMPLES_DIR / 'Op_Interlock_AOI.L5X'
        if not fpath.exists():
            self.skipTest("Op_Interlock_AOI.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertGreaterEqual(len(results), 3)  # STR_40 UDT + Str_Size + Str_Clear + Op_Interlock

        # Find Op_Interlock
        interlock = next(sc for sc in results if sc.name == 'Op_Interlock')
        self.assertEqual(interlock.type, 'AOI')
        self.assertGreater(len(interlock.input_tags), 40)
        self.assertGreater(len(interlock.output_tags), 5)

        # Verify has ST routines with content
        self.assertEqual(len(interlock.routines), 2)
        logic = next(r for r in interlock.routines if r['name'] == 'Logic')
        self.assertEqual(logic['type'], 'ST')
        self.assertIn('raw_content', logic)
        self.assertGreater(len(logic['raw_content']), 100)

        # Verify Prescan routine exists
        prescan = next(r for r in interlock.routines if r['name'] == 'Prescan')
        self.assertEqual(prescan['type'], 'ST')

    def test_parse_op_permissive(self):
        fpath = REAL_SAMPLES_DIR / 'Op_Permissive_AOI.L5X'
        if not fpath.exists():
            self.skipTest("Op_Permissive_AOI.L5X not found")

        results = self.parser.parse_file(str(fpath))
        self.assertGreaterEqual(len(results), 2)

        permissive = next(sc for sc in results if sc.name == 'Op_Permissive')
        self.assertEqual(permissive.type, 'AOI')
        self.assertGreater(len(permissive.input_tags), 30)

    def test_tag_descriptions_extracted(self):
        """Test that CDATA descriptions are properly extracted from tags."""
        fpath = REAL_SAMPLES_DIR / 'AOI_CV_CONTROL.L5X'
        if not fpath.exists():
            self.skipTest("AOI_CV_CONTROL.L5X not found")

        results = self.parser.parse_file(str(fpath))
        aoi = results[0]

        # AutoPctRef should have a description
        auto_pct = next(t for t in aoi.input_tags if t.name == 'AutoPctRef')
        self.assertIsNotNone(auto_pct.description)
        self.assertIn('SpeedRef', auto_pct.description)

    def test_rung_comments_extracted(self):
        """Test that rung comments are properly extracted."""
        fpath = REAL_SAMPLES_DIR / 'AOI_CV_CONTROL.L5X'
        if not fpath.exists():
            self.skipTest("AOI_CV_CONTROL.L5X not found")

        results = self.parser.parse_file(str(fpath))
        aoi = results[0]
        rungs = aoi.routines[0]['rungs']

        # Some rungs should have comments
        commented_rungs = [r for r in rungs if r.comment]
        self.assertGreater(len(commented_rungs), 0,
                           "No rung comments were extracted")

    def test_all_real_samples_parse(self):
        """Ensure every L5X file in real_samples/ parses without errors."""
        for fpath in sorted(REAL_SAMPLES_DIR.glob('*.L5X')):
            with self.subTest(file=fpath.name):
                results = self.parser.parse_file(str(fpath))
                self.assertGreater(len(results), 0,
                                   f"No components parsed from {fpath.name}")
                for sc in results:
                    self.assertIsInstance(sc, SCFile)
                    self.assertTrue(sc.name)


class TestL5KParser(unittest.TestCase):
    """Test L5K ASCII text parser."""

    def setUp(self):
        self.parser = L5KParser()

    def test_parse_sample_project(self):
        fpath = SAMPLES_DIR / 'sample_project.L5K'
        if not fpath.exists():
            self.skipTest("sample_project.L5K not found")

        results = self.parser.parse_file(str(fpath))
        self.assertGreater(len(results), 0)

        types = {sc.type for sc in results}
        self.assertIn('UDT', types)
        self.assertIn('AOI', types)
        self.assertIn('PROGRAM', types)
        self.assertIn('CONTROLLER', types)

    def test_udt_parsing(self):
        fpath = SAMPLES_DIR / 'sample_project.L5K'
        if not fpath.exists():
            self.skipTest("sample_project.L5K not found")

        results = self.parser.parse_file(str(fpath))
        udts = [sc for sc in results if sc.type == 'UDT']
        self.assertEqual(len(udts), 3)

        udt_names = {sc.name for sc in udts}
        self.assertIn('HMI_PumpControl', udt_names)
        self.assertIn('PID_Config', udt_names)
        self.assertIn('AnalogInput', udt_names)

    def test_aoi_parsing(self):
        fpath = SAMPLES_DIR / 'sample_project.L5K'
        if not fpath.exists():
            self.skipTest("sample_project.L5K not found")

        results = self.parser.parse_file(str(fpath))
        aois = [sc for sc in results if sc.type == 'AOI']
        self.assertEqual(len(aois), 2)

        pump = next(sc for sc in aois if sc.name == 'PumpControl')
        self.assertGreater(len(pump.input_tags), 0)
        self.assertGreater(len(pump.output_tags), 0)
        self.assertGreater(len(pump.routines), 0)

    def test_program_parsing(self):
        fpath = SAMPLES_DIR / 'sample_project.L5K'
        if not fpath.exists():
            self.skipTest("sample_project.L5K not found")

        results = self.parser.parse_file(str(fpath))
        programs = [sc for sc in results if sc.type == 'PROGRAM']
        self.assertEqual(len(programs), 1)
        self.assertEqual(programs[0].name, 'MainProgram')
        self.assertGreater(len(programs[0].routines), 0)

    def test_controller_tags(self):
        fpath = SAMPLES_DIR / 'sample_project.L5K'
        if not fpath.exists():
            self.skipTest("sample_project.L5K not found")

        results = self.parser.parse_file(str(fpath))
        controller = next(sc for sc in results if sc.type == 'CONTROLLER')
        self.assertEqual(controller.name, 'WaterTreatment')
        # Controller should have tags
        total_tags = (len(controller.input_tags) + len(controller.output_tags) +
                      len(controller.local_tags))
        self.assertGreater(total_tags, 0)


class TestUnifiedParser(unittest.TestCase):
    """Test the unified rockwell_export parser."""

    def test_parse_l5x_via_unified(self):
        fpath = SAMPLES_DIR / 'sample_project.L5X'
        if not fpath.exists():
            self.skipTest("sample_project.L5X not found")

        results = parse_rockwell_file(str(fpath))
        self.assertGreater(len(results), 0)

    def test_parse_l5k_via_unified(self):
        fpath = SAMPLES_DIR / 'sample_project.L5K'
        if not fpath.exists():
            self.skipTest("sample_project.L5K not found")

        results = parse_rockwell_file(str(fpath))
        self.assertGreater(len(results), 0)

    def test_format_hint_override(self):
        fpath = SAMPLES_DIR / 'sample_project.L5X'
        if not fpath.exists():
            self.skipTest("sample_project.L5X not found")

        results = parse_rockwell_file(str(fpath), format_hint='L5X')
        self.assertGreater(len(results), 0)


class TestSCFileExport(unittest.TestCase):
    """Test .sc file export functionality."""

    def test_sanitize_comment_single_line(self):
        self.assertEqual(_sanitize_comment("Hello world"), "Hello world")

    def test_sanitize_comment_multi_line(self):
        self.assertEqual(
            _sanitize_comment("0 = Normal\n1 = Inverted"),
            "0 = Normal | 1 = Inverted"
        )

    def test_sanitize_comment_empty_lines(self):
        self.assertEqual(
            _sanitize_comment("Line 1\n\nLine 3"),
            "Line 1 | Line 3"
        )

    def test_export_round_trip(self):
        """Test that parsing and exporting produces valid .sc files."""
        import tempfile

        fpath = REAL_SAMPLES_DIR / 'AOI_DEBOUNCER.L5X'
        if not fpath.exists():
            self.skipTest("AOI_DEBOUNCER.L5X not found")

        parser = L5XParser()
        results = parser.parse_file(str(fpath))
        self.assertEqual(len(results), 1)

        # Write to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sc',
                                         delete=False) as tmp:
            tmp_path = tmp.name

        try:
            _write_sc_file(results[0], tmp_path)
            # Verify file was created and has content
            self.assertTrue(os.path.exists(tmp_path))
            with open(tmp_path) as f:
                content = f.read()
            self.assertIn('AOI_DEBOUNCER', content)
            self.assertIn('VAR_INPUT', content)
            self.assertIn('VAR_OUTPUT', content)
            self.assertIn('Rung 0', content)
            # Verify no stray newlines in comments
            for line in content.split('\n'):
                if line.strip().startswith('//') or '  //' in line:
                    # Comment lines should be single-line
                    self.assertNotIn('\n', line)
        finally:
            os.unlink(tmp_path)


if __name__ == '__main__':
    unittest.main(verbosity=2)
