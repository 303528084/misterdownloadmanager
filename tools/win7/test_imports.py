#!/usr/bin/env python3
"""Tests of tools/win7/imports.py (CI's windows-7 job): its list's rows, the check's verdicts, the
documentation's version texts, and its PE reader against LLVM's (llvm-readobj --coff-imports) on
Windows' own programs, 64-bit and 32-bit, where LLVM is installed (GitHub's Windows runners).

    python tools/win7/test_imports.py
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import imports  # noqa: E402


def readobj():
    """LLVM's llvm-readobj, if installed."""
    found = shutil.which("llvm-readobj")
    if found:
        return found
    default = r"C:\Program Files\LLVM\bin\llvm-readobj.exe"
    return default if os.path.exists(default) else None


class TheList(unittest.TestCase):
    def test_every_row_is_well_formed_and_windows_7s(self):
        known = imports.load_list()
        self.assertGreater(len(known), 100)
        for (dll, fn), (version, text, source) in known.items():
            self.assertEqual(dll, dll.lower())
            self.assertTrue(dll.endswith(".dll"), dll)
            self.assertTrue(source.strip(), f"{dll}!{fn} says where its version comes from")
            # The list is of Windows 7's functions: a newer one is looked up at run time instead,
            # never imported, so it has no row (the check would refuse it anyway).
            self.assertLessEqual(version[:2], imports.WIN7, f"{dll}!{fn} needs Windows {text}")

    def test_every_dll_of_a_function_has_its_own_row(self):
        known = imports.load_list()
        for dll, fn in known:
            self.assertIn((dll, "*"), known, f"{dll} has functions but no row of its own")


class TheCheck(unittest.TestCase):
    def setUp(self):
        self.known = {
            ("kernel32.dll", "*"): ((5, 0), "5.0", "test"),
            ("kernel32.dll", "CreateFileW"): ((5, 0), "5.0", "test"),
            ("kernel32.dll", "GetSystemTimePreciseAsFileTime"): ((6, 2), "6.2", "test"),
            ("combase.dll", "*"): ((6, 2), "6.2", "test"),
            ("combase.dll", "CoTaskMemAlloc"): ((5, 0), "5.0", "test"),
        }

    def verdicts(self, imported):
        real = imports.read_imports
        imports.read_imports = lambda path: imported
        try:
            return [(name, why) for _, name, why in imports.check(["x.exe"], self.known)]
        finally:
            imports.read_imports = real

    def test_windows_7s_functions_pass(self):
        self.assertEqual(self.verdicts([("KERNEL32.dll", "CreateFileW", False)]), [])

    def test_a_newer_function_fails(self):
        (name, why), = self.verdicts([("kernel32.dll", "GetSystemTimePreciseAsFileTime", False)])
        self.assertIn("6.2", why)

    def test_a_function_of_a_newer_dll_fails(self):
        (name, why), = self.verdicts([("combase.dll", "CoTaskMemAlloc", False)])
        self.assertIn("combase.dll is new", why)

    def test_an_unknown_function_or_dll_fails(self):
        got = self.verdicts([("kernel32.dll", "Unheard", False), ("shcore.dll", "X", True)])
        self.assertEqual(len(got), 2)
        self.assertIn("not in tools/win7/win7-imports.tsv", got[0][1])
        self.assertIn("(delay-loaded)", got[1][0])


class TheDocsVersions(unittest.TestCase):
    def test_minimum_clients(self):
        for text, want in [
            ("Windows 2000 Professional [desktop apps only]", "5.0"),
            ("Windows XP [desktop apps \\| UWP apps]", "5.1"),
            ("Windows\u00a0Vista [desktop apps \\| UWP apps]", "6.0"),
            ("Windows 7 [desktop apps only]", "6.1"),
            ("Windows 8 [desktop apps \\| UWP apps]", "6.2"),
            ("Windows 8.1 [desktop apps only]", "6.3"),
            ("Windows 10, version 1607 [desktop apps only]", "10.0.14393"),
            ("Windows 10 [desktop apps only]", "10.0"),
        ]:
            self.assertEqual(imports.client_version(text), want, text)


@unittest.skipUnless(os.name == "nt" and readobj(), "needs Windows and LLVM's llvm-readobj")
class TheReader(unittest.TestCase):
    def llvm(self, path):
        out = subprocess.run([readobj(), "--coff-imports", path], check=True,
                             capture_output=True, text=True).stdout
        got, dll, delayed = [], None, False
        for raw in out.splitlines():
            line = raw.strip()
            # A table's block starts at the line's start; a delay table's entries are nested
            # "Import {" blocks.
            if raw.startswith("Import {"):
                delayed = False
            elif raw.startswith("DelayImport {"):
                delayed = True
            elif line.startswith("Name: "):
                dll = line[6:]
            elif line.startswith("Symbol: "):
                m = re.match(r"Symbol: (\S*) \((\d+)\)", line)
                name = m.group(1) if m and m.group(1) else f"#{m.group(2)}" if m else line[8:]
                got.append((dll, name, delayed))
        return got

    def test_as_llvm_reads_windows_own_programs(self):
        root = os.environ.get("SystemRoot", r"C:\Windows")
        programs = [os.path.join(root, "System32", n) for n in ("notepad.exe", "cmd.exe")]
        programs += [os.path.join(root, "SysWOW64", n) for n in ("cmd.exe",)]
        programs = [p for p in programs if os.path.exists(p)]
        self.assertTrue(programs)
        for path in programs:
            # Copied first: System32 is redirected for a 32-bit Python, and LLVM reads the copy.
            with tempfile.TemporaryDirectory() as d:
                copy = os.path.join(d, os.path.basename(path))
                shutil.copy(path, copy)
                mine = imports.read_imports(copy)
                self.assertTrue(mine, path)
                self.assertEqual(sorted(mine), sorted(self.llvm(copy)), path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
