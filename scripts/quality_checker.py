"""Output Quality Checker"""
import re
import logging
from dataclasses import dataclass
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

@dataclass
class QualityIssue:
    line_number: int
    issue_type: str
    description: str
    severity: str
    suggestion: Optional[str] = None

@dataclass 
class QualityReport:
    issues: List[QualityIssue]
    table_count: int
    heading_count: int
    total_lines: int
    total_chars: int
    is_valid: bool

class OutputChecker:
    def __init__(self):
        self.issues = []
    
    def check(self, content):
        self.issues = []
        lines = content.split(chr(10))
        self._check_frontmatter(content)
        self._check_tables(lines)
        self._check_truncation(lines)
        table_count = len(re.findall(r"^\|.*\|$", content, re.MULTILINE))
        heading_count = len(re.findall(r"^#{1,6}\s", content, re.MULTILINE))
        has_errors = any(i.severity == "error" for i in self.issues)
        return QualityReport(self.issues, table_count, heading_count, len(lines), len(content), not has_errors)
    
    def _check_frontmatter(self, content):
        if not content.startswith("---"):
            self.issues.append(QualityIssue(1, "frontmatter", "Missing YAML frontmatter", "warning"))
            return
        if content.count("---") < 2:
            self.issues.append(QualityIssue(1, "frontmatter", "Unclosed YAML frontmatter", "error"))
    
    def _check_tables(self, lines):
        in_table = False
        expected_cols = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            is_row = stripped.startswith("|") and stripped.endswith("|")
            if is_row:
                if not in_table:
                    in_table = True
                    expected_cols = line.count("|") - 1
                else:
                    cols = line.count("|") - 1
                    if cols != expected_cols:
                        sep = re.match(r"^\|[-:\s|]+\|$", stripped)
                        if not sep:
                            self.issues.append(QualityIssue(i+1, "table", f"Column mismatch: {expected_cols} vs {cols}", "warning"))
            else:
                in_table = False
    
    def _check_truncation(self, lines):
        if len(lines) < 10:
            self.issues.append(QualityIssue(len(lines), "truncation", "Content very short", "warning"))
        last = chr(10).join(lines[-3:]).rstrip()
        endings = [",", chr(12289), chr(12398), chr(12364), chr(12434), chr(12395), chr(12391), chr(12392), chr(12399)]
        if last and last[-1] in endings:
            self.issues.append(QualityIssue(len(lines), "truncation", "Content may be truncated", "error"))

def check_output(content):
    checker = OutputChecker()
    return checker.check(content)

def log_report(report):
    logger.info(f"Quality: {report.total_lines} lines, {report.table_count} tables")
    for issue in report.issues:
        msg = f"[{issue.issue_type}] Line {issue.line_number}: {issue.description}"
        if issue.severity == "error":
            logger.error(msg)
        else:
            logger.warning(msg)
    logger.info("Result: " + ("PASSED" if report.is_valid else "ISSUES FOUND"))
