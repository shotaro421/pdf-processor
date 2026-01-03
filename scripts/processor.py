"""PDF Processor with Quality Check"""
import os
import re
import yaml
import logging
from pathlib import Path
from markitdown import MarkItDown
from llm_client import create_multi_llm_client_from_config
from chunker import create_chunker_from_config
from queue_manager import create_queue_manager_from_config
from quality_checker import check_output, log_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
BACKTICKS = chr(96) * 3

class PDFProcessor:
    def __init__(self, config):
        self.config = config
        self.markitdown = MarkItDown()
        self.llm_client = create_multi_llm_client_from_config(config)
        self.chunker = create_chunker_from_config(config)
        self.queue = create_queue_manager_from_config(config)
        self.max_retries = 2

    def _clean_llm_output(self, text):
        p1 = r"^" + BACKTICKS + r"\w*\n?"
        p2 = r"\n?" + BACKTICKS + r"\s*$"
        p3 = r"\n" + BACKTICKS + r"\s*\n"
        text = re.sub(p1, "", text, flags=re.MULTILINE)
        text = re.sub(p2, "", text)
        text = re.sub(p3, "\n", text)
        return text.strip()

    def _get_structure_prompt(self):
        return """Convert raw text to Markdown. RULES:
1. Output ONLY plain Markdown - NO code blocks
2. Preserve ALL content - never skip anything
3. Use proper headings (##, ###)
4. Format tables with | and - (consistent columns)
5. Keep numbers/dates exactly as shown
6. Preserve Japanese text exactly
7. Process the ENTIRE text to the end
Output plain Markdown only."""

    def _get_frontmatter_prompt(self):
        return """Create YAML frontmatter:
---
title: "Title"
summary: "Summary."
keywords: ["key1", "key2"]
---
No code blocks."""

    def _process_chunk(self, chunk, i, total, prompt):
        for attempt in range(self.max_retries + 1):
            try:
                msg = "Chunk " + str(i+1) + "/" + str(total)
                if attempt > 0:
                    msg += " retry " + str(attempt)
                logger.info(msg)
                resp = self.llm_client.generate(prompt, chunk.content,
                    complexity="complex" if chunk.has_tables else "normal")
                cleaned = self._clean_llm_output(resp.content)
                if len(cleaned) < len(chunk.content) * 0.3:
                    logger.warning("Chunk " + str(i+1) + " too short, retry")
                    continue
                return cleaned, resp.cost_usd
            except Exception as e:
                logger.warning("Chunk " + str(i+1) + " error: " + str(e))
        return chunk.content, 0.0

    def process_file(self, input_path, output_path, doc_type="default"):
        logger.info("Processing: " + str(input_path))
        result = {"input": input_path, "output": output_path, "chunks": 0, "cost": 0.0}
        try:
            md_result = self.markitdown.convert(input_path)
            raw_text = md_result.text_content
            logger.info("Extracted " + str(len(raw_text)) + " chars")

            chunks = self.chunker.chunk(raw_text, doc_type)
            result["chunks"] = len(chunks)
            logger.info("Created " + str(len(chunks)) + " chunks")

            processed = []
            total_cost = 0.0
            prompt = self._get_structure_prompt()
            for i, chunk in enumerate(chunks):
                content, cost = self._process_chunk(chunk, i, len(chunks), prompt)
                processed.append(content)
                total_cost += cost

            try:
                resp = self.llm_client.generate(self._get_frontmatter_prompt(),
                    chunks[0].content[:5000], complexity="normal")
                fm = self._clean_llm_output(resp.content)
                total_cost += resp.cost_usd
                if not fm.startswith("---"):
                    fm = "---\n" + fm
                if not fm.rstrip().endswith("---"):
                    fm = fm.rstrip() + "\n---"
            except Exception:
                fm = "---\ntitle: Document\n---"

            final = fm + "\n\n" + "\n\n".join(processed)

            logger.info("Quality check...")
            report = check_output(final)
            log_report(report)

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final, encoding="utf-8")

            result["cost"] = total_cost
            result["status"] = "success"
            result["quality"] = report.is_valid
            logger.info("Done: " + str(output_path))
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error("Failed: " + str(e))
        return result

    def run_queue(self, max_parallel=1):
        input_dir = self.config.get("processing", {}).get("input_dir", "input")
        output_dir = self.config.get("processing", {}).get("output_dir", "output")
        self.queue.scan_input_dir(input_dir, output_dir)
        while True:
            job = self.queue.get_next_job()
            if not job:
                break
            self.queue.start_job(job.id)
            result = self.process_file(job.input_path, job.output_path)
            if result.get("status") == "success":
                self.queue.complete_job(job.id, result.get("cost", 0))
            else:
                self.queue.fail_job(job.id, result.get("error", "Unknown"))
        return self.queue.get_stats()

def load_config(path="config.yaml"):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    os.chdir(Path(__file__).parent.parent)
    processor = PDFProcessor(load_config())
    stats = processor.run_queue()
    print("Complete: " + str(stats))

if __name__ == "__main__":
    main()
