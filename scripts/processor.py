"""PDF Processor - Full document conversion to structured Markdown"""
import os
import re
import yaml
import logging
from pathlib import Path
from markitdown import MarkItDown
from llm_client import create_multi_llm_client_from_config
from chunker import create_chunker_from_config
from queue_manager import create_queue_manager_from_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BACKTICKS = chr(96) * 3

class PDFProcessor:
    def __init__(self, config):
        self.config = config
        self.markitdown = MarkItDown()
        self.llm_client = create_multi_llm_client_from_config(config)
        self.chunker = create_chunker_from_config(config)
        self.queue = create_queue_manager_from_config(config)

    def _clean_llm_output(self, text):
        """Remove code block markers from LLM output"""
        pattern_start = r"^" + BACKTICKS + r"\w*\n?"
        pattern_end = r"\n?" + BACKTICKS + r"\s*$"
        pattern_mid = r"\n" + BACKTICKS + r"\s*\n"
        text = re.sub(pattern_start, "", text, flags=re.MULTILINE)
        text = re.sub(pattern_end, "", text)
        text = re.sub(pattern_mid, "\n", text)
        return text.strip()

    def _get_structure_prompt(self):
        return """You are a document structuring expert. Convert the provided raw text into well-formatted Markdown.

CRITICAL RULES:
1. Output ONLY plain Markdown text - NO code blocks, NO triple backticks
2. Preserve ALL content from the original text - do not summarize or omit anything
3. Use proper Markdown headings (##, ###, etc.) based on document structure
4. Format tables using Markdown table syntax with | and -
5. Keep all numerical data, dates, and statistics exactly as they appear
6. Preserve Japanese text exactly as written
7. Do NOT add YAML frontmatter. Do NOT wrap output in code blocks.

Output plain Markdown directly."""

    def _get_frontmatter_prompt(self):
        return """Analyze this document and create YAML frontmatter.

Output ONLY raw YAML like this:
---
title: "Document Title"
summary: "Brief summary."
keywords: ["keyword1", "keyword2"]
---

Do NOT use code blocks. Output raw YAML only."""

    def process_file(self, input_path, output_path, doc_type="default"):
        logger.info(f"Processing: {input_path}")
        result = {"input": input_path, "output": output_path, "chunks": 0, "cost": 0.0}

        try:
            logger.info("Step 1: Converting PDF to text...")
            md_result = self.markitdown.convert(input_path)
            raw_text = md_result.text_content
            logger.info(f"Extracted {len(raw_text)} characters")

            logger.info("Step 2: Splitting into chunks...")
            chunks = self.chunker.chunk(raw_text, doc_type)
            result["chunks"] = len(chunks)
            logger.info(f"Created {len(chunks)} chunks")

            logger.info("Step 3: Processing chunks with LLM...")
            processed_chunks = []
            total_cost = 0.0
            structure_prompt = self._get_structure_prompt()

            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}/{len(chunks)}")
                try:
                    resp = self.llm_client.generate(
                        structure_prompt,
                        chunk.content,
                        complexity="complex" if chunk.has_tables else "normal"
                    )
                    cleaned = self._clean_llm_output(resp.content)
                    processed_chunks.append(cleaned)
                    total_cost += resp.cost_usd
                    logger.info(f"Chunk {i+1} done, cost: ${resp.cost_usd:.4f}")
                except Exception as e:
                    logger.warning(f"LLM failed for chunk {i+1}: {e}")
                    processed_chunks.append(chunk.content)

            logger.info("Step 4: Generating frontmatter...")
            try:
                frontmatter_resp = self.llm_client.generate(
                    self._get_frontmatter_prompt(),
                    chunks[0].content[:5000],
                    complexity="normal"
                )
                frontmatter = self._clean_llm_output(frontmatter_resp.content)
                total_cost += frontmatter_resp.cost_usd
                if not frontmatter.startswith("---"):
                    frontmatter = "---\n" + frontmatter
                if not frontmatter.rstrip().endswith("---"):
                    frontmatter = frontmatter.rstrip() + "\n---"
            except Exception as e:
                logger.warning(f"Frontmatter failed: {e}")
                frontmatter = "---\ntitle: Processed Document\n---"

            logger.info("Step 5: Combining chunks...")
            final_content = frontmatter + "\n\n" + "\n\n".join(processed_chunks)

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_content, encoding="utf-8")

            result["cost"] = total_cost
            result["status"] = "success"
            logger.info(f"Done: {input_path} -> {output_path}")

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Failed: {e}")

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
    config = load_config()
    processor = PDFProcessor(config)
    stats = processor.run_queue()
    print(f"Processing complete: {stats}")

if __name__ == "__main__":
    main()
