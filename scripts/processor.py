"""Main Processor - PDF Structural Processing Pipeline"""

import os
import sys
import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from markitdown import MarkItDown

from llm_client import create_multi_llm_client_from_config, MultiLLMClient
from chunker import create_chunker_from_config, DocumentChunker, Chunk
from queue_manager import create_queue_manager_from_config, QueueManager, Job, JobStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PDFProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.markitdown = MarkItDown()
        self.llm_client = create_multi_llm_client_from_config(config)
        self.chunker = create_chunker_from_config(config)
        self.queue = create_queue_manager_from_config(config)
        self.prompts = self._load_prompts()

    def _load_prompts(self) -> Dict[str, str]:
        prompts = {}
        prompts_dir = Path("prompts")
        if prompts_dir.exists():
            for p in prompts_dir.glob("*.txt"):
                prompts[p.stem] = p.read_text(encoding="utf-8")
        if "default" not in prompts:
            prompts["default"] = self._get_default_prompt()
        return prompts

    def _get_default_prompt(self) -> str:
        return """あなたは高度なドキュメント解析エキスパートです。
提供されたテキストを解析し、以下の形式で出力してください。

1. YAMLフロントマター:
   - title: 文書タイトル
   - summary: 30文字以内の要約
   - keywords: [重要語1, 重要語2, ...]
   - structure: 文書の論理構造
   - visual_logic: Mermaidコード(フローチャート)

2. 本文の整形:
   - 見出しレベルを適切に設定
   - 表はMarkdownテーブル形式で維持
   - 重要ポイントは太字に

元の情報の意味を変えないこと。"""

    def process_file(self, input_path: str, output_path: str, doc_type: str = "default") -> Dict:
        logger.info(f"Processing: {input_path}")
        result = {"input": input_path, "output": output_path, "chunks": 0, "cost": 0.0}

        try:
            # 1. PDF to Markdown
            md_result = self.markitdown.convert(input_path)
            raw_text = md_result.text_content
            logger.info(f"Extracted {len(raw_text)} chars from PDF")

            # 2. Chunk the document
            chunks = self.chunker.chunk(raw_text, doc_type)
            result["chunks"] = len(chunks)
            logger.info(f"Split into {len(chunks)} chunks")

            # 3. Process each chunk with LLM
            prompt = self.prompts.get(doc_type, self.prompts["default"])
            processed_chunks = []
            total_cost = 0.0

            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}/{len(chunks)}")
                complexity = "complex" if chunk.has_tables else "normal"
                response = self.llm_client.generate(prompt, chunk.content, complexity=complexity)
                processed_chunks.append(response.content)
                total_cost += response.cost_usd

            # 4. Merge and save
            final_content = self._merge_chunks(processed_chunks)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_content, encoding="utf-8")

            result["cost"] = total_cost
            result["status"] = "success"
            logger.info(f"Saved to {output_path}, cost: ${total_cost:.4f}")

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Failed: {e}")

        return result

    def _merge_chunks(self, chunks: List[str]) -> str:
        if len(chunks) == 1:
            return chunks[0]
        merged = chunks[0]
        for chunk in chunks[1:]:
            if chunk.startswith("---"):
                lines = chunk.split("\n")
                end_idx = next((i for i, l in enumerate(lines[1:], 1) if l == "---"), len(lines))
                chunk = "\n".join(lines[end_idx+1:])
            merged += "\n\n" + chunk
        return merged

    def run_queue(self, max_parallel: int = 5):
        input_dir = self.config.get("processing", {}).get("input_dir", "input")
        output_dir = self.config.get("processing", {}).get("output_dir", "output")

        new_jobs = self.queue.scan_input_dir(input_dir, output_dir)
        logger.info(f"Found {len(new_jobs)} new files to process")

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {}
            while True:
                job = self.queue.get_next_job()
                if not job:
                    break
                self.queue.start_job(job.id)
                future = executor.submit(self.process_file, job.input_path, job.output_path)
                futures[future] = job

            for future in as_completed(futures):
                job = futures[future]
                try:
                    result = future.result()
                    if result.get("status") == "success":
                        self.queue.complete_job(job.id, result.get("cost", 0))
                    else:
                        self.queue.fail_job(job.id, result.get("error", "Unknown"))
                except Exception as e:
                    self.queue.fail_job(job.id, str(e))

        return self.queue.get_stats()


def load_config(config_path: str = "config.yaml") -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()
    processor = PDFProcessor(config)

    max_parallel = config.get("processing", {}).get("max_parallel_jobs", 5)
    stats = processor.run_queue(max_parallel)

    logger.info(f"Processing complete: {stats}")
    print(f"\n=== Processing Summary ===" )
    print(f"Total jobs: {stats.get('total_jobs', 0)}")
    print(f"By status: {stats.get('by_status', {})}")
    print(f"Total cost: ${stats.get('total_cost_usd', 0):.4f}")


if __name__ == "__main__":
    main()
