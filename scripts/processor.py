"""Main Processor - PDF Structural Processing Pipeline"""

import os
import sys
import re
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

BACKTICKS = chr(96) * 3


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
提供されたテキストを解析し、構造化されたMarkdownとして出力してください。

【重要なルール】
1. 入力テキストの内容をすべて出力に含めること。省略は禁止。
2. 文章は最後まで完全に出力すること。途中で切らない。
3. 元の情報の意味を変えないこと。

【出力形式】
最初のチャンクのみ以下のYAMLフロントマターを含める:
---
title: 文書タイトル
summary: 30文字以内の要約
keywords: [キーワード1, キーワード2, ...]
---

【本文の整形ルール】
- 見出しレベルを適切に設定（#, ##, ### など）
- 段落は適切に分割
- 重要ポイントは**太字**に

【表の処理ルール - 重要】
表データは以下の優先順位で処理:
1. シンプルな表（5列以下）: Markdownテーブル形式で出力
   | 列1 | 列2 | 列3 |
   |-----|-----|-----|
   | 値1 | 値2 | 値3 |

2. 複雑な表（6列以上または内容が長い）: 構造化リスト形式で出力
   ### [表のタイトル]
   **項目1**: 値1
   **項目2**: 値2
   - サブ項目A: 値A
   - サブ項目B: 値B

3. 数値データが多い表: 箇条書きリストで出力
   - 2023年度売上高: 1,234百万円
   - 2022年度売上高: 1,100百万円
   - 前年比: +12.2%

【禁止事項】
- コードブロック記号で出力を囲まない
- 「以下省略」「...」などで内容を省略しない
- 文を途中で終わらせない"""

    def _clean_llm_output(self, text: str) -> str:
        """LLM出力からコードブロックマーカーを除去"""
        p1 = r"^" + BACKTICKS + r"\w*\n?"
        p2 = r"\n?" + BACKTICKS + r"\s*$"
        p3 = r"\n" + BACKTICKS + r"\s*\n"
        text = re.sub(p1, "", text, flags=re.MULTILINE)
        text = re.sub(p2, "", text)
        text = re.sub(p3, "\n", text)
        return text.strip()

    def _validate_output(self, input_text: str, output_text: str) -> Dict:
        """出力の完全性を検証"""
        input_len = len(input_text)
        output_len = len(output_text)
        ratio = output_len / input_len if input_len > 0 else 0
        
        truncation_indicators = ["の", "が", "を", "に", "は", "で", "と", "も", "や", "へ"]
        last_char = output_text.rstrip()[-1] if output_text.rstrip() else ""
        possibly_truncated = last_char in truncation_indicators
        
        proper_endings = ["。", "」", "）", ")", "]", "】", ".", "!", "?", ":", "\n"]
        proper_end = any(output_text.rstrip().endswith(e) for e in proper_endings)
        
        return {
            "input_chars": input_len,
            "output_chars": output_len,
            "ratio": ratio,
            "possibly_truncated": possibly_truncated,
            "proper_ending": proper_end,
            "is_valid": ratio >= 0.3 and (proper_end or not possibly_truncated)
        }

    def _process_chunk_with_retry(self, chunk: Chunk, prompt: str, chunk_idx: int, total_chunks: int) -> str:
        """チャンクを処理し、必要に応じてリトライ"""
        max_retries = 3
        
        if chunk_idx > 0:
            chunk_prompt = prompt + "\n\n【注意】これは文書の途中部分です。YAMLフロントマターは出力しないでください。"
        else:
            chunk_prompt = prompt
        
        cleaned_output = ""
        for attempt in range(max_retries):
            complexity = "complex" if chunk.has_tables else "normal"
            response = self.llm_client.generate(chunk_prompt, chunk.content, complexity=complexity)
            cleaned_output = self._clean_llm_output(response.content)
            
            validation = self._validate_output(chunk.content, cleaned_output)
            logger.info(f"Chunk {chunk_idx+1}/{total_chunks} validation: ratio={validation['ratio']:.2f}, valid={validation['is_valid']}")
            
            if validation["is_valid"]:
                return cleaned_output
            
            if attempt < max_retries - 1:
                logger.warning(f"Chunk {chunk_idx+1} may be truncated (ratio={validation['ratio']:.2f}), retrying...")
                chunk_prompt = prompt + "\n\n【最重要】すべての内容を完全に出力してください。途中で切らないでください。"
        
        logger.warning(f"Chunk {chunk_idx+1} validation failed after {max_retries} attempts, using last output")
        return cleaned_output

    def process_file(self, input_path: str, output_path: str, doc_type: str = "default") -> Dict:
        logger.info(f"Processing: {input_path}")
        result = {"input": input_path, "output": output_path, "chunks": 0, "cost": 0.0}

        try:
            md_result = self.markitdown.convert(input_path)
            raw_text = md_result.text_content
            logger.info(f"Extracted {len(raw_text)} chars from PDF")

            chunks = self.chunker.chunk(raw_text, doc_type)
            result["chunks"] = len(chunks)
            logger.info(f"Split into {len(chunks)} chunks")

            prompt = self.prompts.get(doc_type, self.prompts["default"])
            processed_chunks = []
            total_cost = 0.0

            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}/{len(chunks)} ({chunk.token_count} tokens)")
                
                try:
                    processed_content = self._process_chunk_with_retry(chunk, prompt, i, len(chunks))
                    processed_chunks.append(processed_content)
                    total_cost += 0.001
                except Exception as e:
                    logger.error(f"Failed to process chunk {i+1}: {e}")
                    processed_chunks.append(chunk.content)

            final_content = self._merge_chunks(processed_chunks)
            
            final_validation = self._validate_output(raw_text, final_content)
            logger.info(f"Final validation: {final_validation}")
            
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_content, encoding="utf-8")

            result["cost"] = total_cost
            result["status"] = "success"
            result["validation"] = final_validation
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
            merged += "\n\n" + chunk.strip()
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
    print(f"\n=== Processing Summary ===")
    print(f"Total jobs: {stats.get('total_jobs', 0)}")
    print(f"By status: {stats.get('by_status', {})}")
    print(f"Total cost: ${stats.get('total_cost_usd', 0):.4f}")


if __name__ == "__main__":
    main()
