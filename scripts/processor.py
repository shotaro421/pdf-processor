"""Main Processor"""
import os, yaml, logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from markitdown import MarkItDown
from llm_client import create_multi_llm_client_from_config
from chunker import create_chunker_from_config
from queue_manager import create_queue_manager_from_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self, config):
        self.config = config
        self.markitdown = MarkItDown()
        self.llm_client = create_multi_llm_client_from_config(config)
        self.chunker = create_chunker_from_config(config)
        self.queue = create_queue_manager_from_config(config)
        self.prompt = self._default_prompt()
    def _default_prompt(self):
        return "You are a document analysis expert. Structure the provided text with YAML frontmatter (title, summary, keywords) and format the content with proper headings and markdown tables."
    def process_file(self, input_path, output_path, doc_type="default"):
        logger.info(f"Processing: {input_path}")
        result = {"input": input_path, "output": output_path, "chunks": 0, "cost": 0.0}
        try:
            md_result = self.markitdown.convert(input_path)
            raw_text = md_result.text_content
            chunks = self.chunker.chunk(raw_text, doc_type)
            result["chunks"] = len(chunks)
            processed = []
            total_cost = 0.0
            for i, chunk in enumerate(chunks):
                logger.info(f"Chunk {i+1}/{len(chunks)}")
                complexity = "complex" if chunk.has_tables else "normal"
                resp = self.llm_client.generate(self.prompt, chunk.content, complexity=complexity)
                processed.append(resp.content)
                total_cost += resp.cost_usd
            final = "\n\n".join(processed)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final, encoding="utf-8")
            result["cost"] = total_cost
            result["status"] = "success"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Failed: {e}")
        return result
    def run_queue(self, max_parallel=5):
        input_dir = self.config.get("processing", {}).get("input_dir", "input")
        output_dir = self.config.get("processing", {}).get("output_dir", "output")
        self.queue.scan_input_dir(input_dir, output_dir)
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {}
            while True:
                job = self.queue.get_next_job()
                if not job: break
                self.queue.start_job(job.id)
                futures[executor.submit(self.process_file, job.input_path, job.output_path)] = job
            for future in as_completed(futures):
                job = futures[future]
                try:
                    result = future.result()
                    if result.get("status") == "success": self.queue.complete_job(job.id, result.get("cost", 0))
                    else: self.queue.fail_job(job.id, result.get("error", "Unknown"))
                except Exception as e: self.queue.fail_job(job.id, str(e))
        return self.queue.get_stats()

def load_config(path="config.yaml"):
    with open(path, encoding="utf-8") as f: return yaml.safe_load(f)

def main():
    os.chdir(Path(__file__).parent.parent)
    config = load_config()
    processor = PDFProcessor(config)
    stats = processor.run_queue(config.get("processing", {}).get("max_parallel_jobs", 5))
    print(f"Done: {stats}")

if __name__ == "__main__": main()

