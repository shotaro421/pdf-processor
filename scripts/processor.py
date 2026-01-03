"""PDF Processor - Full document conversion to structured Markdown"""
import os
import re
import yaml
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from markitdown import MarkItDown
from llm_client import create_multi_llm_client_from_config
from chunker import create_chunker_from_config
from queue_manager import create_queue_manager_from_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self, config):
        self.config = config
        self.markitdown = MarkItDown()
        self.llm_client = create_multi_llm_client_from_config(config)
        self.chunker = create_chunker_from_config(config)
        self.queue = create_queue_manager_from_config(config)
    def _clean_llm_output(self, text):
        """Remove code block markers from LLM output"""
        import re
        text = re.sub(r'^' + chr(96)*3 + r'\w*\s*
?', '', text, flags=re.MULTILINE)
        text = re.sub(r'
?' + chr(96)*3 + r'\s*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'
' + chr(96)*3 + r'\s*
', '
', text)
        return text.strip()


    def _get_structure_prompt(self):
        return """You are a document structuring expert. Convert the provided raw text into well-formatted Markdown.

IMPORTANT RULES:
1. Preserve ALL content from the original text - do not summarize or omit anything
2. Use proper Markdown headings (##, ###, etc.) based on document structure
3. Format tables using Markdown table syntax with | and -
4. Keep all numerical data, dates, and statistics exactly as they appear
5. Preserve Japanese text exactly as written
6. Add appropriate line breaks for readability
7. Do NOT add YAML frontmatter. Do NOT wrap output in code blocks or triple backticks

Output the formatted Markdown content directly."""

    def _get_frontmatter_prompt(self):
        return """Analyze this document and create YAML frontmatter with:
- title: The document title
- summary: A 2-3 sentence summary of the document
- keywords: An array of 10-15 relevant keywords

Output ONLY raw YAML starting with --- and ending with ---. Do NOT use code blocks or triple backticks
Example:
---
title: "Document Title"
summary: "Brief summary here."
keywords: ["keyword1", "keyword2"]
---"""

    def process_file(self, input_path, output_path, doc_type="default"):
        """Process a single PDF file"""
        logger.info(f"Processing: {input_path}")
        result = {"input": input_path, "output": output_path, "chunks": 0, "cost": 0.0}
        
        try:
            # Step 1: Convert PDF to raw text
            logger.info("Step 1: Converting PDF to text with markitdown...")
            md_result = self.markitdown.convert(input_path)
            raw_text = md_result.text_content
            logger.info(f"Extracted {len(raw_text)} characters from PDF")
            
            # Step 2: Split into chunks
            logger.info("Step 2: Splitting into chunks...")
            chunks = self.chunker.chunk(raw_text, doc_type)
            result["chunks"] = len(chunks)
            logger.info(f"Created {len(chunks)} chunks")
            
            # Step 3: Process each chunk with LLM
            logger.info("Step 3: Processing chunks with LLM...")
            processed_chunks = []
            total_cost = 0.0
            structure_prompt = self._get_structure_prompt()
            
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}/{len(chunks)} ({chunk.token_count} tokens)...")
                
                try:
                    resp = self.llm_client.generate(
                        structure_prompt,
                        chunk.content,
                        complexity="complex" if chunk.has_tables else "normal"
                    )
                    processed_chunks.append(self._clean_llm_output(resp.content))
                    total_cost += resp.cost_usd
                    logger.info(f"Chunk {i+1} processed, cost: ${resp.cost_usd:.4f}")
                except Exception as e:
                    logger.warning(f"LLM processing failed for chunk {i+1}, using raw content: {e}")
                    processed_chunks.append(chunk.content)
            
            # Step 4: Generate frontmatter from first chunk
            logger.info("Step 4: Generating frontmatter...")
            try:
                frontmatter_resp = self.llm_client.generate(
                    self._get_frontmatter_prompt(),
                    chunks[0].content[:5000],  # Use first 5000 chars for frontmatter
                    complexity="normal"
                )
                frontmatter = self._clean_llm_output(frontmatter_resp.content)
                total_cost += frontmatter_resp.cost_usd
                
                # Ensure frontmatter has proper delimiters
                if not frontmatter.startswith("---"):
                    frontmatter = "---\n" + frontmatter
                if not frontmatter.endswith("---"):
                    frontmatter = frontmatter + "\n---"
            except Exception as e:
                logger.warning(f"Frontmatter generation failed: {e}")
                frontmatter = "---\ntitle: \"Processed Document\"\n---"
            
            # Step 5: Combine all parts
            logger.info("Step 5: Combining all chunks...")
            final_content = frontmatter + "\n\n" + "\n\n".join(processed_chunks)
            
            # Step 6: Write output
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_content, encoding="utf-8")
            
            result["cost"] = total_cost
            result["status"] = "success"
            logger.info(f"Successfully processed {input_path} -> {output_path}")
            logger.info(f"Total chunks: {len(chunks)}, Total cost: ${total_cost:.4f}")
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Failed to process {input_path}: {e}", exc_info=True)
        
        return result

    def run_queue(self, max_parallel=1):
        """Process all PDFs in the queue (sequential for large files)"""
        input_dir = self.config.get("processing", {}).get("input_dir", "input")
        output_dir = self.config.get("processing", {}).get("output_dir", "output")
        
        self.queue.scan_input_dir(input_dir, output_dir)
        
        # Process sequentially for large PDFs to avoid memory issues
        while True:
            job = self.queue.get_next_job()
            if not job:
                break
            
            self.queue.start_job(job.id)
            result = self.process_file(job.input_path, job.output_path)
            
            if result.get("status") == "success":
                self.queue.complete_job(job.id, result.get("cost", 0))
            else:
                self.queue.fail_job(job.id, result.get("error", "Unknown error"))
        
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
