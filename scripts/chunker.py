"""Chunker - Token-based document splitting"""
import re
import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)

@dataclass
class Chunk:
    content: str
    index: int
    token_count: int = 0
    has_tables: bool = False
    section_path: List[str] = field(default_factory=list)

@dataclass
class ChunkingConfig:
    max_tokens_per_chunk: int = 30000
    overlap_tokens: int = 500
    preserve_tables: bool = True

class TokenEstimator:
    @staticmethod
    def estimate(text):
        # Japanese characters count as ~1.5 tokens, others as ~0.25
        jp_chars = len(re.findall(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))
        other_chars = len(text) - jp_chars
        return int(jp_chars * 1.5 + other_chars * 0.25)

class DocumentChunker:
    def __init__(self, config: ChunkingConfig):
        self.config = config
        self.estimator = TokenEstimator()

    def chunk(self, text: str, doc_type: str = "default") -> List[Chunk]:
        """Split text into chunks based on token limits"""
        chunks = []
        
        # Split by paragraphs (double newlines) or single newlines
        paragraphs = re.split(r'\n\n+', text)
        
        current_content = ""
        current_tokens = 0
        chunk_index = 0
        
        for para in paragraphs:
            para_tokens = self.estimator.estimate(para)
            
            # If single paragraph exceeds limit, split it further
            if para_tokens > self.config.max_tokens_per_chunk:
                # Save current chunk if exists
                if current_content.strip():
                    chunks.append(self._create_chunk(current_content, chunk_index, current_tokens))
                    chunk_index += 1
                    current_content = ""
                    current_tokens = 0
                
                # Split large paragraph by lines
                lines = para.split('\n')
                for line in lines:
                    line_tokens = self.estimator.estimate(line)
                    
                    if current_tokens + line_tokens > self.config.max_tokens_per_chunk:
                        if current_content.strip():
                            chunks.append(self._create_chunk(current_content, chunk_index, current_tokens))
                            chunk_index += 1
                        current_content = line + "\n"
                        current_tokens = line_tokens
                    else:
                        current_content += line + "\n"
                        current_tokens += line_tokens
            
            # If adding this paragraph exceeds limit, start new chunk
            elif current_tokens + para_tokens > self.config.max_tokens_per_chunk:
                if current_content.strip():
                    chunks.append(self._create_chunk(current_content, chunk_index, current_tokens))
                    chunk_index += 1
                current_content = para + "\n\n"
                current_tokens = para_tokens
            else:
                current_content += para + "\n\n"
                current_tokens += para_tokens
        
        # Don't forget the last chunk
        if current_content.strip():
            chunks.append(self._create_chunk(current_content, chunk_index, current_tokens))
        
        logger.info(f"Split document into {len(chunks)} chunks")
        for i, chunk in enumerate(chunks):
            logger.info(f"  Chunk {i}: {chunk.token_count} tokens, {len(chunk.content)} chars")
        
        return chunks

    def _create_chunk(self, content: str, index: int, tokens: int) -> Chunk:
        return Chunk(
            content=content.strip(),
            index=index,
            token_count=tokens,
            has_tables='|' in content and '-|-' in content
        )

def create_chunker_from_config(config: dict) -> DocumentChunker:
    c = config.get("chunking", {})
    return DocumentChunker(ChunkingConfig(
        max_tokens_per_chunk=c.get("max_tokens_per_chunk", 30000),
        overlap_tokens=c.get("overlap_tokens", 500)
    ))
