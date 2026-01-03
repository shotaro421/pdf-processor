"""Chunker"""
import re, logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum

logger = logging.getLogger(__name__)

class ChunkType(Enum):
    TEXT = "text"
    TABLE = "table"

@dataclass
class Chunk:
    content: str
    chunk_type: ChunkType
    index: int
    section_path: List[str] = field(default_factory=list)
    token_count: int = 0
    has_tables: bool = False

@dataclass
class ChunkingConfig:
    max_tokens_per_chunk: int = 30000
    overlap_tokens: int = 500
    preserve_tables: bool = True
    preserve_sections: bool = True

class TokenEstimator:
    @staticmethod
    def estimate(text):
        jp = len(re.findall(r"[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]", text))
        return int(jp * 1.5 + (len(text) - jp) * 0.25)

class DocumentChunker:
    def __init__(self, config):
        self.config = config
        self.estimator = TokenEstimator()
    def chunk(self, text, doc_type="default"):
        sections = self._detect_sections(text)
        chunks = self._split(sections)
        logger.info(f"Split into {len(chunks)} chunks")
        return chunks
    def _detect_sections(self, text):
        sections = []
        pattern = r"^(#{1,6})\s+(.+)$"
        pos = 0
        for m in re.finditer(pattern, text, re.MULTILINE):
            sections.append((m.group(2).strip(), text[pos:m.start()], len(m.group(1))))
            pos = m.start()
        if pos < len(text): sections.append(("END", text[pos:], 0))
        return sections if sections else [("MAIN", text, 1)]
    def _split(self, sections):
        chunks = []
        content = ""
        tokens = 0
        idx = 0
        path = []
        for title, sec_content, level in sections:
            if level > 0: path = path[:level-1] + [title]
            sec_tokens = self.estimator.estimate(sec_content)
            if tokens + sec_tokens > self.config.max_tokens_per_chunk:
                if content:
                    chunks.append(Chunk(content=content, chunk_type=ChunkType.TEXT, index=idx, section_path=path.copy(), token_count=tokens, has_tables="|" in content))
                    idx += 1
                content = sec_content
                tokens = sec_tokens
            else:
                content += sec_content
                tokens += sec_tokens
        if content:
            chunks.append(Chunk(content=content, chunk_type=ChunkType.TEXT, index=idx, section_path=path.copy(), token_count=tokens))
        return chunks

def create_chunker_from_config(config):
    c = config.get("chunking", {})
    return DocumentChunker(ChunkingConfig(max_tokens_per_chunk=c.get("max_tokens_per_chunk", 30000), overlap_tokens=c.get("overlap_tokens", 500)))

