"""Chunker - Intelligent Document Chunking"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class ChunkType(Enum):
    TEXT = "text"
    TABLE = "table"
    FIGURE = "figure"
    HEADER = "header"


@dataclass
class Chunk:
    content: str
    chunk_type: ChunkType
    index: int
    page_start: Optional[int] = None
    page_end: Optional[int] = None
    section_path: List[str] = field(default_factory=list)
    token_count: int = 0
    has_tables: bool = False
    has_figures: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChunkingConfig:
    max_tokens_per_chunk: int = 30000
    overlap_tokens: int = 500
    preserve_tables: bool = True
    preserve_sections: bool = True
    section_markers: List[str] = field(default_factory=list)


class TokenEstimator:
    @staticmethod
    def estimate(text: str) -> int:
        jp_chars = len(re.findall(r"[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]", text))
        en_chars = len(text) - jp_chars
        return int(jp_chars * 1.5 + en_chars * 0.25)


class DocumentChunker:
    def __init__(self, config: ChunkingConfig):
        self.config = config
        self.estimator = TokenEstimator()

    def chunk(self, text: str, doc_type: str = "default") -> List[Chunk]:
        sections = self._detect_sections(text)
        text_marked, special_blocks = self._mark_special_blocks(text)
        chunks = self._split_into_chunks(sections, special_blocks)
        logger.info(f"Split document into {len(chunks)} chunks")
        return chunks

    def _detect_sections(self, text: str) -> List[Tuple[str, str, int]]:
        sections = []
        pattern = r"^(#{1,6})\s+(.+)$"
        current_pos = 0
        for match in re.finditer(pattern, text, re.MULTILINE):
            level = len(match.group(1))
            title = match.group(2).strip()
            sections.append((title, text[current_pos:match.start()], level))
            current_pos = match.start()
        if current_pos < len(text):
            sections.append(("END", text[current_pos:], 0))
        return sections if sections else [("MAIN", text, 1)]

    def _mark_special_blocks(self, text: str) -> Tuple[str, Dict[str, str]]:
        special_blocks = {}
        return text, special_blocks

    def _split_into_chunks(self, sections, special_blocks) -> List[Chunk]:
        chunks = []
        current_content = ""
        current_tokens = 0
        chunk_idx = 0
        section_path = []
        for title, content, level in sections:
            if level > 0:
                section_path = section_path[:level-1] + [title]
            content_tokens = self.estimator.estimate(content)
            if current_tokens + content_tokens > self.config.max_tokens_per_chunk:
                if current_content:
                    chunks.append(Chunk(
                        content=current_content, chunk_type=ChunkType.TEXT,
                        index=chunk_idx, section_path=section_path.copy(),
                        token_count=current_tokens, has_tables="|" in current_content
                    ))
                    chunk_idx += 1
                current_content = content
                current_tokens = content_tokens
            else:
                current_content += content
                current_tokens += content_tokens
        if current_content:
            chunks.append(Chunk(
                content=current_content, chunk_type=ChunkType.TEXT,
                index=chunk_idx, section_path=section_path.copy(),
                token_count=current_tokens
            ))
        return chunks


def create_chunker_from_config(config: Dict) -> DocumentChunker:
    chunk_config = config.get("chunking", {})
    return DocumentChunker(ChunkingConfig(
        max_tokens_per_chunk=chunk_config.get("max_tokens_per_chunk", 30000),
        overlap_tokens=chunk_config.get("overlap_tokens", 500),
        preserve_tables=chunk_config.get("preserve_tables", True),
        preserve_sections=chunk_config.get("preserve_sections", True)
    ))
