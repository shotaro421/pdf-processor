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
    max_tokens_per_chunk: int = 8000
    overlap_tokens: int = 200
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
        """ドキュメントをチャンクに分割"""
        if not text.strip():
            return []
        
        total_tokens = self.estimator.estimate(text)
        logger.info(f"Total tokens: {total_tokens}, max per chunk: {self.config.max_tokens_per_chunk}")
        
        if total_tokens <= self.config.max_tokens_per_chunk:
            return [Chunk(
                content=text,
                chunk_type=ChunkType.TEXT,
                index=0,
                token_count=total_tokens,
                has_tables="|" in text and "---" in text
            )]
        
        chunks = self._split_by_paragraphs(text)
        logger.info(f"Split document into {len(chunks)} chunks")
        return chunks

    def _split_by_paragraphs(self, text: str) -> List[Chunk]:
        """段落ベースでテキストを分割"""
        paragraphs = re.split(r'\n\s*\n', text)
        
        chunks = []
        current_content = ""
        current_tokens = 0
        chunk_idx = 0
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            para_tokens = self.estimator.estimate(para)
            
            if para_tokens > self.config.max_tokens_per_chunk:
                if current_content:
                    chunks.append(self._create_chunk(current_content, chunk_idx, current_tokens))
                    chunk_idx += 1
                    current_content = ""
                    current_tokens = 0
                
                sub_chunks = self._split_large_paragraph(para, chunk_idx)
                chunks.extend(sub_chunks)
                chunk_idx += len(sub_chunks)
                continue
            
            if current_tokens + para_tokens > self.config.max_tokens_per_chunk:
                if current_content:
                    chunks.append(self._create_chunk(current_content, chunk_idx, current_tokens))
                    chunk_idx += 1
                current_content = para
                current_tokens = para_tokens
            else:
                if current_content:
                    current_content += "\n\n" + para
                else:
                    current_content = para
                current_tokens += para_tokens
        
        if current_content:
            chunks.append(self._create_chunk(current_content, chunk_idx, current_tokens))
        
        return chunks

    def _split_large_paragraph(self, para: str, start_idx: int) -> List[Chunk]:
        """大きな段落を行単位で分割"""
        lines = para.split('\n')
        chunks = []
        current_content = ""
        current_tokens = 0
        chunk_idx = start_idx
        
        for line in lines:
            line_tokens = self.estimator.estimate(line)
            
            if line_tokens > self.config.max_tokens_per_chunk:
                if current_content:
                    chunks.append(self._create_chunk(current_content, chunk_idx, current_tokens))
                    chunk_idx += 1
                    current_content = ""
                    current_tokens = 0
                
                char_chunks = self._split_by_chars(line, chunk_idx)
                chunks.extend(char_chunks)
                chunk_idx += len(char_chunks)
                continue
            
            if current_tokens + line_tokens > self.config.max_tokens_per_chunk:
                if current_content:
                    chunks.append(self._create_chunk(current_content, chunk_idx, current_tokens))
                    chunk_idx += 1
                current_content = line
                current_tokens = line_tokens
            else:
                if current_content:
                    current_content += "\n" + line
                else:
                    current_content = line
                current_tokens += line_tokens
        
        if current_content:
            chunks.append(self._create_chunk(current_content, chunk_idx, current_tokens))
        
        return chunks

    def _split_by_chars(self, text: str, start_idx: int) -> List[Chunk]:
        """文字数ベースで分割（最終手段）"""
        chunks = []
        chunk_idx = start_idx
        chars_per_chunk = int(self.config.max_tokens_per_chunk / 1.5)
        
        for i in range(0, len(text), chars_per_chunk):
            chunk_text = text[i:i + chars_per_chunk]
            chunks.append(self._create_chunk(
                chunk_text, 
                chunk_idx, 
                self.estimator.estimate(chunk_text)
            ))
            chunk_idx += 1
        
        return chunks

    def _create_chunk(self, content: str, index: int, token_count: int) -> Chunk:
        """チャンクオブジェクトを作成"""
        has_tables = bool(re.search(r'\|.*\|.*\|', content))
        return Chunk(
            content=content,
            chunk_type=ChunkType.TEXT,
            index=index,
            token_count=token_count,
            has_tables=has_tables
        )


def create_chunker_from_config(config: Dict) -> DocumentChunker:
    chunk_config = config.get("chunking", {})
    return DocumentChunker(ChunkingConfig(
        max_tokens_per_chunk=chunk_config.get("max_tokens_per_chunk", 8000),
        overlap_tokens=chunk_config.get("overlap_tokens", 200),
        preserve_tables=chunk_config.get("preserve_tables", True),
        preserve_sections=chunk_config.get("preserve_sections", True)
    ))
