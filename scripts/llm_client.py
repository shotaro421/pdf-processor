"""LLM Client"""
import os, time, logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, List
from enum import Enum
import google.generativeai as genai
from openai import OpenAI
import anthropic

logger = logging.getLogger(__name__)

class LLMProvider(Enum):
    GEMINI = "gemini"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"

@dataclass
class LLMResponse:
    content: str
    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    latency_ms: float

@dataclass
class LLMConfig:
    provider: LLMProvider
    model: str
    max_tokens: int = 8192
    temperature: float = 0
    api_key: Optional[str] = None

class BaseLLMClient(ABC):
    PRICING = {"gemini-2.0-flash": {"input": 0.10, "output": 0.40}, "gemini-2.5-pro": {"input": 1.25, "output": 5.00}, "gpt-4o-mini": {"input": 0.15, "output": 0.60}}
    def __init__(self, config): self.config = config; self._setup_client()
    @abstractmethod
    def _setup_client(self): pass
    @abstractmethod
    def generate(self, system_prompt, user_prompt): pass
    def calculate_cost(self, model, input_tokens, output_tokens):
        p = self.PRICING.get(model, {"input": 0, "output": 0})
        return (input_tokens / 1000000) * p["input"] + (output_tokens / 1000000) * p["output"]

class GeminiClient(BaseLLMClient):
    def _setup_client(self):
        api_key = self.config.api_key or os.environ.get("GOOGLE_API_KEY")
        if not api_key: raise ValueError("GOOGLE_API_KEY not found")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(self.config.model)
    def generate(self, system_prompt, user_prompt):
        start = time.time()
        prompt = system_prompt + "\n\n---\n\n" + user_prompt
        resp = self.model.generate_content(prompt, generation_config=genai.types.GenerationConfig(max_output_tokens=self.config.max_tokens, temperature=self.config.temperature))
        latency = (time.time() - start) * 1000
        inp = resp.usage_metadata.prompt_token_count
        out = resp.usage_metadata.candidates_token_count
        return LLMResponse(content=resp.text, model=self.config.model, provider="gemini", input_tokens=inp, output_tokens=out, cost_usd=self.calculate_cost(self.config.model, inp, out), latency_ms=latency)

class OpenAIClient(BaseLLMClient):
    def _setup_client(self):
        api_key = self.config.api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key: raise ValueError("OPENAI_API_KEY not found")
        self.client = OpenAI(api_key=api_key)
    def generate(self, system_prompt, user_prompt):
        start = time.time()
        resp = self.client.chat.completions.create(model=self.config.model, messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], max_tokens=self.config.max_tokens, temperature=self.config.temperature)
        latency = (time.time() - start) * 1000
        return LLMResponse(content=resp.choices[0].message.content, model=self.config.model, provider="openai", input_tokens=resp.usage.prompt_tokens, output_tokens=resp.usage.completion_tokens, cost_usd=self.calculate_cost(self.config.model, resp.usage.prompt_tokens, resp.usage.completion_tokens), latency_ms=latency)

class AnthropicClient(BaseLLMClient):
    def _setup_client(self):
        api_key = self.config.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key: raise ValueError("ANTHROPIC_API_KEY not found")
        self.client = anthropic.Anthropic(api_key=api_key)
    def generate(self, system_prompt, user_prompt):
        start = time.time()
        resp = self.client.messages.create(model=self.config.model, max_tokens=self.config.max_tokens, system=system_prompt, messages=[{"role": "user", "content": user_prompt}])
        latency = (time.time() - start) * 1000
        return LLMResponse(content=resp.content[0].text, model=self.config.model, provider="anthropic", input_tokens=resp.usage.input_tokens, output_tokens=resp.usage.output_tokens, cost_usd=self.calculate_cost(self.config.model, resp.usage.input_tokens, resp.usage.output_tokens), latency_ms=latency)

class LLMClientFactory:
    _clients = {LLMProvider.GEMINI: GeminiClient, LLMProvider.OPENAI: OpenAIClient, LLMProvider.ANTHROPIC: AnthropicClient}
    @classmethod
    def create(cls, config):
        client_class = cls._clients.get(config.provider)
        if not client_class: raise ValueError(f"Unknown provider: {config.provider}")
        return client_class(config)

class MultiLLMClient:
    def __init__(self, configs, retry_attempts=3):
        self.configs = configs
        self.retry_attempts = retry_attempts
        self.clients = {}
        self._init_clients()
    def _init_clients(self):
        for name, config in self.configs.items():
            try: self.clients[name] = LLMClientFactory.create(config)
            except Exception as e: logger.warning(f"Failed to init {name}: {e}")
    def generate(self, system_prompt, user_prompt, preferred="primary", complexity="normal"):
        if complexity == "complex" and "secondary" in self.clients: preferred = "secondary"
        order = [preferred] + [n for n in ["primary", "secondary", "fallback"] if n != preferred]
        last_error = None
        for name in order:
            if name not in self.clients: continue
            for attempt in range(self.retry_attempts):
                try: return self.clients[name].generate(system_prompt, user_prompt)
                except Exception as e: last_error = e; time.sleep(2 ** attempt) if attempt < self.retry_attempts - 1 else None
        raise RuntimeError(f"All failed: {last_error}")

def create_multi_llm_client_from_config(config):
    llm_config = config.get("llm", {})
    configs = {}
    for key in ["primary", "secondary", "fallback"]:
        if key in llm_config:
            cfg = llm_config[key]
            configs[key] = LLMConfig(provider=LLMProvider(cfg["provider"]), model=cfg["model"], max_tokens=cfg.get("max_tokens", 8192), temperature=cfg.get("temperature", 0))
    return MultiLLMClient(configs, config.get("processing", {}).get("retry_attempts", 3))

