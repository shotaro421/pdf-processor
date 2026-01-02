"""LLM Client"""
import os, time, logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
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
    def __init__(self, config: LLMConfig):
        self.config = config
        self._setup_client()
    @abstractmethod
    def _setup_client(self): pass
    @abstractmethod
    def generate(self, system_prompt: str, user_prompt: str) -> LLMResponse: pass
    def calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        pricing = self.PRICING.get(model, {"input": 0, "output": 0})
        return (input_tokens / 1_000_000) * pricing["input"] + (output_tokens / 1_000_000) * pricing["output"]

class GeminiClient(BaseLLMClient):
    def _setup_client(self):
        api_key = self.config.api_key or os.environ.get("GOOGLE_API_KEY")
        if not api_key: raise ValueError("GOOGLE_API_KEY not found")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(self.config.model)
    def generate(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        start_time = time.time()
        full_prompt = system_prompt + "\n\n---\n\n" + user_prompt
        response = self.model.generate_content(full_prompt, generation_config=genai.types.GenerationConfig(max_output_tokens=self.config.max_tokens, temperature=self.config.temperature))
        latency_ms = (time.time() - start_time) * 1000
        input_tokens = response.usage_metadata.prompt_token_count
        output_tokens = response.usage_metadata.candidates_token_count
        return LLMResponse(content=response.text, model=self.config.model, provider="gemini", input_tokens=input_tokens, output_tokens=output_tokens, cost_usd=self.calculate_cost(self.config.model, input_tokens, output_tokens), latency_ms=latency_ms)

class OpenAIClient(BaseLLMClient):
    def _setup_client(self):
        api_key = self.config.api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key: raise ValueError("OPENAI_API_KEY not found")
        self.client = OpenAI(api_key=api_key)
    def generate(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        start_time = time.time()
        response = self.client.chat.completions.create(model=self.config.model, messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], max_tokens=self.config.max_tokens, temperature=self.config.temperature)
        latency_ms = (time.time() - start_time) * 1000
        return LLMResponse(content=response.choices[0].message.content, model=self.config.model, provider="openai", input_tokens=response.usage.prompt_tokens, output_tokens=response.usage.completion_tokens, cost_usd=self.calculate_cost(self.config.model, response.usage.prompt_tokens, response.usage.completion_tokens), latency_ms=latency_ms)

class AnthropicClient(BaseLLMClient):
    def _setup_client(self):
        api_key = self.config.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key: raise ValueError("ANTHROPIC_API_KEY not found")
        self.client = anthropic.Anthropic(api_key=api_key)
    def generate(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        start_time = time.time()
        response = self.client.messages.create(model=self.config.model, max_tokens=self.config.max_tokens, system=system_prompt, messages=[{"role": "user", "content": user_prompt}])
        latency_ms = (time.time() - start_time) * 1000
        return LLMResponse(content=response.content[0].text, model=self.config.model, provider="anthropic", input_tokens=response.usage.input_tokens, output_tokens=response.usage.output_tokens, cost_usd=self.calculate_cost(self.config.model, response.usage.input_tokens, response.usage.output_tokens), latency_ms=latency_ms)

class LLMClientFactory:
    _clients = {LLMProvider.GEMINI: GeminiClient, LLMProvider.OPENAI: OpenAIClient, LLMProvider.ANTHROPIC: AnthropicClient}
    @classmethod
    def create(cls, config: LLMConfig) -> BaseLLMClient:
        client_class = cls._clients.get(config.provider)
        if not client_class: raise ValueError(f"Unknown provider: {config.provider}")
        return client_class(config)

class MultiLLMClient:
    def __init__(self, configs: Dict[str, LLMConfig], retry_attempts: int = 3):
        self.configs = configs
        self.retry_attempts = retry_attempts
        self.clients: Dict[str, BaseLLMClient] = {}
        self._initialize_clients()
    def _initialize_clients(self):
        for name, config in self.configs.items():
            try:
                self.clients[name] = LLMClientFactory.create(config)
                logger.info(f"Initialized {name} client: {config.model}")
            except Exception as e:
                logger.warning(f"Failed to initialize {name} client: {e}")
    def generate(self, system_prompt: str, user_prompt: str, preferred_client: str = "primary", complexity: str = "normal") -> LLMResponse:
        if complexity == "complex" and "secondary" in self.clients: preferred_client = "secondary"
        order = self._get_fallback_order(preferred_client)
        last_error = None
        for client_name in order:
            if client_name not in self.clients: continue
            client = self.clients[client_name]
            for attempt in range(self.retry_attempts):
                try:
                    response = client.generate(system_prompt, user_prompt)
                    return response
                except Exception as e:
                    last_error = e
                    if attempt < self.retry_attempts - 1: time.sleep(2 ** attempt)
        raise RuntimeError(f"All LLM clients failed. Last error: {last_error}")
    def _get_fallback_order(self, preferred: str) -> List[str]:
        order = [preferred]
        for name in ["primary", "secondary", "fallback"]:
            if name not in order: order.append(name)
        return order

def create_multi_llm_client_from_config(config: Dict) -> MultiLLMClient:
    llm_config = config.get("llm", {})
    configs = {}
    for key in ["primary", "secondary", "fallback"]:
        if key in llm_config:
            cfg = llm_config[key]
            configs[key] = LLMConfig(provider=LLMProvider(cfg["provider"]), model=cfg["model"], max_tokens=cfg.get("max_tokens", 8192), temperature=cfg.get("temperature", 0))
    return MultiLLMClient(configs, config.get("processing", {}).get("retry_attempts", 3))

