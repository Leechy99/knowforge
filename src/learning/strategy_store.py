"""
Strategy Store - Store and manage learned processing strategies
"""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from src.utils.time import utc_now


class LearnedStrategy(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy_name: str
    applicable_patterns: dict[str, Any] = Field(default_factory=dict)
    success_count: int = 0
    failure_count: int = 0
    parser_config: dict[str, Any] = Field(default_factory=dict)
    preprocessing_steps: list[str] = Field(default_factory=list)
    postprocessing_rules: dict[str, Any] = Field(default_factory=dict)
    avg_quality_score: float = 0.0
    avg_processing_time_ms: int = 0
    is_active: bool = True
    is_verified: bool = False
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class StrategyStore:
    def __init__(self) -> None:
        self.strategies: dict[str, LearnedStrategy] = {}

    def record_success(
        self,
        strategy_name: str,
        quality_score: float,
        processing_time_ms: int,
    ) -> None:
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            strategy.success_count += 1
            n = strategy.success_count
            strategy.avg_quality_score = (strategy.avg_quality_score * (n - 1) + quality_score) / n
            strategy.avg_processing_time_ms = int(
                (strategy.avg_processing_time_ms * (n - 1) + processing_time_ms) / n
            )
        else:
            self.strategies[strategy_name] = LearnedStrategy(
                strategy_name=strategy_name,
                success_count=1,
                avg_quality_score=quality_score,
                avg_processing_time_ms=processing_time_ms,
            )
        self.strategies[strategy_name].updated_at = utc_now()

    def record_failure(self, strategy_name: str) -> None:
        if strategy_name in self.strategies:
            self.strategies[strategy_name].failure_count += 1
            self.strategies[strategy_name].updated_at = utc_now()

    def get_strategy(self, strategy_name: str) -> LearnedStrategy | None:
        return self.strategies.get(strategy_name)

    def get_all_strategies(self) -> list[LearnedStrategy]:
        return sorted(
            self.strategies.values(),
            key=lambda s: s.success_count / max(s.success_count + s.failure_count, 1),
            reverse=True,
        )

    def learn_from_feedback(
        self,
        feedback_strategy: str,
        patterns: dict[str, Any],
        config: dict[str, Any],
    ) -> LearnedStrategy:
        strategy = LearnedStrategy(
            strategy_name=feedback_strategy,
            applicable_patterns=patterns,
            parser_config=config,
            is_verified=False,
        )
        self.strategies[feedback_strategy] = strategy
        return strategy
