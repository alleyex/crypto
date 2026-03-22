"""REINFORCE (Monte-Carlo policy gradient) agent — pure Python.

Policy
------
Binary softmax policy parameterised as a linear-sigmoid:

    P(LONG | s) = sigmoid(w · s + b)
    P(HOLD | s) = 1 - P(LONG | s)

Training (REINFORCE with mean-return baseline)
----------------------------------------------
For each episode:
  1. Roll out: collect (s_t, a_t, r_t) tuples.
  2. Compute discounted returns G_t = Σ_{k≥t} γ^{k-t} r_k.
  3. Subtract baseline b = mean(G_t) to reduce variance.
  4. Update: θ += α * Σ_t (G_t - b) * ∇_θ log π(a_t | s_t)

Gradient of log-policy:
  a=1  → ∇_w log π = (1 - p) * s,  ∇_b = (1 - p)
  a=0  → ∇_w log π = -p * s,       ∇_b = -p
"""

import math
import random
from typing import Any, Dict, List, Tuple

from app.rl.environment import TradingEnv


class ReinforceAgent:
    def __init__(
        self,
        n_features: int,
        learning_rate: float = 1e-3,
        gamma: float = 1.0,
        seed: int = 42,
    ) -> None:
        self.n_features = n_features
        self.lr = learning_rate
        self.gamma = gamma
        self._rng = random.Random(seed)

        # Xavier init
        scale = math.sqrt(2.0 / n_features) if n_features > 0 else 1.0
        self.weights = [self._rng.gauss(0, scale) for _ in range(n_features)]
        self.bias = 0.0

    # ------------------------------------------------------------------
    # Policy
    # ------------------------------------------------------------------

    def _sigmoid(self, z: float) -> float:
        if z >= 0:
            return 1.0 / (1.0 + math.exp(-z))
        e = math.exp(z)
        return e / (1.0 + e)

    def action_prob(self, obs: List[float]) -> float:
        """Return P(LONG | obs)."""
        z = sum(w * x for w, x in zip(self.weights, obs)) + self.bias
        return self._sigmoid(z)

    def select_action(self, obs: List[float]) -> Tuple[int, float]:
        """Sample action from policy. Returns (action, log_prob)."""
        p = self.action_prob(obs)
        action = 1 if self._rng.random() < p else 0
        p_clamped = max(1e-12, min(1.0 - 1e-12, p))
        log_prob = math.log(p_clamped) if action == 1 else math.log(1.0 - p_clamped)
        return action, log_prob

    def greedy_action(self, obs: List[float]) -> int:
        """Deterministic action (argmax policy)."""
        return 1 if self.action_prob(obs) >= 0.5 else 0

    # ------------------------------------------------------------------
    # REINFORCE update
    # ------------------------------------------------------------------

    def _discounted_returns(self, rewards: List[float]) -> List[float]:
        G = 0.0
        returns = []
        for r in reversed(rewards):
            G = r + self.gamma * G
            returns.append(G)
        returns.reverse()
        return returns

    def update(
        self,
        observations: List[List[float]],
        actions: List[int],
        rewards: List[float],
    ) -> float:
        """Run one REINFORCE update. Returns mean absolute policy loss."""
        returns = self._discounted_returns(rewards)
        baseline = sum(returns) / len(returns) if returns else 0.0

        grad_w = [0.0] * self.n_features
        grad_b = 0.0
        loss_sum = 0.0

        for obs, action, G in zip(observations, actions, returns):
            advantage = G - baseline
            p = self.action_prob(obs)

            if action == 1:
                # ∇ log π = (1-p) * obs
                for j in range(self.n_features):
                    grad_w[j] += advantage * (1.0 - p) * obs[j]
                grad_b += advantage * (1.0 - p)
            else:
                # ∇ log π = -p * obs
                for j in range(self.n_features):
                    grad_w[j] += advantage * (-p) * obs[j]
                grad_b += advantage * (-p)

            p_c = max(1e-12, min(1.0 - 1e-12, p))
            log_p = math.log(p_c) if action == 1 else math.log(1.0 - p_c)
            loss_sum += -advantage * log_p

        n = len(observations)
        # Gradient ascent (policy gradient maximises expected return)
        for j in range(self.n_features):
            self.weights[j] += self.lr * grad_w[j] / n
        self.bias += self.lr * grad_b / n

        return loss_sum / n if n > 0 else 0.0

    # ------------------------------------------------------------------
    # Full episode rollout
    # ------------------------------------------------------------------

    def run_episode(self, env: TradingEnv, train: bool = True) -> Dict[str, Any]:
        """Roll out one full episode. If train=True, update weights."""
        obs = env.reset()
        observations: List[List[float]] = []
        actions: List[int] = []
        rewards: List[float] = []

        while True:
            if train:
                action, _ = self.select_action(obs)
            else:
                action = self.greedy_action(obs)

            observations.append(obs)
            actions.append(action)
            next_obs, reward, done = env.step(action)
            rewards.append(reward)

            if done:
                break
            obs = next_obs

        loss = self.update(observations, actions, rewards) if train else None

        return {
            "rewards": rewards,
            "actions": actions,
            "loss": loss,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_type": "reinforce_v1",
            "weights": list(self.weights),
            "bias": self.bias,
            "n_features": self.n_features,
            "learning_rate": self.lr,
            "gamma": self.gamma,
        }
