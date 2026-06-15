# CostGuard: a smart actual inference cost minimization scorer

**Authors:** David Breitgand (IBM), Ronen Kat (IBM)

**ProposalStatus:** proposed

**Based on:** offline Python implementation POC using [the traces published here](https://github.com/lchen001/pricing-reversal/blob/main/README.md).

## Executive summary

This document describes the design for a pluggable actual inference cost-aware model scorer for the `ModelSelector` [architecture](https://github.com/ms-llmd/llm-d-inference-payload-processor/tree/main/docs/proposals/043-model-selection-framework) of Inference Payload Processor (IPP).

## General: awareness to the actual cost of inference is critical

The cost of inference has become a critical bottleneck in generative AI and especially for reasoning models and agentic AI. Unlike traditional software that scales with predictable, flat infrastructure costs, the new wave of generative AI incurs highly variable and unpredictable expenses because every prompt produces a different number of output tokens when sent to a different model, and the number of output tokens is unpredictable.

Indeed, as shown in the [recent study](https://arxiv.org/abs/2603.23971), when reasoning models of comparable capabilities are prompted with the same prompt, this results in highly unpredictable costs. Furthermore, the study shows that even the same prompt repeated to the same inference SaaS vendor at different times, might result in wildly different numbers of output tokens.

This financial unpredictability is additionally exacerbated by the rise of agentic AI. An agentic harness can execute a variable number of internal loops and multi-model reasoning steps for the exact same initial prompt, turning what used to be a stable software expense into a runaway one.

## Problem

The objective is to evaluate model candidates from a filtered candidate set and select the most cost-effective model to serve incoming requests based on the **actual** cost of inference.

This stands in contrast to the typical greedy approach, which scores model candidates for a given request using the cost of **input** tokens as a static proxy for the actual cost of inference. For instance, this greedy heuristic is the current implementation of the [costaware scorer](https://github.com/ms-llmd/llm-d-inference-payload-processor/blob/main/pkg/framework/plugins/modelselector/scorer/costaware/plugin.go) in IPP.

The core flaw in this strategy is that a model with a lower input token price may generate a significantly larger volume of output tokens than a model with more expensive input rates. Because output tokens are usually priced much higher than the input tokens, these verbose generations can dominate the total expense, ultimately making the seemingly cheaper model far more expensive in practice. Consequently, routing decisions must be guided by actual end-to-end costs. However, achieving this is non-trivial, as on the **per request** basis, actual costs are highly unpredictable and can only be discovered after the request is served.

## CostGuard approach summary

In alignment with the `ModelSelector` [architecture](https://github.com/ms-llmd/llm-d-inference-payload-processor/tree/main/docs/proposals/043-model-selection-framework), **CostGuard** assigns each model candidate a real value score in [0,1] with higher-is-better semantics meaning that the model that received the highest score is most cost-efficient.

Per incoming inference request, the **CostGuard** scorer ranks models in the candidate set of models such that (a) expected total cost of inference over all inference requests in a time window is minimized and (b) the tail cost of inference is minimized. This way, **CostGuard** aims at keeping the total inference cost down over multiple requests while simultaneously striving to minimize impact of an outlier cost on any given request.

Algorithmically **CostGuard** is a variation on a well-known [Multi-Armed Bandit problem](https://en.wikipedia.org/wiki/Multi-armed_bandit#The_multi-armed_bandit_model), employing $\epsilon$-Greedy exploration-exploitation strategy, in which most of the time, the scorer ranks highest (i.e., exploits) a model with minimal known actual cost ties broken arbitrarily, but with a small probability $\epsilon$, **CostGuard** explores other models randomly.

## Goals

- **Minimize total cost** of inference over the stream of inference requests by scoring filtered candidate models, treating each model as a bandit arm whose draws are per-prompt costs;
- **Control both halves of the cost distribution simultaneously**: the body
  (typical draw) and the tail (rare expensive draw) without forcing the
  user to pick a hard cost threshold, which is unknown;
- **Assign score in [0,1]** so that for the model set having a large spread of actual $ costs, the score discrimination in [0,1] would be preserved, and for the model set with close $ costs, the scores will be spread further apart to provide discriminative power for the score. Avoid extreme scoring to exclude models too early if other scorers are deployed;
- **Variable set of models** should be handled seamlessly.

## Design Principles

- **Simplicity** intuitive and easy to tune heuristic, demonstrably better performance than input price based scorer, but no optimality guarantees
- **Simple configuration:** one tail knob, one tail-weight knob, one exploration knob to keep the policy small enough to reason about and tune by hand
- **Contained behavior:** no assumptions on other scorers or pickers configured in a pipeline; all behavior contained in the scorer itself
- **Pluggable** into the existing Filter->Scorer->Picker framework
- **Integrates with the existing datalayer framework**
- **No prior knowledge** on inference requests is assumed
- **No prior knowledge** on model candidates is assumed
- **No knowledge** on accuracy of models either offline or at runtime
- **Memory efficient**: don't store requests
- **Sensitive to outliers**: use a self adjusting tdigest rather than a fixed bin structure histogram
- **Fast Decisions**
- **Not tightly coupled to a single method of scoring**
- **Do not assume a classifier** of an inference request (intent, domain, complexity, etc.)

## Non-goals

- **Optimal regret bounds (i.e., a regret for exploring a specific model)** CostGuard uses fixed-schedule $\epsilon$-Greedy exploration for simplicity, computational efficiency, and robustness to drift.
- **Adapting to non-stationary cost distributions.** In the initial version, tdigest grows unboundedly and old samples are weighted equally with new ones. A better management of tdigest is deferred to the future work.
- **Hard cost / budget guarantees.** CostGuard assigns a score to the model with minimal body+tail cost based on the previous observations stored in the tdigest in the compressed memory-efficient form. CostGuard does not enforce per-request or per-batch budget caps and does not necessarily minimize a single request cost. It works on the total cost minimization. CostGuard does not minimize the risk for a single request.
- **Accuracy maximization.** The objective is cost. If the models in the candidate set are compatible in terms of accuracy (this is the assumption), then the scorer will not be less accurate than the least accurate model in the set and can actually be more accurate than the most cost-efficient model in the set thanks to the $\epsilon$-Greedy exploration.
- **Cross-arm sample sharing.** Each arm (i.e., model) maintains its own tdigest. Samples observed for arm `i` never inform arm `j`'s estimates.

## Design

### Algorithm

Per time window (i.e., epoch)

**Initialization:**

- load models list;
- load cost matrix;
- tdigest per model (compression default=200);
- warmup counter per model = 0;
- rank per model = 0;
- time window (epoch) = default
  
**Per epoch:**

**Warmup:** for the incoming request, assign the highest score (i.e., 1) to a random model and score other models neutrally (0.5). Note that this is a heuristic warmup, because it is not guaranteed that the model scored 1 will answer the request. Whichever model answers, on the response path, the tdigest of this model is updated.
  
**Exploration:** if there is an under-explored model (i.e., a model that didn't receive minimal warmup requests yet), score 1 the least explored model, ties broken arbitrarily, otherwise with probability $\epsilon$ score a random model 1 and score all the rest 0.5. On the response path, update the model cost tdigest from `usage`, increment the warmup counter for this model by one, update the rank for this model. Again, it is not guaranteed that the model that answers is the model that was scored 1.

Note: the exploration and warmup phases can be controlled by dynamically changing $\epsilon$: set $\epsilon=1$ to trigger warmup; set $\epsilon$ to the value specified in the scorer's configuration or to the default value, if no configuration for $\epsilon$ is specified.

**Exploitation:** using only the models that were actually explored, score models using sigmoid scoring function.

#### Per-model Rank

For each model, maintain a tdigest over the costs when this model was selected (i.e., the arm was pulled). Rank of a model $m$:

$$rank_{m} = TrimmedMean(0, \alpha) + \lambda * CTE(\alpha) $$

The first term is the body cost and the second term is the tail cost. $\lambda$ is the penalizing factor. By default it is 1. CTE is Conditional Tail Expectation for the tail defined by the $\alpha \in [0,1]$ quantile $q_{\alpha}$ (e.g., a 95th percentile is $q_{0.95}$, 99th percentile is $q_{0.99}$, etc.).

$$TrimmedMean(0, \alpha) = E[X | X \leq q_{\alpha}]$$

TrimmedMean is the mean of the bottom $\alpha$ fraction of the distribution (we want to separate between the body and the tail).

$CTE(\alpha) = E[X | X > q_{\alpha}]$

CTE is the **expected** value of a draw from the tail, not just the threshold.

### Score function

Minimum rank model gets the highest score. We use sigmoid with temperature for scoring. The score will always be in (0, 1).
$$score(m) = \frac{1}{1+\exp(\beta \cdot (rank_{m} - M)) }$$

- $\beta = \frac{1}{\sigma}$ is the temperature of the sigmoid
- $\sigma$ is STD
- M is median

The larger $\beta$ is, the sharper the sigmoid is. This widens the differences in (0, 1) score for close rank values (expressed in USD cost) apart in the score range. The smaller $\beta$ is, the more flattened sigmoid is. This will not push too far apart the scores in (0, 1) for the ranks (in USD), which are not close. Thus, sigmoid will autocalibrate itself for any model set based on STD $\sigma$. For the model with the median rank (USD cost value), the score will be neutral 0.5.

#### Example (large STD)

Models m1, m2, m3. Rank(m1) = 100, rank(m2)=110, rank(m3)=130. Median (M) = 110. STD = 12.48. $\beta = \frac{1}{\sigma}$ = 0.08. Apply sigmoid, get scores:

- score(m1) = 0.690
- score(m2) = 0.5
- score(m3) = 0.168

#### Example (small STD)

Rank(m1)=49.9, rank(m2) = 50.0, rank(m3)=50.1. STD = 0.08165. $\beta = \frac{1}{\sigma}$ = 12.25

- score(m1) = 0.772
- score(m2) = 0.5
- score(m3) = 0.228

Note: the score assigned by the temperatured sigmoid is not in [0, 1], but in (0,1). $\beta$ automatically controls how close the score will approach 0 or 1. The scores of the very large ranks asymptotically approach 0 and the scores of very low ranks asymptotically approach 1. In the warmup/exploration steps, the score of a model under warmup/exploration is set to 1 while the scores of all other models are set to 0. Thus, overall, the score function outputs model scores in [0, 1] as expected.

## Architecture

 1. Model information is populated using the `modelconfigcollector` plugin
 2. tdigest per model and warmup counters per `WindowDuration` are maintained per model in `AttributeMap` in the `datastore`
 3. The input and output token counters are extracted by a specialized `requestcostdata` extractor plugin and convert these two counters into a single `cost` counter
 4. The extracted `cost` sample is used by `requestmetadata` to update the tdigest of the model that responded
 5. The CostGuard scorer computes ranks and scores per model directly from tdigest on the fly.
 6. When an epoch ends, tdigest is frozen and a new tdigest is initialized.

### Roadmap

Implement CostGuard via a series of small PRs

 1. Extend `modelconfigcollector` to collect input and output token prices;
 2. Implement a specialized `requestcostmetadata` extractor to maintain tdigest of cost;
 3. Extend `AttributeMap` in the `datastore` to maintain tdigest and warmup counters;
 4. Implement CostGuard;
 5. Wire `CostGuard` with the rest of the system.
 6. Connect `CostGuard` to the proposed `OpenCost` [AI Inference API](https://github.com/opencost/opencost/pull/3829) to retrieve pricing attributes of the models. Allocation-based cost will be used as a proxy for variable per input and output token prices in a self-hosted cluster.

### Alternatives considered

1. Extend existing `requestmetadata` scope to deal with the cost information vs specialized extractor

- Pros: less plugins, simpler configuration in short term
- Cons: `requestmetadata` scope becomes poorly defined, harder to maintain in longer term

1. Dynamic model set vs static model set

- Dynamic model set should be supported in the future, but for the first step, a static model set is assumed

1. Dynamic autoadjusting exploration rate vs a static configurable

- Pros: cleaner implementation and simpler behavior from the user PoV
- Cons: none spotted
