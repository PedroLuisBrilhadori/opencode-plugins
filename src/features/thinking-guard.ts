/**
 * Feature: Thinking Guard (API V2 do opencode2)
 *
 * Interrompe loops repetitivos de raciocínio do modelo — e somente isso.
 *
 * O raciocínio (thinking) chega via eventos `session.reasoning.delta`
 * (deltas diretos, já extraídos pelo opencode2). O texto é acumulado por
 * sessão e dividido em chunks que alimentam um RepetitionBuffer (Jaccard
 * similarity em bigramas). Somente a confirmação de repetição consecutiva
 * dispara a intervenção:
 *
 * - ctx.session.interrupt() para a execução atual
 * - ctx.session.synthetic(resume) injeta "pare de pensar e responda"
 *
 * Token count (session.usage.updated) é apenas observabilidade —
 * raciocínio longo e produtivo NUNCA é interrompido.
 *
 * Todos os eventos relevantes são emitidos via src/observability/emitter.ts
 * para JSONL local e, opcionalmente, Loki.
 */

import type { Context } from "@opencode-ai/plugin/promise/plugin"
import type { ThinkingConfig } from "../config"
import {
  createEventEmitter,
  type EventSink,
  type ObservabilityOptions,
} from "../observability/emitter"
import { RepetitionBuffer } from "../utils/repetition"
import { SessionTokenCounter } from "../utils/tokens"

export interface ThinkingGuardOptions extends Partial<ThinkingConfig> {
  /** Texto para injetar ao detectar loop. */
  stopText?: string
  /** Cooldown entre intervenções em ms (default: 5_000). */
  cooldownMs?: number
  /** Máx. de intervenções por turno do usuário (default: 2). */
  maxInterventions?: number
  /** Observabilidade: JSONL local + Loki opcional (default: ligado). */
  observability?: ObservabilityOptions
}

const DEFAULT_STOP_TEXT =
  "[thinking-guard] Você está em loop de raciocínio. Pare de pensar e responda diretamente ao usuário com o que já tem."

interface TurnState {
  count: number
  lastAt: number
  suppressed: number
}

/** Evento genérico do bus do opencode2 — lido defensivamente. */
export interface GuardEvent {
  type: string
  data?: any
}

/** Ações de sessão — injetadas para facilitar teste. */
export interface SessionActions {
  interrupt(sessionID: string): Promise<unknown>
  injectStop(sessionID: string, text: string): Promise<unknown>
}

/**
 * Acumula texto de raciocínio e emite chunks de tamanho fixo para o
 * RepetitionBuffer. Chunks de tamanho fixo evitam que o streaming (deltas
 * pequenos) fragmente a análise de similaridade.
 */
class ReasoningLoopDetector {
  private pending = ""
  readonly buffer: RepetitionBuffer

  constructor(
    windowSize: number,
    private readonly chunkSize: number,
  ) {
    this.buffer = new RepetitionBuffer(windowSize)
  }

  push(text: string): void {
    this.pending += text
    while (this.pending.length >= this.chunkSize) {
      this.buffer.push(this.pending.slice(0, this.chunkSize))
      this.pending = this.pending.slice(this.chunkSize)
    }
  }

  isRepeating(threshold: number, consecutive: number): boolean {
    return this.buffer.isRepeating(threshold, consecutive)
  }

  /** Número de chunks completos no buffer. */
  get chunkCount(): number {
    return this.buffer.size
  }

  /** Similaridades da última verificação (mais antigo → mais recente). */
  get similarityStreak(): number[] {
    return this.buffer.similarityStreak
  }

  /** Maior similaridade adjacente vista desde o último clear(). */
  get maxSimilarity(): number {
    return this.buffer.maxSimilarity
  }

  clear(): void {
    this.pending = ""
    this.buffer.clear()
  }
}

/**
 * Núcleo do guard: máquina de estados por sessão dirigida pelos eventos
 * do opencode2. Sem dependência de Context — as ações são injetadas.
 */
export const createGuardCore = (
  options: ThinkingGuardOptions | undefined,
  actions: SessionActions,
  emitterInput?: EventSink,
) => {
  const softLimit = options?.softLimit ?? 10_000
  const windowSize = options?.repetitionWindow ?? 4
  const similarityThreshold = options?.repetitionSimilarity ?? 0.6
  const consecutiveThreshold = options?.repetitionConsecutive ?? 3
  const chunkSize = options?.reasoningChunkSize ?? 600
  const stopText = options?.stopText ?? DEFAULT_STOP_TEXT
  const cooldownMs = options?.cooldownMs ?? 5_000
  const maxInterventions = options?.maxInterventions ?? 2

  const emitter: EventSink = emitterInput ?? createEventEmitter(options?.observability)
  const tokenCounter = new SessionTokenCounter()
  const detectors = new Map<string, ReasoningLoopDetector>()
  const turnStates = new Map<string, TurnState>()
  const sessionModel = new Map<string, { providerID?: string; modelID?: string }>()
  const lastMessageID = new Map<string, string>()

  const getDetector = (sessionID: string) => {
    let detector = detectors.get(sessionID)
    if (!detector) {
      detector = new ReasoningLoopDetector(windowSize, chunkSize)
      detectors.set(sessionID, detector)
    }
    return detector
  }

  const emitTurnSummary = (sessionID: string) => {
    const detector = detectors.get(sessionID)
    const tokens = tokenCounter.get(sessionID)
    const turn = turnStates.get(sessionID)
    if (!detector && tokens.output === 0) return
    emitter.emit({
      event: "turn_summary",
      sessionID,
      ...sessionModel.get(sessionID),
      chunksAnalyzed: detector?.chunkCount ?? 0,
      maxSimilarity: round2(detector?.maxSimilarity ?? 0),
      tokens: { output: tokens.output, reasoning: tokens.reasoning },
      interventions: turn?.count ?? 0,
      suppressed: turn?.suppressed ?? 0,
    })
  }

  /**
   * Intervém no loop: interrupt + synthetic(resume). Respeita máx. de
   * intervenções por turno e cooldown; executadas e suprimidas são
   * emitidas como eventos.
   */
  const intervene = async (sessionID: string, detector: ReasoningLoopDetector) => {
    const state = turnStates.get(sessionID) ?? { count: 0, lastAt: 0, suppressed: 0 }
    const streak = detector.similarityStreak
    const base = {
      sessionID,
      messageID: lastMessageID.get(sessionID),
      ...sessionModel.get(sessionID),
      similarity: round2(streak[streak.length - 1] ?? 0),
      consecutiveMatches: streak.filter((s) => s >= similarityThreshold).length,
      tokens: {
        output: tokenCounter.get(sessionID).output,
        reasoning: tokenCounter.get(sessionID).reasoning,
      },
    }

    const suppressed =
      state.count >= maxInterventions
        ? ("max_interventions" as const)
        : state.count > 0 && Date.now() - state.lastAt < cooldownMs
          ? ("cooldown" as const)
          : undefined

    if (suppressed) {
      state.suppressed++
      turnStates.set(sessionID, state)
      console.log(
        `[thinking-guard] loop detected but suppressed (${suppressed}) (${sessionID})`,
      )
      emitter.emit({ ...base, event: "loop_suppressed", action: suppressed })
      return
    }

    turnStates.set(sessionID, { ...state, count: state.count + 1, lastAt: Date.now() })
    try {
      await actions.interrupt(sessionID)
      await actions.injectStop(sessionID, stopText)
      emitter.emit({ ...base, event: "intervention", action: "abort+prompt" })
    } catch (err) {
      console.error("[thinking-guard] intervention failed:", err)
    }
  }

  /** Processa um evento do bus do opencode2. */
  const onEvent = (event: GuardEvent) => {
    const data = event.data ?? {}

    switch (event.type) {
      // Novo turno (prompt do usuário) → resumo do turno anterior e reset.
      case "session.execution.started": {
        const sessionID = data.sessionID as string | undefined
        if (!sessionID) return
        emitTurnSummary(sessionID)
        tokenCounter.reset(sessionID)
        detectors.get(sessionID)?.clear()
        turnStates.delete(sessionID)
        return
      }

      // Thinking do modelo — é aqui que o loop vive.
      case "session.reasoning.delta": {
        const sessionID = data.sessionID as string | undefined
        const delta = data.delta
        if (!sessionID || typeof delta !== "string" || delta.length === 0) return
        if (data.assistantMessageID) lastMessageID.set(sessionID, data.assistantMessageID)

        const detector = getDetector(sessionID)
        detector.push(delta)
        if (detector.isRepeating(similarityThreshold, consecutiveThreshold)) {
          console.log(`[thinking-guard] repetition loop detected → abort (${sessionID})`)
          void intervene(sessionID, detector)
        }
        return
      }

      // Modelo selecionado para a sessão.
      case "session.model.selected": {
        const sessionID = data.sessionID as string | undefined
        const model = data.model
        if (sessionID && model?.id) {
          sessionModel.set(sessionID, {
            providerID: model.providerID,
            modelID: model.id,
          })
        }
        return
      }

      // Uso de tokens — apenas observabilidade (valores absolutos).
      case "session.usage.updated": {
        const sessionID = data.sessionID as string | undefined
        const tokens = data.tokens
        if (!sessionID || !tokens) return
        const count = tokenCounter.set(
          sessionID,
          tokens.output ?? 0,
          tokens.reasoning ?? 0,
        )
        if (count.output >= softLimit) {
          console.log(
            `[thinking-guard] soft limit reached: output=${count.output} reasoning=${count.reasoning} (${sessionID}) — log only, no action`,
          )
        }
        return
      }

      // Fim da sessão → resumo final e limpeza.
      case "session.idle": {
        const sessionID = data.sessionID as string | undefined
        if (!sessionID) return
        emitTurnSummary(sessionID)
        detectors.delete(sessionID)
        turnStates.delete(sessionID)
        return
      }
    }
  }

  return { onEvent }
}

/**
 * Cria o plugin thinking-guard para a API V2 do opencode2.
 *
 * Uso no `.opencode/plugins/`:
 * ```ts
 * export { createThinkingGuardPlugin as default } from "../../opencode-plugins/src/index.ts"
 * ```
 */
export const createThinkingGuardPlugin = (options?: ThinkingGuardOptions) => ({
  id: "thinking-guard",
  setup: async (ctx: Context) => {
      const controller = new AbortController()
      const actions: SessionActions = {
        interrupt: (sessionID) => ctx.session.interrupt({ sessionID }),
        injectStop: (sessionID, text) =>
          ctx.session.synthetic({
            sessionID,
            text,
            delivery: "steer",
            resume: true,
          }),
      }
      const core = createGuardCore(options, actions)

      void (async () => {
        try {
          for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
            core.onEvent(event as unknown as GuardEvent)
          }
        } catch (err: any) {
          if (err?.name !== "AbortError") {
            console.error("[thinking-guard] event subscription error:", err)
          }
        }
      })()

      return () => controller.abort()
    },
  })

function round2(n: number): number {
  return Math.round(n * 100) / 100
}
