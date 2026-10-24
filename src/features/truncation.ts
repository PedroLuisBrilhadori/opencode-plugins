/**
 * Feature: Truncation Detection (API V2 do opencode2)
 *
 * Detecta respostas truncadas pelo limite de output do modelo e injeta
 * um "continue" automático via ctx.session.synthetic(resume).
 *
 * Gatilho (verificado no session.idle):
 * - output tokens >= softLimit (perto do teto do modelo)
 *
 * Guardrails:
 * - Máx. maxAutoContinues por turno do usuário.
 * - Contador reseta quando o usuário manda mensagem nova
 *   (session.execution.started), não quando o plugin injeta.
 */

import type { Context } from "@opencode-ai/plugin/promise/plugin"
import type { TruncationConfig } from "../config"
import type { GuardEvent } from "./thinking-guard"

export interface TruncationOptions extends Partial<TruncationConfig> {
  /** Texto do prompt de continue. */
  continueText?: string
}

const DEFAULT_CONTINUE_TEXT =
  "[auto-continue] Sua resposta anterior foi interrompida pelo limite de output. Continue EXATAMENTE de onde parou — não repita nada do que já foi dito ou feito."

/**
 * Núcleo da truncation — máquina de estados dirigida pelos eventos do
 * opencode2, com ações injetadas para facilitar teste.
 */
export const createTruncationCore = (
  options: TruncationOptions | undefined,
  actions: { injectContinue(sessionID: string, text: string): Promise<unknown> },
) => {
  const softLimit = options?.softLimit ?? 115_000
  const maxContinues = options?.maxAutoContinues ?? 3
  const continueText = options?.continueText ?? DEFAULT_CONTINUE_TEXT

  const lastOutput = new Map<string, number>()
  const counts = new Map<string, number>()

  const onEvent = async (event: GuardEvent) => {
    const data = event.data ?? {}

    if (event.type === "session.execution.started") {
      const sessionID = data.sessionID as string | undefined
      if (sessionID) counts.delete(sessionID)
      return
    }

    if (event.type === "session.usage.updated") {
      const sessionID = data.sessionID as string | undefined
      const tokens = data.tokens
      if (sessionID && tokens) {
        lastOutput.set(sessionID, Math.max(lastOutput.get(sessionID) ?? 0, tokens.output ?? 0))
      }
      return
    }

    if (event.type !== "session.idle") return
    const sessionID = data.sessionID as string | undefined
    if (!sessionID) return

    const output = lastOutput.get(sessionID) ?? 0
    lastOutput.delete(sessionID)
    if (output < softLimit) return
    if ((counts.get(sessionID) ?? 0) >= maxContinues) return

    counts.set(sessionID, (counts.get(sessionID) ?? 0) + 1)
    console.log(
      `[truncation] output=${output} → continue #${counts.get(sessionID)} (${sessionID})`,
    )
    try {
      await actions.injectContinue(sessionID, continueText)
    } catch (err) {
      console.error("[truncation] continue failed:", err)
    }
  }

  return { onEvent }
}

/**
 * Cria o plugin de truncation para a API V2 do opencode2.
 */
export const createTruncationPlugin = (options?: TruncationOptions) => ({
  id: "truncation",
  setup: async (ctx: Context) => {
      const controller = new AbortController()
      const core = createTruncationCore(options, {
        injectContinue: (sessionID, text) =>
          ctx.session.synthetic({
            sessionID,
            text,
            delivery: "steer",
            resume: true,
          }),
      })

      void (async () => {
        try {
          for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
            await core.onEvent(event as unknown as GuardEvent)
          }
        } catch (err: any) {
          if (err?.name !== "AbortError") {
            console.error("[truncation] event subscription error:", err)
          }
        }
      })()

      return () => controller.abort()
    },
  })
