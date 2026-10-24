/**
 * opencode-plugins — plugin OpenCode modular (API V2 do opencode2).
 *
 * Combina thinking-guard (anti-loop de raciocínio) e truncation
 * (auto-continue de respostas truncadas) num único plugin com uma única
 * assinatura de eventos.
 *
 * Uso no `.opencode/plugins/`:
 * ```ts
 * export { default } from "../../opencode-plugins/src/index.ts"
 * ```
 */

import { config } from "./config"
import {
  createGuardCore,
  createThinkingGuardPlugin,
  type GuardEvent,
} from "./features/thinking-guard"
import {
  createTruncationCore,
  createTruncationPlugin,
} from "./features/truncation"

/**
 * Plugin completo: thinking-guard + truncation com config centralizada.
 */
export default ({
  id: "opencode-plugins",
  setup: async (ctx) => {
    const controller = new AbortController()

    const guard = createGuardCore(
      {
        ...config.thinking,
        cooldownMs: config.global.cooldownMs,
        maxInterventions: config.global.maxInterventionsPerTurn,
        observability: config.observability,
      },
      {
        interrupt: (sessionID) => ctx.session.interrupt({ sessionID }),
        injectStop: (sessionID, text) =>
          ctx.session.synthetic({ sessionID, text, delivery: "steer", resume: true }),
      },
    )
    const truncation = createTruncationCore(config.truncation, {
      injectContinue: (sessionID, text) =>
        ctx.session.synthetic({ sessionID, text, delivery: "steer", resume: true }),
    })

    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          const ev = event as unknown as GuardEvent
          guard.onEvent(ev)
          await truncation.onEvent(ev)
        }
      } catch (err: any) {
        if (err?.name !== "AbortError") {
          console.error("[opencode-plugins] event subscription error:", err)
        }
      }
    })()

    return () => controller.abort()
  },
})

// Re-exports para uso individual.
export { createThinkingGuardPlugin, createTruncationPlugin }
export { createGuardCore } from "./features/thinking-guard"
export { createTruncationCore } from "./features/truncation"
export { config } from "./config"
export type {
  PluginsConfig,
  ThinkingConfig,
  TruncationConfig,
  GlobalConfig,
  ObservabilityConfig,
} from "./config"
export {
  createEventEmitter,
  DEFAULT_EVENTS_PATH,
  DEFAULT_LOKI_URL_ENV,
} from "./observability/emitter"
export type {
  ThinkingGuardEvent,
  ThinkingGuardEventType,
  EventSink,
  ObservabilityOptions,
} from "./observability/emitter"
