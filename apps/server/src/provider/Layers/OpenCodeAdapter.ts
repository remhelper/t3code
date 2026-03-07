/**
 * OpenCodeAdapterLive - Scoped live implementation for the OpenCode provider adapter.
 *
 * Uses the OpenCode server HTTP API to manage sessions and send turns.
 *
 * @module OpenCodeAdapterLive
 */
import {
  ProviderSession,
  type ProviderRuntimeEvent,
  ProviderApprovalDecision,
  RuntimeItemId,
  ThreadId,
  TurnId,
  EventId,
} from "@t3tools/contracts";
import { Effect, Layer, Queue, Stream } from "effect";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { OpenCodeAdapter, type OpenCodeAdapterShape } from "../Services/OpenCodeAdapter.ts";

const PROVIDER = "opencode" as const;
const DEFAULT_BASE_URL = "http://127.0.0.1:4096";
const DELTA_CHUNK_SIZE = 600;

function baseUrlFromEnv(): string {
  return process.env.OPENCODE_SERVER_URL?.trim() || DEFAULT_BASE_URL;
}

function buildAuthHeader(): string | null {
  const password = process.env.OPENCODE_SERVER_PASSWORD;
  if (!password) return null;
  const username = process.env.OPENCODE_SERVER_USERNAME || "opencode";
  const token = Buffer.from(`${username}:${password}`).toString("base64");
  return `Basic ${token}`;
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) {
    return cause.message;
  }
  return fallback;
}

function eventBase(threadId: ThreadId, turnId?: TurnId) {
  return {
    eventId: EventId.makeUnsafe(crypto.randomUUID()),
    provider: PROVIDER,
    threadId,
    createdAt: new Date().toISOString(),
    ...(turnId ? { turnId } : {}),
  } satisfies Pick<ProviderRuntimeEvent, "eventId" | "provider" | "threadId" | "createdAt" | "turnId">;
}

function chunkText(text: string, chunkSize: number): string[] {
  if (!text) return [];
  const chunks: string[] = [];
  for (let index = 0; index < text.length; index += chunkSize) {
    chunks.push(text.slice(index, index + chunkSize));
  }
  return chunks;
}

function extractTextFromParts(parts: unknown): string {
  if (!Array.isArray(parts)) return "";
  const textParts: string[] = [];
  for (const part of parts) {
    if (!part || typeof part !== "object") continue;
    const record = part as Record<string, unknown>;
    if (typeof record.text === "string") {
      textParts.push(record.text);
      continue;
    }
    if (record.type === "text" && typeof record.content === "string") {
      textParts.push(record.content);
      continue;
    }
    if (record.content && typeof record.content === "object") {
      const contentRecord = record.content as Record<string, unknown>;
      if (typeof contentRecord.text === "string") {
        textParts.push(contentRecord.text);
      }
    }
  }
  return textParts.join("");
}

function extractAssistantText(payload: unknown): string {
  if (!payload || typeof payload !== "object") return "";
  const record = payload as Record<string, unknown>;
  if (Array.isArray(record.parts)) {
    return extractTextFromParts(record.parts);
  }
  if (record.data && typeof record.data === "object") {
    const nested = record.data as Record<string, unknown>;
    if (Array.isArray(nested.parts)) {
      return extractTextFromParts(nested.parts);
    }
  }
  if (record.info && typeof record.info === "object") {
    const info = record.info as Record<string, unknown>;
    if (typeof info.text === "string") {
      return info.text;
    }
    if (Array.isArray(info.parts)) {
      return extractTextFromParts(info.parts);
    }
  }
  return "";
}

async function requestJson<T>(method: string, path: string, body?: unknown): Promise<T> {
  const baseUrl = baseUrlFromEnv();
  const url = new URL(path, baseUrl);
  const headers: Record<string, string> = {
    "content-type": "application/json",
  };
  const authHeader = buildAuthHeader();
  if (authHeader) {
    headers.authorization = authHeader;
  }
  const response = await fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`OpenCode request failed (${response.status}): ${detail}`);
  }
  return (await response.json()) as T;
}

const makeOpenCodeAdapter = () =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, ProviderSession & { opencodeSessionId: string }>();

    const emit = (event: ProviderRuntimeEvent) => Queue.offer(queue, event);

    const startSession: OpenCodeAdapterShape["startSession"] = (input) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* Effect.fail(
            new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            }),
          );
        }

        const response = yield* Effect.tryPromise({
          try: () => requestJson<{ id?: string }>("POST", "/session", {
            title: `Thread ${input.threadId}`,
          }),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "POST /session",
              detail: toMessage(cause, "Failed to start OpenCode session."),
              cause,
            }),
        });

        const sessionId = response.id;
        if (!sessionId) {
          return yield* Effect.fail(
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "POST /session",
              detail: "OpenCode did not return a session id.",
            }),
          );
        }

        const now = new Date().toISOString();
        const session: ProviderSession & { opencodeSessionId: string } = {
          provider: PROVIDER,
          status: "ready",
          runtimeMode: input.runtimeMode,
          ...(input.cwd !== undefined ? { cwd: input.cwd } : {}),
          ...(input.model !== undefined ? { model: input.model } : {}),
          threadId: input.threadId,
          resumeCursor: undefined,
          createdAt: now,
          updatedAt: now,
          opencodeSessionId: sessionId,
        };
        sessions.set(input.threadId, session);

        yield* emit({
          ...eventBase(input.threadId),
          type: "session.started",
          payload: {},
        });
        yield* emit({
          ...eventBase(input.threadId),
          type: "session.state.changed",
          payload: { state: "ready" },
        });
        yield* emit({
          ...eventBase(input.threadId),
          type: "thread.started",
          payload: { providerThreadId: sessionId },
        });

        return session satisfies ProviderSession;
      });

    const sendTurn: OpenCodeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const session = sessions.get(input.threadId);
        if (!session) {
          return yield* Effect.fail(
            new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId: input.threadId,
            }),
          );
        }

        const turnId = TurnId.makeUnsafe(crypto.randomUUID());
        const itemId = RuntimeItemId.makeUnsafe(crypto.randomUUID());

        yield* emit({
          ...eventBase(input.threadId, turnId),
          type: "session.state.changed",
          payload: { state: "running" },
        });
        yield* emit({
          ...eventBase(input.threadId, turnId),
          type: "turn.started",
          payload: {
            ...(input.model ? { model: input.model } : {}),
          },
        });
        yield* emit({
          ...eventBase(input.threadId, turnId),
          itemId,
          type: "item.started",
          payload: {
            itemType: "assistant_message",
            status: "inProgress",
          },
        });

        const body = {
          messageID: turnId,
          ...(input.model ? { model: input.model } : {}),
          parts: [{ type: "text", text: input.input ?? "" }],
        };

        const response = yield* Effect.tryPromise({
          try: () =>
            requestJson<Record<string, unknown>>(
              "POST",
              `/session/${session.opencodeSessionId}/message`,
              body,
            ),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "POST /session/:id/message",
              detail: toMessage(cause, "Failed to send OpenCode message."),
              cause,
            }),
        });

        const assistantText = extractAssistantText(response);
        for (const chunk of chunkText(assistantText, DELTA_CHUNK_SIZE)) {
          yield* emit({
            ...eventBase(input.threadId, turnId),
            itemId,
            type: "content.delta",
            payload: {
              streamKind: "assistant_text",
              delta: chunk,
            },
          });
        }

        yield* emit({
          ...eventBase(input.threadId, turnId),
          itemId,
          type: "item.completed",
          payload: {
            itemType: "assistant_message",
            status: "completed",
            ...(assistantText ? { detail: assistantText } : {}),
          },
        });
        yield* emit({
          ...eventBase(input.threadId, turnId),
          type: "turn.completed",
          payload: {
            state: "completed",
          },
        });
        yield* emit({
          ...eventBase(input.threadId, turnId),
          type: "session.state.changed",
          payload: { state: "ready" },
        });

        return {
          threadId: input.threadId,
          turnId,
        };
      });

    const interruptTurn: OpenCodeAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const session = sessions.get(threadId);
        if (!session) {
          return yield* Effect.fail(
            new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            }),
          );
        }

        yield* Effect.tryPromise({
          try: () => requestJson("POST", `/session/${session.opencodeSessionId}/abort`),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "POST /session/:id/abort",
              detail: toMessage(cause, "Failed to abort OpenCode session."),
              cause,
            }),
        });
      });

    const respondToRequest: OpenCodeAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const session = sessions.get(threadId);
        if (!session) {
          return yield* Effect.fail(
            new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            }),
          );
        }

        const response = (() => {
          switch (decision) {
            case "accept":
            case "acceptForSession":
              return "accept";
            case "decline":
              return "decline";
            case "cancel":
              return "cancel";
            default:
              return "decline";
          }
        })();

        yield* Effect.tryPromise({
          try: () =>
            requestJson(
              "POST",
              `/session/${session.opencodeSessionId}/permissions/${requestId}`,
              { response },
            ),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "POST /session/:id/permissions/:permissionId",
              detail: toMessage(cause, "Failed to respond to OpenCode permission request."),
              cause,
            }),
        });
      });

    const respondToUserInput: OpenCodeAdapterShape["respondToUserInput"] = (threadId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToUserInput",
          detail: `OpenCode user input responses are not yet supported for thread ${threadId}.`,
        }),
      );

    const stopSession: OpenCodeAdapterShape["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const session = sessions.get(threadId);
        if (!session) {
          return yield* Effect.fail(
            new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            }),
          );
        }

        yield* Effect.tryPromise({
          try: () => requestJson("DELETE", `/session/${session.opencodeSessionId}`),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "DELETE /session/:id",
              detail: toMessage(cause, "Failed to stop OpenCode session."),
              cause,
            }),
        });

        sessions.delete(threadId);
        yield* emit({
          ...eventBase(threadId),
          type: "session.exited",
          payload: { exitKind: "graceful" },
        });
      });

    const listSessions: OpenCodeAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values()));

    const hasSession: OpenCodeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId));

    const readThread: OpenCodeAdapterShape["readThread"] = (threadId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "readThread",
          detail: `OpenCode thread snapshots are not yet supported for thread ${threadId}.`,
        }),
      );

    const rollbackThread: OpenCodeAdapterShape["rollbackThread"] = (threadId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "rollbackThread",
          detail: `OpenCode rollback is not yet supported for thread ${threadId}.`,
        }),
      );

    const stopAll: OpenCodeAdapterShape["stopAll"] = () =>
      Effect.gen(function* () {
        for (const threadId of sessions.keys()) {
          yield* stopSession(threadId).pipe(Effect.ignore);
        }
        yield* Queue.shutdown(queue);
      });

    const streamEvents = Stream.fromQueue(queue);

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "restart-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents,
    } satisfies OpenCodeAdapterShape;
  });

export const makeOpenCodeAdapterLive = () => Layer.effect(OpenCodeAdapter, makeOpenCodeAdapter());
