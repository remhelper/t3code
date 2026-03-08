/**
 * OpenCodeAdapterLive - Scoped live implementation for the OpenCode provider adapter.
 *
 * Uses the OpenCode server HTTP API to manage sessions and send turns.
 *
 * @module OpenCodeAdapterLive
 */
import { pathToFileURL } from "node:url";

import {
  ProviderSession,
  type ProviderRuntimeEvent,
  ProviderApprovalDecision,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
  EventId,
} from "@t3tools/contracts";
import { Effect, Layer, Queue, Stream } from "effect";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";

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
const SSE_RETRY_DELAY_MS = 2_000;

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

function resolveRequestType(type: string | undefined):
  | "command_execution_approval"
  | "file_read_approval"
  | "file_change_approval"
  | "unknown" {
  if (!type) return "unknown";
  const lower = type.toLowerCase();
  if (lower.includes("read")) return "file_read_approval";
  if (lower.includes("write") || lower.includes("edit") || lower.includes("patch")) {
    return "file_change_approval";
  }
  if (lower.includes("command") || lower.includes("exec")) {
    return "command_execution_approval";
  }
  return "unknown";
}

function resolveStreamKind(partType: string): "assistant_text" | "reasoning_text" {
  return partType === "reasoning" ? "reasoning_text" : "assistant_text";
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
    const sessionIdToThreadId = new Map<string, ThreadId>();
    const requestTypeById = new Map<string, "command_execution_approval" | "file_read_approval" | "file_change_approval" | "unknown">();
    const serverConfig = yield* Effect.service(ServerConfig);
    let streamReady = false;
    const streamAbortController = new AbortController();

    const emit = (event: ProviderRuntimeEvent) => Queue.offer(queue, event);
    const emitNow = (event: ProviderRuntimeEvent) => {
      void Effect.runPromise(Queue.offer(queue, event));
    };

    const handleEvent = (payload: unknown) => {
      if (!payload || typeof payload !== "object") return;
      const record = payload as Record<string, unknown>;
      const type = typeof record.type === "string" ? record.type : undefined;
      const properties = record.properties && typeof record.properties === "object"
        ? (record.properties as Record<string, unknown>)
        : undefined;
      const sessionId =
        (properties?.sessionID as string | undefined) ||
        (properties?.info && typeof properties.info === "object"
          ? ((properties.info as Record<string, unknown>).sessionID as string | undefined)
          : undefined) ||
        (properties?.part && typeof properties.part === "object"
          ? ((properties.part as Record<string, unknown>).sessionID as string | undefined)
          : undefined);
      if (!sessionId) return;
      const threadId = sessionIdToThreadId.get(sessionId);
      if (!threadId) return;

      if (type === "session.status") {
        const status = properties?.status && typeof properties.status === "object"
          ? (properties.status as Record<string, unknown>)
          : null;
        const statusType = status?.type;
        const state = statusType === "busy" ? "running" : statusType === "retry" ? "running" : "ready";
        emitNow({
          ...eventBase(threadId),
          type: "session.state.changed",
          payload: { state },
        });
        return;
      }

      if (type === "session.idle") {
        emitNow({
          ...eventBase(threadId),
          type: "session.state.changed",
          payload: { state: "ready" },
        });
        return;
      }

      if (type === "session.error") {
        const error = properties?.error && typeof properties.error === "object"
          ? (properties.error as Record<string, unknown>)
          : undefined;
        const message = (error?.message as string | undefined) || "OpenCode session error";
        emitNow({
          ...eventBase(threadId),
          type: "runtime.error",
          payload: { message, class: "provider_error" },
        });
        return;
      }

      if (type === "message.part.updated") {
        const part = properties?.part && typeof properties.part === "object"
          ? (properties.part as Record<string, unknown>)
          : undefined;
        if (!part) return;
        const messageId = part.messageID as string | undefined;
        if (!messageId) return;
        const turnId = TurnId.makeUnsafe(messageId);
        const itemId = RuntimeItemId.makeUnsafe(part.id as string ?? messageId);
        const partType = part.type as string | undefined;
        if (partType === "text" || partType === "reasoning") {
          const delta =
            (typeof properties?.delta === "string" ? properties.delta : undefined) ??
            (typeof part.text === "string" ? part.text : "");
          if (!delta) return;
          emitNow({
            ...eventBase(threadId, turnId),
            itemId,
            type: "content.delta",
            payload: {
              streamKind: resolveStreamKind(partType),
              delta,
            },
          });
          return;
        }

        if (partType === "tool") {
          const state = part.state && typeof part.state === "object"
            ? (part.state as Record<string, unknown>)
            : undefined;
          const status = state?.status as string | undefined;
          emitNow({
            ...eventBase(threadId, turnId),
            itemId,
            type: "item.updated",
            payload: {
              itemType: "dynamic_tool_call",
              status:
                status === "completed"
                  ? "completed"
                  : status === "error"
                    ? "failed"
                    : status === "running"
                      ? "inProgress"
                      : "inProgress",
              detail: typeof state?.output === "string" ? state.output : undefined,
            },
          });
        }
        return;
      }

      if (type === "message.updated") {
        const info = properties?.info && typeof properties.info === "object"
          ? (properties.info as Record<string, unknown>)
          : undefined;
        if (!info || info.role !== "assistant") return;
        const messageId = info.id as string | undefined;
        if (!messageId) return;
        const turnId = TurnId.makeUnsafe(messageId);
        const itemId = RuntimeItemId.makeUnsafe(messageId);
        emitNow({
          ...eventBase(threadId, turnId),
          itemId,
          type: "item.completed",
          payload: {
            itemType: "assistant_message",
            status: "completed",
          },
        });
        emitNow({
          ...eventBase(threadId, turnId),
          type: "turn.completed",
          payload: { state: "completed" },
        });
        emitNow({
          ...eventBase(threadId, turnId),
          type: "session.state.changed",
          payload: { state: "ready" },
        });
        return;
      }

      if (type === "permission.updated") {
        const permission = properties ?? {};
        const permissionId = permission.id as string | undefined;
        if (!permissionId) return;
        const requestType = resolveRequestType(permission.type as string | undefined);
        requestTypeById.set(permissionId, requestType);
        emitNow({
          ...eventBase(threadId),
          requestId: RuntimeRequestId.makeUnsafe(permissionId),
          type: "request.opened",
          payload: {
            requestType,
            detail: typeof permission.title === "string" ? permission.title : undefined,
            args: permission.metadata,
          },
        });
        return;
      }

      if (type === "permission.replied") {
        const permissionId = properties?.permissionID as string | undefined;
        if (!permissionId) return;
        const requestType = requestTypeById.get(permissionId) ?? "unknown";
        requestTypeById.delete(permissionId);
        emitNow({
          ...eventBase(threadId),
          requestId: RuntimeRequestId.makeUnsafe(permissionId),
          type: "request.resolved",
          payload: { requestType, decision: String(properties?.response ?? "") },
        });
      }
    };

    const readSseOnce = async () => {
      const decoder = new TextDecoder();
      const baseUrl = baseUrlFromEnv();
      const authHeader = buildAuthHeader();

      const response = await fetch(new URL("/global/event", baseUrl), {
        headers: authHeader ? { authorization: authHeader } : undefined,
        signal: streamAbortController.signal,
      });
      if (!response.ok || !response.body) {
        streamReady = false;
        throw new Error("OpenCode SSE connection failed.");
      }

      streamReady = true;
      const reader = response.body.getReader();
      let buffer = "";
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        buffer += decoder.decode(chunk.value, { stream: true });
        let boundaryIndex = buffer.indexOf("\n\n");
        while (boundaryIndex !== -1) {
          const rawEvent = buffer.slice(0, boundaryIndex);
          buffer = buffer.slice(boundaryIndex + 2);
          const dataLines = rawEvent
            .split("\n")
            .map((line) => line.trim())
            .filter((line) => line.startsWith("data:"))
            .map((line) => line.slice(5).trim());
          const data = dataLines.join("\n").trim();
          if (data) {
            try {
              handleEvent(JSON.parse(data));
            } catch {
              // ignore parse errors
            }
          }
          boundaryIndex = buffer.indexOf("\n\n");
        }
      }
    };

    yield* Effect.forkScoped(
      Effect.gen(function* () {
        while (true) {
          yield* Effect.tryPromise({
            try: () => readSseOnce(),
            catch: () => undefined,
          });
          streamReady = false;
          yield* Effect.sleep(SSE_RETRY_DELAY_MS);
        }
      }),
    );

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
          try: () =>
            requestJson<{ id?: string }>("POST", "/session", {
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
        sessionIdToThreadId.set(sessionId, input.threadId);

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

        const messageId = crypto.randomUUID();
        const turnId = TurnId.makeUnsafe(messageId);
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

        const parts: Array<Record<string, unknown>> = [{ type: "text", text: input.input ?? "" }];
        if (Array.isArray(input.attachments)) {
          for (const attachment of input.attachments) {
            if (attachment.type !== "image") continue;
            const resolvedPath = resolveAttachmentPath({
              stateDir: serverConfig.stateDir,
              attachment,
            });
            if (!resolvedPath) continue;
            const fileUrl = pathToFileURL(resolvedPath).toString();
            parts.push({
              type: "file",
              mime: attachment.mimeType,
              filename: attachment.name,
              url: fileUrl,
            });
          }
        }

        const body = {
          messageID: messageId,
          ...(input.model ? { model: input.model } : {}),
          parts,
        };

        if (streamReady) {
          yield* Effect.tryPromise({
            try: () =>
              requestJson(
                "POST",
                `/session/${session.opencodeSessionId}/prompt_async`,
                body,
              ),
            catch: (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "POST /session/:id/prompt_async",
                detail: toMessage(cause, "Failed to send OpenCode message (async)."),
                cause,
              }),
          });
        } else {
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
        }

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
        sessionIdToThreadId.delete(session.opencodeSessionId);
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
        streamAbortController.abort();
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
