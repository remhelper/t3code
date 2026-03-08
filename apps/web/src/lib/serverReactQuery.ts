import { queryOptions } from "@tanstack/react-query";
import { ensureNativeApi } from "~/nativeApi";

export const serverQueryKeys = {
  all: ["server"] as const,
  config: () => ["server", "config"] as const,
  providerModels: () => ["server", "provider-models"] as const,
};

export function serverConfigQueryOptions() {
  return queryOptions({
    queryKey: serverQueryKeys.config(),
    queryFn: async () => {
      const api = ensureNativeApi();
      return api.server.getConfig();
    },
    staleTime: Infinity,
  });
}

export function serverProviderModelsQueryOptions() {
  return queryOptions({
    queryKey: serverQueryKeys.providerModels(),
    queryFn: async () => {
      const api = ensureNativeApi();
      return api.server.getProviderModels();
    },
    staleTime: 5 * 60 * 1000,
  });
}
