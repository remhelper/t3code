/**
 * OpenCodeAdapter - OpenCode implementation of the generic provider adapter contract.
 *
 * This service owns OpenCode server API semantics and emits OpenCode provider events.
 *
 * @module OpenCodeAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors";
import type { ProviderAdapterShape } from "./ProviderAdapter";

/**
 * OpenCodeAdapterShape - Service API for the OpenCode provider adapter.
 */
export interface OpenCodeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "opencode";
}

/**
 * OpenCodeAdapter - Service tag for OpenCode provider adapter operations.
 */
export class OpenCodeAdapter extends ServiceMap.Service<OpenCodeAdapter, OpenCodeAdapterShape>()(
  "t3/provider/Services/OpenCodeAdapter",
) {}
