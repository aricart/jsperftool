/*
 * Copyright 2024 Synadia Communications, Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ConnectionOptions, NatsConnection } from "@nats-io/nats-core/internal";

export type ConnectFn = (opts?: ConnectionOptions) => Promise<NatsConnection>;
export async function getConnectFn(): Promise<ConnectFn> {
  if (globalThis.Deno) {
    console.log("using transport-deno");
    const { connect } = await import("./denoconn.ts");
    return connect;
  } else {
    console.log("using transport-node");
    const { connect } = await import("./nodeconn.ts");
    return connect;
  }
}
