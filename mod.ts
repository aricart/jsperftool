import { cli, Command, Flag, Flags } from "@aricart/cobra";
import { getRuntime } from "@aricart/runtime";
import { progressFactory } from "@aricart/nd-progress";
import {
  ConnectionOptions,
  createInbox,
  deferred,
  delay,
  Events,
  nanos,
  NatsConnection,
  NatsConnectionImpl,
  nuid,
  wsconnect,
} from "@nats-io/nats-core/internal";
import {
  AckPolicy,
  AdvisoryKind,
  ConsumeMessages,
  Consumer,
  ConsumerConfig,
  ConsumerDebugEvents,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  JsMsg,
  OrderedConsumerOptions,
  PushConsumer,
} from "@nats-io/jetstream";
import { getConnectFn } from "./connectfn.ts";

const runtime = await getRuntime();

const stats = {
  reconnects: 0,
  streamLeader: 0,
  consumerLeader: 0,
  orderedResets: 0,
};

const root = cli({
  use: "perf [init|delete|run] --stream name",
});

root.addFlag({
  name: "ws",
  type: "boolean",
  usage: "use websocket",
  persistent: true,
  default: false,
});

root.addFlag({
  name: "stream",
  type: "string",
  usage: "name of the stream",
  persistent: true,
  required: true,
});

root.addFlag({
  name: "debug",
  type: "boolean",
  default: false,
  usage: "debug client connection",
  persistent: true,
});

root.addFlag({
  name: "events",
  type: "boolean",
  default: false,
  usage: "show server/client events",
  persistent: true,
});

const hb: Partial<Flag> = {
  name: "hb",
  type: "number",
  default: 0,
  usage: "heartbeat interval",
};

const callback: Partial<Flag> = {
  name: "callback",
  type: "boolean",
  usage: "use callback",
};

const ordered: Partial<Flag> = {
  name: "ordered",
  type: "boolean",
  usage: "use ordered consumer",
};

const push: Partial<Flag> = {
  name: "push",
  type: "boolean",
  usage: "use a push consumer",
};

const batch: Partial<Flag> = {
  short: "b",
  name: "batch",
  default: 500,
  usage: "batch size (for acks and pulls)",
};

const refill: Partial<Flag> = {
  short: "r",
  name: "msg-threshold",
  usage: "threshold messages to refill (default is 1/2 batch)",
};

const expires: Partial<Flag> = {
  short: "e",
  name: "expires",
  default: 30_000,
  usage: "time to wait in millis for a pull request",
};

const ack: Partial<Flag> = {
  name: "ack",
  type: "string",
  usage: "ack policy (all, explicit, none)",
  default: "none",
};

const count: Partial<Flag> = {
  short: "c",
  name: "count",
  type: "number",
  default: 1_000_000,
  usage: "number of messages",
};

const replication: Partial<Flag> = {
  name: "replication",
  type: "number",
  default: 1,
  usage: "replication count",
};

const work: Partial<Flag> = {
  name: "work",
  type: "number",
  default: 0,
  usage: "number of millis to spend processing a message",
};

const durable: Partial<Flag> = {
  name: "durable",
  type: "boolean",
  default: 0,
  usage: "make a durable consumer",
};

function checkFlags(flags: Flags): Promise<void> {
  const work = flags.value<number>("work");
  const callback = flags.value<boolean>("callback");
  const durable = flags.value<boolean>("durable");
  const ordered = flags.value<boolean>("ordered");
  const replication = flags.value<number>("replication");
  const ack = ackPolicy(flags);

  if (work !== 0 && callback) {
    return Promise.reject(
      new Error(`--work and --callback flags are mutually exclusive`),
    );
  }

  if (durable === true && ordered) {
    return Promise.reject(
      new Error("--durable and --ordered are mutually exclusive"),
    );
  }

  if (replication > 1 && ordered) {
    return Promise.reject(
      new Error("--replication and --ordered are mutually exclusive"),
    );
  }
  if (ordered && ack !== AckPolicy.None) {
    return Promise.reject(
      new Error("--ack and --ordered are mutually exclusive"),
    );
  }
  return Promise.resolve();
}

function ackPolicy(flags: Flags): AckPolicy {
  const ack = flags.value<string>("ack");
  switch (ack) {
    case AckPolicy.Explicit:
      return AckPolicy.Explicit;
    case AckPolicy.All:
      return AckPolicy.All;
    default:
      return AckPolicy.None;
  }
}

// init stream command
const init = root.addCommand({
  use: "init --stream name --count number --size bytes",
  short:
    "inits a stream by adding the specified number of messages with the specified payload size",
  run: initFn,
});
init.addFlag({
  short: "s",
  name: "size",
  type: "number",
  default: 1024,
  usage: "payload size (1024 bytes)",
});
init.addFlag(batch);
init.addFlag(count);
init.addFlag(replication);

// delete stream command
root.addCommand({
  use: "delete --stream name",
  short: "deletes a stream",
  run: deleteFn,
});

// stream info command
root.addCommand({
  use: "info --stream name",
  short: "stream info",
  run: infoFn,
});

const simplified = root.addCommand({
  use: "run --stream name",
  short: "measure run consumer",
  run: simplifiedFn,
});
simplified.addFlag(ordered);
simplified.addFlag(callback);
simplified.addFlag(ack);
simplified.addFlag(batch);
simplified.addFlag(hb);
simplified.addFlag(replication);
simplified.addFlag(work);
simplified.addFlag(durable);
simplified.addFlag(refill);
simplified.addFlag(expires);
simplified.addFlag(push);

runtime.exit(await root.execute());

async function createConnection(flags: Flags): Promise<NatsConnection> {
  const debug = flags.value<boolean>("debug");
  const ws = flags.value<boolean>("ws");
  if (ws) {
    runtime.stdout("using websockets\n");
  }
  const connectFn = ws ? wsconnect : await getConnectFn();

  if (typeof connectFn !== "function") {
    throw new Error("connectFn is not defined");
  }

  const conf = {
    debug,
    maxReconnectAttempts: -1,
    noAsyncTraces: true,
  } as ConnectionOptions;
  if (ws) {
    conf.servers = "ws://localhost";
  }

  const nc = await connectFn(conf);
  runtime.stdout(`connected ${nc.getServer()}\n`);
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === Events.Reconnect) {
        stats.reconnects++;
      }
    }
  })().then();

  nc.closed().then((err) => {
    if (err) {
      console.log(`connecting closed with error: ${err.message}`);
    }
  });

  waitForStop(nc as NatsConnectionImpl);

  return nc;
}

function waitForStop(nc: NatsConnectionImpl): void {
  console.log("control+c to terminate");
  runtime.signal("SIGTERM", () => {
    runtime.exit(0);
  });

  console.log("control+t to dump client status");
  runtime.signal("SIGINFO", () => {
    const subs = nc.protocol.subscriptions.all().map((s) => {
      const { subject, requestSubject } = s;
      return { subject, requestSubject: requestSubject || "" };
    });

    console.table(subs);
    //@ts-ignore: allow any
    const stats = nc.stats() as Record<string, unknown>;
    stats.currentServer = nc.getServer();
    console.table(stats);
  });
}

async function deleteFn(
  _cmd: Command,
  _args: string[],
  flags: Flags,
): Promise<number> {
  const stream = flags.value<string>("stream");
  const nc = await createConnection(flags);
  const jsm = await jetstreamManager(nc);
  await jsm.streams.delete(stream);
  await nc.close();
  return 0;
}

async function infoFn(
  _cmd: Command,
  _args: string[],
  flags: Flags,
): Promise<number> {
  const stream = flags.value<string>("stream");
  const nc = await createConnection(flags);
  const jsm = await jetstreamManager(nc);
  const si = await jsm.streams.info(stream);
  console.log(si);
  await nc.close();
  return 0;
}

async function initFn(
  _cmd: Command,
  _args: string[],
  flags: Flags,
): Promise<number> {
  const stream = flags.value<string>("stream");
  const n = flags.value<number>("count");
  const p = flags.value<number>("size");
  const batch = flags.value<number>("batch");
  const num_replicas = flags.value<number>("replication");

  const nc = await createConnection(flags);
  const jsm = await jetstreamManager(nc);

  try {
    await jsm.streams.info(stream);
    return Promise.reject(new Error(`stream "${stream}" already exists`));
  } catch (err) {
    if (err) {
      await jsm.streams.add({
        name: stream,
        subjects: [stream],
        allow_direct: true,
        num_replicas,
      });
      const js = jetstream(nc);
      const proms = [];
      const payload = new Uint8Array(p);

      const sendProgress = await progressFactory(n);

      for (let i = 1; i <= n; i++) {
        const p = js.publish(stream, payload);
        // nc.publish(stream, payload);
        sendProgress.update(i);
        proms.push(p);
        if (proms.length % batch === 0) {
          await nc.flush();
        }
      }
      if (proms.length) {
        await Promise.all(proms);
        proms.length = 0;
      }
    } else {
      throw err;
    }
  }
  await nc.close();
  return 0;
}

async function getConsumer(
  nc: NatsConnection,
  flags: Flags,
  opts: Partial<OrderedConsumerOptions> = {},
): Promise<Consumer | PushConsumer> {
  const stream = flags.value<string>("stream");
  const ordered = flags.value<boolean>("ordered");
  const push = flags.value<boolean>("push");
  const durable = flags.value<boolean>("durable");
  const ack_policy = ackPolicy(flags);
  const batch = flags.value<number>("batch");
  const num_replicas = flags.value<number>("replication");
  const jsm = await jetstreamManager(nc);

  const name = nuid.next();
  const src = {} as ConsumerConfig;
  if (durable) {
    src.durable_name = name;
  } else {
    src.name = name;
    src.inactive_threshold = nanos(2 * 60 * 1000);
  }
  if (push) {
    src.deliver_subject = createInbox();
    src.idle_heartbeat = nanos(30_000);
    src.flow_control = true;
  }

  opts = Object.assign({}, src, opts) as ConsumerConfig;

  if (!ordered) {
    const copts = opts as ConsumerConfig;
    if (ack_policy == AckPolicy.All) {
      copts.max_ack_pending = batch;
    }
    copts.num_replicas = num_replicas;
    copts.ack_policy = ack_policy;
    copts.name = name;
    copts.deliver_policy = DeliverPolicy.All;
    await jsm.consumers.add(stream, copts);
  }

  const js = jsm.jetstream();
  if (push) {
    return ordered
      ? js.consumers.getPushConsumer(stream)
      : js.consumers.getPushConsumer(stream, name);
  }
  return ordered
    ? js.consumers.get(stream, opts)
    : js.consumers.get(stream, name);
}

async function simplifiedFn(
  _cmd: Command,
  _args: string[],
  flags: Flags,
): Promise<number> {
  try {
    await checkFlags(flags);
    const callback = flags.value<boolean>("callback");
    const max_messages = flags.value<number>("batch");
    const idle_heartbeat = flags.value<number>("hb");
    const ack = ackPolicy(flags);
    const batch = flags.value<number>("batch");
    const work = flags.value<number>("work");
    const threshold = flags.value<number>("msg-threshold");
    const expires = flags.value<number>("expires");
    const isOrdered = flags.value<boolean>("ordered");

    const nc = await createConnection(flags);

    const consumer = await getConsumer(nc, flags);

    runtime.signal("SIGINFO", async () => {
      const ci = await consumer.info(true);
      console.log(ci);
    });

    const ci = await consumer.info(true);
    const d = deferred<void>();
    const progress = await progressFactory(ci.num_pending);
    let messages = 0;

    const cb = (r: JsMsg) => {
      messages++;
      switch (ack) {
        case AckPolicy.Explicit:
          r.ack();
          break;
        case AckPolicy.All:
          if (messages % batch === 0) {
            r.ack();
          }
          break;
        default:
          // nothing
      }
      if (work > 0 || messages % 1000 === 0) {
        progress.update(messages);
      }
      if (r.info.pending === 0) {
        d.resolve();
        r.ack();
      }
    };

    const opts = {
      max_messages,
      idle_heartbeat,
    } as Partial<ConsumeMessages>;
    if (callback) {
      opts.callback = cb;
    }
    if (threshold) {
      opts.threshold_messages = threshold;
    }
    if (expires) {
      opts.expires = expires;
    }

    const start = Date.now();
    const iter = await consumer.consume(opts);

    let notifications = 1;
    (async () => {
      for await (const s of iter.status()) {
        if (s.type !== ConsumerDebugEvents.Next) {
          await progress.log(`${notifications++} - ${s.type}: ${s.data}`);
        }
      }
    })().catch();

    (async () => {
      for await (const s of nc.status()) {
        if (s.type === Events.Disconnect || s.type === Events.Reconnect) {
          await progress.log(`${notifications++} - ${s.type}`);
        }
      }
    })().catch();

    const jsm = await jetstreamManager(nc);
    (async () => {
      for await (const a of jsm.advisories()) {
        switch (a.kind) {
          case AdvisoryKind.StreamLeaderElected:
            stats.streamLeader++;
            break;
          case AdvisoryKind.ConsumerLeaderElected:
            stats.consumerLeader++;
            break;
        }
        if (a.kind !== AdvisoryKind.API) {
          progress.log(`${notifications++} - ${a.kind}`);
        }
      }
    })().then();

    if (!callback) {
      (async () => {
        for await (const r of iter) {
          if (work) {
            await delay(work);
          }
          cb(r);
        }
      })().then(() => {
        console.log("iter closed");
      });
    }

    progress.update(messages);

    await d;
    progress.stop();
    perf(Date.now() - start, messages);
    await nc.close();
    if (isOrdered) {
      //@ts-ignore: internal
      stats.orderedResets = consumer.serial;
    }
    console.table(stats);
  } catch (err) {
    console.log(err);
    throw err;
  }
  return 0;
}

function perf(t: number, msgs: number) {
  runtime.stdout(
    `\nprocessed ${msgs} in ${t} millis: ${
      Math.floor((msgs / t) * 1000)
    } msgs/sec\n`,
  );
}
