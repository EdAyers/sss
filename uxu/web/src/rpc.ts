type Id = string | number;
declare const workspace_dir: string;
interface RpcRequest {
  id?: Id;
  method: string;
  params: any;
  jsonrpc: "2.0";
}

interface RpcNotification {
  method: string;
  params: any;
  jsonrpc: "2.0";
}

enum RpcErrorCode {
  ParseError = -32700,
  InvalidRequest = -32600,
  MethodNotFound = -32601,
  InvalidParams = -32602,
  InternalError = -32603,
  ServerError = -32000,
}

interface RpcResponseError {
  jsonrpc: "2.0";
  id: Id;
  error: {
    code: RpcErrorCode;
    message: string;
    data?: any;
  };
}
interface RpcResponseSuccess {
  jsonrpc: "2.0";
  id: Id;
  result: any;
}
type RpcResponse = RpcResponseError | RpcResponseSuccess;

type RpcMessage = RpcRequest | RpcNotification | RpcResponse;

function isError(x: RpcMessage): x is RpcResponseError {
  const xx = x as any;
  return xx.error !== undefined;
}
function isNotification(x: RpcMessage): x is RpcNotification {
  const xx = x as any;
  return xx.id === undefined && xx.method !== undefined;
}
function isRequest(x: RpcMessage): x is RpcRequest {
  const xx = x as any;
  return xx.id !== undefined && xx.method !== undefined;
}
function isResponse(x: RpcMessage): x is RpcResponseSuccess {
  const xx = x as any;
  return xx.result !== undefined;
}
function success(id: Id, result: any): RpcResponseSuccess {
  return { jsonrpc: "2.0", result, id };
}
function error(
  id: Id,
  message: string,
  code = RpcErrorCode.ServerError
): RpcResponseError {
  return {
    jsonrpc: "2.0",
    error: {
      code,
      message,
    },
    id,
  };
}
function methodNotFound(id: Id, method: string): RpcResponseError {
  return error(id, `Method ${method} not found`, RpcErrorCode.MethodNotFound);
}

interface Future {
  resolve(x: any): void;
  reject(err: any): void;
}

class Pubsub<T> {
  private count = 0;
  readonly subs: Map<number, (t: T) => void> = new Map();
  /* Subscribe and return a disposal function. */
  sub(handler: (t: T) => void): () => void {
    const id = this.count++;
    this.subs.set(id, handler);
    return () => this.subs.delete(id);
  }
  pub(t: T): void {
    this.subs.forEach((s) => s(t));
  }
}

export class JsonRpc {
  pending = new Map<Id, Future>();
  methods = new Map<string, (params: any) => Promise<any>>();
  notifications = new Map<string, Pubsub<any>>();
  count = 0;
  constructor(readonly transport: WebSocket) {
    transport.addEventListener("message", this.handleMessage.bind(this));
  }

  getNotification(method: string): Pubsub<any> {
    if (!this.notifications.has(method)) {
      this.notifications.set(method, new Pubsub());
    }
    return this.notifications.get(method)!;
  }

  sub(method: string, handler: (t: any) => void) {
    const p = this.getNotification(method);
    return p.sub(handler);
  }

  async handleCore(msg: RpcMessage) {
    console.log("handling: ", msg);
    if (isNotification(msg)) {
      this.getNotification(msg.method).pub(msg.params);
      const method = this.methods.get(msg.method);
      if (method) {
        await method(msg.params);
      }
    } else if (isRequest(msg)) {
      const method = this.methods.get(msg.method);
      if (!method) {
        this.sendMessage(methodNotFound(msg.id, msg.method));
      } else {
        try {
          const result = await method(msg.params);
          this.sendMessage(success(msg.id, result));
        } catch (e) {
          const em = (e as any).message ?? "Unknown error";
          this.sendMessage(error(msg.id, em));
        }
      }
    } else if (isResponse(msg) || isError(msg)) {
      if (msg.id === undefined) {
        if (isError(msg)) {
          console.error(msg.error.code, msg.error.message);
        } else {
          console.error("got a response without an id", msg);
        }
        return
      }
      const fut = this.pending.get(msg.id);
      if (!fut) {
        console.error(`no pending future for ${msg.id}, ignoring`);
        return;
      }
      this.pending.delete(msg.id);
      if (isResponse(msg)) {
        fut.resolve(msg.result);
      } else {
        fut.reject(msg.error);
      }
    } else {
      console.error(`unrecognised message object`, msg);
    }
  }

  async handleMessage(ev: MessageEvent<string | Blob>) {
    let data: string;
    if (ev.data instanceof Blob) {
      data = await ev.data.text();
    } else if (typeof ev.data === "string") {
      data = ev.data;
    } else {
      throw new TypeError(`Message data must be a string but was ${ev.data}`);
    }

    const msg: RpcMessage | RpcMessage[] = JSON.parse(data);
    const msgs = msg instanceof Array ? msg : [msg];
    msgs.forEach((msg) => this.handleCore(msg));
  }

  /** Encode the message and use transport to send it. */
  sendMessage(message: RpcMessage) {
    const s = JSON.stringify(message);
    this.transport.send(s);
  }

  register(method: string, fn: (params: any) => Promise<any>) {
    this.methods.set(method, fn);
  }

  notify(method: string, params: any) {
    const req: RpcNotification = {
      method,
      params,
      jsonrpc: "2.0" as const,
    };
    this.sendMessage(req);
  }

  request(method: string, params: any) {
    const id = this.count++;
    const req = {
      id,
      method,
      params,
      jsonrpc: "2.0" as const,
    };
    return new Promise<any>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      console.log("sending", req);
      this.sendMessage(req);
    });
  }
}
