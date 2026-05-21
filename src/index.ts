#!/usr/bin/env node

import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';

type JsonRpcId = string | number | null;

type JsonRpcMessage = {
  jsonrpc?: string;
  id?: JsonRpcId;
  method?: string;
  params?: unknown;
  result?: unknown;
  error?: unknown;
};

type JsonRpcRequest = JsonRpcMessage & {
  id: JsonRpcId;
  method: string;
};

class FramedJsonRpcStream {
  private buffer = Buffer.alloc(0);

  constructor(
    private readonly input: NodeJS.ReadableStream,
    private readonly output: NodeJS.WritableStream,
    private readonly onMessage: (message: JsonRpcMessage) => void,
    private readonly label: string,
  ) {
    this.input.on('data', (chunk: Buffer | string) => {
      this.pushChunk(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    this.input.on('error', (error) => {
      log(`${this.label} stream error: ${String(error)}`);
    });
  }

  send(message: JsonRpcMessage): void {
    const payload = Buffer.from(`${JSON.stringify({ jsonrpc: '2.0', ...message })}\n`, 'utf8');
    this.output.write(payload);
  }

  private pushChunk(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    while (true) {
      const lineEnd = this.buffer.indexOf('\n');
      if (lineEnd === -1) {
        return;
      }

      const payload = this.buffer.subarray(0, lineEnd).toString('utf8').replace(/\r$/, '');
      this.buffer = this.buffer.subarray(lineEnd + 1);

      if (!payload) {
        continue;
      }

      const parsed = JSON.parse(payload) as JsonRpcMessage;
      this.onMessage(parsed);
    }
  }
}

class MapboxMcpWrapper {
  private upstreamRequestCounter = 0;
  private clientRequestCounter = 0;
  private readonly clientRequestMap = new Map<string, JsonRpcId>();
  private readonly upstreamRequestMap = new Map<string, JsonRpcId>();
  private readonly pendingDirectionsRequests = new Set<string>();
  private readonly pendingEnrichments = new Map<string, { originalClientId: JsonRpcId; response: JsonRpcMessage }>();
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly clientTransport: FramedJsonRpcStream;
  private readonly upstreamTransport: FramedJsonRpcStream;

  constructor() {
    this.child = spawnUpstream();
    this.clientTransport = new FramedJsonRpcStream(process.stdin, process.stdout, (message) => {
      this.handleClientMessage(message);
    }, 'client');
    this.upstreamTransport = new FramedJsonRpcStream(this.child.stdout, this.child.stdin, (message) => {
      this.handleUpstreamMessage(message);
    }, 'upstream');

    this.child.stderr.on('data', (chunk: Buffer | string) => {
      process.stderr.write(chunk);
    });

    this.child.on('exit', (code, signal) => {
      log(`Upstream process exited code=${String(code)} signal=${String(signal)}`);
      process.exit(code ?? 1);
    });
  }

  private handleClientMessage(message: JsonRpcMessage): void {
    debug('client->wrapper', summarizeMessage(message));

    if (isRequest(message)) {
      const forwardedId = `client-${++this.upstreamRequestCounter}`;
      this.clientRequestMap.set(forwardedId, message.id);
      if (message.method === 'tools/call' && isRecord(message.params) && message.params.name === 'directions_tool') {
        this.pendingDirectionsRequests.add(forwardedId);
      }
      debug('wrapper->upstream', `${summarizeMessage(message)} rewrittenId=${forwardedId}`);
      this.upstreamTransport.send({
        ...rewriteInitializeForUpstream(message),
        id: forwardedId,
      });
      return;
    }

    if (isResponse(message) && typeof message.id === 'string') {
      const originalId = this.upstreamRequestMap.get(message.id);
      if (originalId === undefined) {
        log(`Ignoring unmatched client response id=${message.id}`);
        return;
      }

      this.upstreamRequestMap.delete(message.id);
      debug('wrapper->upstream', `${summarizeMessage(message)} restoredId=${String(originalId)}`);
      this.upstreamTransport.send({
        ...message,
        id: originalId,
      });
      return;
    }

    debug('wrapper->upstream', summarizeMessage(message));
    this.upstreamTransport.send(message);
  }

  private handleUpstreamMessage(message: JsonRpcMessage): void {
    debug('upstream->wrapper', summarizeMessage(message));

    if (isRequest(message) && message.method === 'elicitation/create') {
      log('Suppressing upstream elicitation/create by returning decline');
      this.upstreamTransport.send({
        id: message.id,
        result: {
          action: 'decline',
        },
      });
      return;
    }

    if (isRequest(message)) {
      const forwardedId = `upstream-${++this.clientRequestCounter}`;
      this.upstreamRequestMap.set(forwardedId, message.id);
      debug('wrapper->client', `${summarizeMessage(message)} rewrittenId=${forwardedId}`);
      this.clientTransport.send({
        ...message,
        id: forwardedId,
      });
      return;
    }

    if (isResponse(message) && typeof message.id === 'string') {
      // Enrichment responses (resource-read): not in clientRequestMap, handle first
      if (this.pendingEnrichments.has(message.id)) {
        const enrichment = this.pendingEnrichments.get(message.id)!;
        this.pendingEnrichments.delete(message.id);
        const enrichedResponse = applyGeometryEnrichment(enrichment.response, message);
        debug('wrapper->client', `Sending enriched directions response restoredId=${String(enrichment.originalClientId)}`);
        this.clientTransport.send({ ...enrichedResponse, id: enrichment.originalClientId });
        return;
      }

      const originalId = this.clientRequestMap.get(message.id);
      if (originalId === undefined) {
        log(`Ignoring unmatched upstream response id=${message.id}`);
        return;
      }

      this.clientRequestMap.delete(message.id);

      // Directions enrichment: intercept large-response case, read resource, then forward
      if (this.pendingDirectionsRequests.has(message.id)) {
        this.pendingDirectionsRequests.delete(message.id);
        const resourceUri = extractResourceUri(message);
        if (resourceUri) {
          const resourceReadId = `resource-read-${++this.upstreamRequestCounter}`;
          this.pendingEnrichments.set(resourceReadId, { originalClientId: originalId, response: message });
          log(`Fetching geometry from temp resource: ${resourceUri}`);
          this.upstreamTransport.send({ jsonrpc: '2.0', method: 'resources/read', id: resourceReadId, params: { uri: resourceUri } });
          return;
        }
      }

      debug('wrapper->client', `${summarizeMessage(message)} restoredId=${String(originalId)}`);
      this.clientTransport.send({
        ...message,
        id: originalId,
      });
      return;
    }

    debug('wrapper->client', summarizeMessage(message));
    this.clientTransport.send(message);
  }
}

function rewriteInitializeForUpstream(message: JsonRpcRequest): JsonRpcRequest {
  if (message.method !== 'initialize' || !isRecord(message.params)) {
    return message;
  }

  const params = { ...message.params };
  if (isRecord(params.capabilities)) {
    const capabilities = { ...params.capabilities };
    delete capabilities.elicitation;
    params.capabilities = capabilities;
  }

  return {
    ...message,
    params,
  };
}

function spawnUpstream(): ChildProcessWithoutNullStreams {
  const localBin = join(dirname(__dirname), 'node_modules', '.bin', process.platform === 'win32' ? 'mcp-server.cmd' : 'mcp-server');
  if (existsSync(localBin)) {
    return spawn(localBin, [], {
      stdio: 'pipe',
      env: process.env,
    });
  }

  log('Local upstream binary not found, falling back to npx -y @mapbox/mcp-server');
  return spawn('npx', ['-y', '@mapbox/mcp-server'], {
    stdio: 'pipe',
    env: process.env,
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isRequest(message: JsonRpcMessage): message is JsonRpcRequest {
  return typeof message.method === 'string' && Object.prototype.hasOwnProperty.call(message, 'id');
}

function isResponse(message: JsonRpcMessage): boolean {
  return message.id !== undefined && message.method === undefined;
}

function log(message: string): void {
  process.stderr.write(`[mapbox-mcp-wrapper] ${message}\n`);
}

function debug(direction: string, message: string): void {
  if (process.env.MAPBOX_MCP_WRAPPER_DEBUG !== '1') {
    return;
  }

  log(`${direction} ${message}`);
}

function extractResourceUri(response: JsonRpcMessage): string | null {
  if (!isRecord(response.result)) return null;
  const content = response.result['content'];
  if (!Array.isArray(content)) return null;
  for (const item of content) {
    if (isRecord(item) && typeof item['text'] === 'string') {
      const match = /Resource URI: (mapbox:\/\/temp\/directions-[a-f0-9]+)/.exec(item['text'] as string);
      if (match) return match[1];
    }
  }
  return null;
}

function applyGeometryEnrichment(
  originalResponse: JsonRpcMessage,
  resourceReadResponse: JsonRpcMessage,
): JsonRpcMessage {
  if (isRecord(resourceReadResponse.error)) {
    log('resources/read returned an error; forwarding original directions response without geometry');
    return originalResponse;
  }

  if (!isRecord(originalResponse.result) || !isRecord(resourceReadResponse.result)) {
    return originalResponse;
  }

  const contents = resourceReadResponse.result['contents'];
  if (!Array.isArray(contents) || contents.length === 0) return originalResponse;

  const firstContent = contents[0];
  if (!isRecord(firstContent) || typeof firstContent['text'] !== 'string') return originalResponse;

  let fullData: unknown;
  try {
    fullData = JSON.parse(firstContent['text'] as string);
  } catch {
    log('Failed to parse resource content as JSON; forwarding original directions response');
    return originalResponse;
  }

  if (!isRecord(fullData)) return originalResponse;

  const routes = fullData['routes'];
  if (!Array.isArray(routes) || routes.length === 0) return originalResponse;
  const geometry = isRecord(routes[0]) ? routes[0]['geometry'] : undefined;
  if (!geometry) {
    log('No geometry in resource data; forwarding original directions response');
    return originalResponse;
  }

  const result = originalResponse.result;
  const structuredContent = result['structuredContent'];
  if (!isRecord(structuredContent)) return originalResponse;

  const scRoutes = structuredContent['routes'];
  if (!Array.isArray(scRoutes) || scRoutes.length === 0) return originalResponse;

  const patchedRoutes = scRoutes.map((route: unknown, i: number) =>
    i === 0 && isRecord(route) ? { ...route, geometry } : route,
  );

  const patchedContent = Array.isArray(result['content'])
    ? (result['content'] as unknown[]).map((item: unknown) => {
        if (isRecord(item) && typeof item['text'] === 'string') {
          return { ...item, text: (item['text'] as string).replace(/\n⚠️ Full response[\s\S]*$/, '') };
        }
        return item;
      })
    : result['content'];

  log(`Geometry enrichment applied: ${(geometry as Record<string, unknown>)['type']} with ${((geometry as Record<string, unknown>)['coordinates'] as unknown[])?.length ?? '?'} coordinates`);

  return {
    ...originalResponse,
    result: {
      ...result,
      content: patchedContent,
      structuredContent: { ...structuredContent, routes: patchedRoutes },
    },
  };
}

function summarizeMessage(message: JsonRpcMessage): string {
  if (isRequest(message)) {
    return `request method=${message.method} id=${String(message.id)}`;
  }

  if (isResponse(message)) {
    return `response id=${String(message.id)}`;
  }

  if (typeof message.method === 'string') {
    return `notification method=${message.method}`;
  }

  return 'message';
}

new MapboxMcpWrapper();