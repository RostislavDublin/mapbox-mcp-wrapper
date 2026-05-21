#!/usr/bin/env node

import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { existsSync, writeFileSync } from 'node:fs';
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

type SyntheticLeg = {
  from: [number, number];
  to: [number, number];
  name: string;
  via?: string;
};

type SyntheticLegResult = {
  geometry: unknown;
  distance_km: number;
  duration_min: number;
  via: string;
};

type SyntheticContext = {
  clientId: JsonRpcId;
  legs: SyntheticLeg[];
  profile: string;
  outputPath?: string;
  pendingCount: number;
  results: Array<SyntheticLegResult | null>;
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
  private readonly pendingEnrichments = new Map<string, { forwardedId: string; originalClientId: JsonRpcId; response: JsonRpcMessage }>();
  private readonly pendingToolsListRequests = new Set<string>();
  private readonly syntheticLegMap = new Map<string, { contextId: string; legIndex: number }>();
  private readonly syntheticContexts = new Map<string, SyntheticContext>();
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
      if (message.method === 'tools/call' && isRecord(message.params) && message.params.name === 'build_route_geojson') {
        this.handleSyntheticBuildRouteGeojson(message);
        return;
      }
      const forwardedId = `client-${++this.upstreamRequestCounter}`;
      this.clientRequestMap.set(forwardedId, message.id);
      if (message.method === 'tools/call' && isRecord(message.params) && message.params.name === 'directions_tool') {
        this.pendingDirectionsRequests.add(forwardedId);
      }
      if (message.method === 'tools/list') {
        this.pendingToolsListRequests.add(forwardedId);
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
        this.deliverResponseToClient(enrichment.forwardedId, enrichment.originalClientId, enrichedResponse);
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
          this.pendingEnrichments.set(resourceReadId, { forwardedId: message.id, originalClientId: originalId, response: message });
          log(`Fetching geometry from temp resource: ${resourceUri}`);
          this.upstreamTransport.send({ jsonrpc: '2.0', method: 'resources/read', id: resourceReadId, params: { uri: resourceUri } });
          return;
        }
      }

      if (this.pendingToolsListRequests.has(message.id)) {
        this.pendingToolsListRequests.delete(message.id);
        debug('wrapper->client', `tools/list: injecting synthetic tools, restoredId=${String(originalId)}`);
        this.deliverResponseToClient(message.id, originalId, addSyntheticTools(message));
        return;
      }

      debug('wrapper->client', `${summarizeMessage(message)} restoredId=${String(originalId)}`);
      this.deliverResponseToClient(message.id, originalId, message);
      return;
    }

    debug('wrapper->client', summarizeMessage(message));
    this.clientTransport.send(message);
  }

  private handleSyntheticBuildRouteGeojson(message: JsonRpcRequest): void {
    if (!isRecord(message.params)) {
      this.clientTransport.send({ id: message.id, error: { code: -32602, message: 'Invalid params' } });
      return;
    }
    const args = message.params['arguments'];
    if (!isRecord(args)) {
      this.clientTransport.send({ id: message.id, error: { code: -32602, message: 'arguments is required' } });
      return;
    }
    const legsRaw = args['legs'];
    if (!Array.isArray(legsRaw) || legsRaw.length === 0) {
      this.clientTransport.send({ id: message.id, error: { code: -32602, message: 'legs must be a non-empty array' } });
      return;
    }
    const profile = typeof args['profile'] === 'string' ? args['profile'] : 'mapbox/driving';
    const outputPath = typeof args['output_path'] === 'string' ? args['output_path'] : undefined;
    const contextId = `synth-${++this.upstreamRequestCounter}`;
    const syntheticLegs: SyntheticLeg[] = legsRaw.map((leg: unknown) => {
      if (!isRecord(leg)) throw new Error('Invalid leg entry');
      return {
        from: leg['from'] as [number, number],
        to: leg['to'] as [number, number],
        name: String(leg['name'] ?? ''),
        via: typeof leg['via'] === 'string' ? leg['via'] : undefined,
      };
    });
    const context: SyntheticContext = {
      clientId: message.id,
      legs: syntheticLegs,
      profile,
      outputPath,
      pendingCount: syntheticLegs.length,
      results: new Array(syntheticLegs.length).fill(null) as Array<SyntheticLegResult | null>,
    };
    this.syntheticContexts.set(contextId, context);
    log(`Synthetic build_route_geojson: starting ${syntheticLegs.length} legs, profile=${profile}`);
    syntheticLegs.forEach((leg, legIndex) => {
      const forwardedId = `client-${++this.upstreamRequestCounter}`;
      this.clientRequestMap.set(forwardedId, message.id);
      this.pendingDirectionsRequests.add(forwardedId);
      this.syntheticLegMap.set(forwardedId, { contextId, legIndex });
      this.upstreamTransport.send({
        jsonrpc: '2.0',
        method: 'tools/call',
        id: forwardedId,
        params: {
          name: 'directions_tool',
          arguments: {
            coordinates: [
              { longitude: leg.from[0], latitude: leg.from[1] },
              { longitude: leg.to[0], latitude: leg.to[1] },
            ],
            routing_profile: profile,
            geometries: 'geojson',
            alternatives: false,
          },
        },
      });
    });
  }

  private deliverResponseToClient(forwardedId: string, originalClientId: JsonRpcId, response: JsonRpcMessage): void {
    if (this.syntheticLegMap.has(forwardedId)) {
      const { contextId, legIndex } = this.syntheticLegMap.get(forwardedId)!;
      this.syntheticLegMap.delete(forwardedId);
      this.collectSyntheticLegResult(contextId, legIndex, response);
      return;
    }
    this.clientTransport.send({ ...response, id: originalClientId });
  }

  private collectSyntheticLegResult(contextId: string, legIndex: number, response: JsonRpcMessage): void {
    const context = this.syntheticContexts.get(contextId);
    if (!context) {
      log(`Synthetic context ${contextId} not found for leg ${legIndex}`);
      return;
    }
    const sc = isRecord(response.result) ? response.result['structuredContent'] : null;
    const routes = isRecord(sc) && Array.isArray(sc['routes']) ? sc['routes'] : [];
    const route0 = routes[0];
    const geometry = isRecord(route0) ? route0['geometry'] : null;
    const distanceM = isRecord(route0) && typeof route0['distance'] === 'number' ? route0['distance'] : 0;
    const durationS = isRecord(route0) && typeof route0['duration'] === 'number' ? route0['duration'] : 0;
    const leg = context.legs[legIndex];
    context.results[legIndex] = {
      geometry: geometry ?? { type: 'LineString', coordinates: [] },
      distance_km: Math.round(distanceM / 1000),
      duration_min: Math.round(durationS / 60),
      via: leg.via ?? '',
    };
    context.pendingCount--;
    log(`Synthetic build_route_geojson: leg ${legIndex + 1}/${context.legs.length} done (${Math.round(distanceM / 1000)}km)`);
    if (context.pendingCount === 0) {
      this.finalizeSyntheticBuildRouteGeojson(contextId);
    }
  }

  private finalizeSyntheticBuildRouteGeojson(contextId: string): void {
    const context = this.syntheticContexts.get(contextId)!;
    this.syntheticContexts.delete(contextId);
    const features = context.legs.map((leg, i) => ({
      type: 'Feature',
      geometry: context.results[i]?.geometry ?? { type: 'LineString', coordinates: [] },
      properties: {
        leg: leg.name,
        distance_km: context.results[i]?.distance_km ?? 0,
        duration_min: context.results[i]?.duration_min ?? 0,
        via: context.results[i]?.via ?? leg.via ?? '',
      },
    }));
    const geojson = { type: 'FeatureCollection', features };
    const totalKm = context.results.reduce((s, r) => s + (r?.distance_km ?? 0), 0);
    const totalMin = context.results.reduce((s, r) => s + (r?.duration_min ?? 0), 0);
    const h = Math.floor(totalMin / 60);
    const m = totalMin % 60;
    const legLines = context.legs
      .map((leg, i) => {
        const r = context.results[i];
        const lh = Math.floor((r?.duration_min ?? 0) / 60);
        const lm = (r?.duration_min ?? 0) % 60;
        return `  ${leg.name}: ${r?.distance_km ?? '?'}km, ${lh}h${lm}m${leg.via ? ', via ' + leg.via : ''}`;
      })
      .join('\n');
    const text = `Route GeoJSON built: ${context.legs.length} legs\n${legLines}\nTotal: ${totalKm}km, ${h}h${m}m`;

    if (context.outputPath) {
      try {
        writeFileSync(context.outputPath, JSON.stringify(geojson), 'utf8');
        log(`Synthetic build_route_geojson: GeoJSON written to ${context.outputPath}`);
        this.clientTransport.send({
          jsonrpc: '2.0',
          id: context.clientId,
          result: {
            content: [{ type: 'text', text: `${text}\n\nGeoJSON written to: ${context.outputPath}` }],
            structuredContent: {
              file_path: context.outputPath,
              legs: context.legs.length,
              total_km: totalKm,
              total_h: h,
              total_m: m,
            },
          },
        });
        log(`Synthetic build_route_geojson finalized: ${context.legs.length} legs, ${totalKm}km, ${h}h${m}m → ${context.outputPath}`);
        return;
      } catch (err) {
        log(`Failed to write ${context.outputPath}: ${String(err)} — falling back to inline response`);
      }
    }

    this.clientTransport.send({
      jsonrpc: '2.0',
      id: context.clientId,
      result: {
        content: [{ type: 'text', text }],
        structuredContent: geojson,
      },
    });
    log(`Synthetic build_route_geojson finalized: ${context.legs.length} legs, ${totalKm}km, ${h}h${m}m`);
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

const SYNTHETIC_TOOL_DEFINITIONS: unknown[] = [
  {
    name: 'build_route_geojson',
    description:
      'Build a GeoJSON FeatureCollection with real road geometry for a multi-leg route. ' +
      'Calls directions_tool for each leg internally and assembles the result. ' +
      'Use this instead of calling directions_tool repeatedly when you need a complete multi-leg route GeoJSON.',
    inputSchema: {
      type: 'object',
      properties: {
        legs: {
          type: 'array',
          description: 'Ordered list of route legs',
          items: {
            type: 'object',
            properties: {
              from: { type: 'array', items: { type: 'number' }, minItems: 2, maxItems: 2, description: '[longitude, latitude] of leg start' },
              to: { type: 'array', items: { type: 'number' }, minItems: 2, maxItems: 2, description: '[longitude, latitude] of leg end' },
              name: { type: 'string', description: 'Human-readable leg name, e.g. "Las Vegas to Springdale"' },
              via: { type: 'string', description: 'Optional road/highway names to include in output properties' },
            },
            required: ['from', 'to', 'name'],
          },
        },
        profile: {
          type: 'string',
          enum: ['mapbox/driving', 'mapbox/driving-traffic', 'mapbox/walking', 'mapbox/cycling'],
          default: 'mapbox/driving',
          description: 'Routing profile (default: mapbox/driving)',
        },
        output_path: {
          type: 'string',
          description:
            'Optional absolute file path to write the GeoJSON to (e.g. "/Users/alice/route.geojson"). ' +
            'When provided, the file is written directly by the server and the response is compact (summary + path). ' +
            'When omitted, the full GeoJSON FeatureCollection is returned inline in structuredContent.',
        },
      },
      required: ['legs'],
    },
  },
];

function addSyntheticTools(response: JsonRpcMessage): JsonRpcMessage {
  if (!isRecord(response.result) || !Array.isArray(response.result['tools'])) {
    return response;
  }
  return {
    ...response,
    result: {
      ...response.result,
      tools: [...(response.result['tools'] as unknown[]), ...SYNTHETIC_TOOL_DEFINITIONS],
    },
  };
}

new MapboxMcpWrapper();