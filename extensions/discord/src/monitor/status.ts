type DiscordMonitorStatusPatch = {
  connected?: boolean;
  lastEventAt?: number | null;
  lastTransportActivityAt?: number | null;
  lastDispatchAt?: number | null;
  lastDispatchType?: string | null;
  lastMessageCreateAt?: number | null;
  lastConnectedAt?: number | null;
  lastDisconnect?:
    | string
    | {
        at: number;
        status?: number;
        error?: string;
        loggedOut?: boolean;
      }
    | null;
  lastInboundAt?: number | null;
  lastError?: string | null;
  busy?: boolean;
  activeRuns?: number;
  lastRunActivityAt?: number | null;
  appInboundWatchdogEnabled?: boolean;
};

export type DiscordMonitorStatusSink = (patch: DiscordMonitorStatusPatch) => void;
