export interface CandidateRuntimeLocation {
  hostname: string;
  port: string;
}

export const CANDIDATE_RUNTIME_IDENTITY = {
  role: "admin",
  username: "candidate",
} as const;

export function isCandidatePreviewOrigin(location: CandidateRuntimeLocation): boolean {
  const hostname = location.hostname.toLowerCase().replace(/\.$/, "");
  if (hostname === "candidate.ojeur.cloud") return location.port === "";
  return (hostname === "127.0.0.1" || hostname === "localhost") && location.port === "18002";
}
