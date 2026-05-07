export interface ConfigProject {
  id: string;
  projectCode: string;
  brand: string;
  model: string;
  marketCountry: string;
  displayName: string;
  status: string;
  createdAt: string;
  updatedAt: string;
}

export interface MsrpSource {
  id: string;
  sourceCode: string;
  country: string;
  brand: string;
  sourceUrl: string;
  sourceType: string;
  tier: number;
  extractorName: string;
  extractorVersion: string;
  priceSemantics: string;
  requiresLocation: boolean;
  enabled: boolean;
  notes: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface MatchOverride {
  id: string;
  country: string;
  brand: string;
  jatoModel: string;
  jatoTrim: string;
  jatoPowertrain: string | null;
  officialModel: string;
  officialTrim: string;
  validFromDate: string;
  validToDate: string | null;
  overrideReason: string;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface DataManagementDatabaseStatus {
  enabled: boolean;
  connected: boolean;
  detail?: string | null;
}

export interface DataManagementMetric {
  label: string;
  value: string | number;
  tone?: string;
}

export interface DataManagementRecentItem {
  label: string;
  value: string | number;
  updatedAt?: string | null;
}

export interface DataManagementDomain {
  key: string;
  label: string;
  status: string;
  storage: string;
  updatedAt?: string | null;
  summary: string;
  metrics: DataManagementMetric[];
  recentItems: DataManagementRecentItem[];
}

export interface DataManagementFileItem {
  key: string;
  label: string;
  kind: string;
  path: string;
  exists: boolean;
  isDir: boolean;
  sizeBytes?: number | null;
  fileCount?: number | null;
  updatedAt?: string | null;
}

export interface DataManagementTableItem {
  key: string;
  label: string;
  domain: string;
  schema: string;
  table: string;
  rowCount: number;
  lastEventAt?: string | null;
  status: string;
}

export interface DataManagementActivityDay {
  date: string;
  count: number;
  level: number;
}

export interface DataManagementActivitySourceCount {
  label: string;
  count: number;
}

export interface DataManagementActivity {
  days: DataManagementActivityDay[];
  maxCount: number;
  totalCount: number;
  rangeStart: string;
  rangeEnd: string;
  sourceCounts: DataManagementActivitySourceCount[];
  databaseConnected: boolean;
}

export interface DataManagementAirflowService {
  service: string;
  state: string;
  status: string;
  health?: string | null;
  containerName?: string | null;
  publishedPorts: string[];
}

export interface DataManagementAirflowActions {
  canStart: boolean;
  canStop: boolean;
  canOpenUi: boolean;
}

export interface DataManagementAirflowStatus {
  available: boolean;
  mode: string;
  detail: string;
  uiUrl: string;
  running: boolean;
  runningServices: number;
  totalServices: number;
  updatedAt: string;
  services: DataManagementAirflowService[];
  actions: DataManagementAirflowActions;
}

export interface DataManagementAirflowActionResponse {
  action: string;
  detail: string;
  status: DataManagementAirflowStatus;
}

export interface DataManagementVocSyncResponse {
  root: string;
  countryCount: number;
  sourceRunCount: number;
  documentCount: number;
  errorCount: number;
}

export interface DataManagementVocCountryOption {
  code: string;
  label: string;
  status: string;
  updatedAt?: string | null;
  rawSourceCount: number;
  rawDocumentCount: number;
  publishReadyCount: number;
  signalObservationCount: number;
  deckReady: boolean;
}

export interface DataManagementVocArtifact {
  key: string;
  label: string;
  kind: string;
  path: string;
  exists: boolean;
  isDir: boolean;
  sizeBytes?: number | null;
  fileCount?: number | null;
  updatedAt?: string | null;
}

export interface DataManagementVocSourceRun {
  sourceCode: string;
  siteName: string;
  siteType: string;
  language?: string | null;
  publishTier?: string | null;
  publishDecision?: string | null;
  documentCount: number;
  publishReadyCount: number;
  errorCount: number;
  updatedAt?: string | null;
  path: string;
  textExtractionMethods: string[];
}

export interface DataManagementVocShareItem {
  label: string;
  rawLabel?: string | null;
  count: number;
  sharePct: number;
  mentionCount?: number | null;
}

export interface DataManagementVocEvidenceCard {
  title: string;
  url: string;
  siteName: string;
  publishTier: string;
  signals: string[];
  snippet: string;
}

export interface DataManagementVocDocumentationRef {
  label: string;
  path: string;
  exists: boolean;
  isDir: boolean;
  sizeBytes?: number | null;
  fileCount?: number | null;
  updatedAt?: string | null;
}

export interface DataManagementVocStagingStatus {
  databaseConnected: boolean;
  sourceRunCount: number;
  documentCount: number;
  publishReadyCount: number;
  latestCollectedAt?: string | null;
}

export interface DataManagementVocOverviewResponse {
  generatedAt: string;
  selectedCountryCode: string;
  selectedCountryLabel: string;
  availableCountries: DataManagementVocCountryOption[];
  overallMetrics: DataManagementMetric[];
  countryMetrics: DataManagementMetric[];
  artifacts: DataManagementVocArtifact[];
  sourceRuns: DataManagementVocSourceRun[];
  observedSections: string[];
  inferredSections: string[];
  topPainPoints: DataManagementVocShareItem[];
  topProductSignals: DataManagementVocShareItem[];
  evidenceCards: DataManagementVocEvidenceCard[];
  documentation: DataManagementVocDocumentationRef[];
  staging: DataManagementVocStagingStatus;
}

export interface DataManagementOverviewResponse {
  generatedAt: string;
  database: DataManagementDatabaseStatus;
  domains: DataManagementDomain[];
  airflow: DataManagementAirflowStatus;
  fileInventory: DataManagementFileItem[];
  databaseTables: DataManagementTableItem[];
  activity: DataManagementActivity;
}
