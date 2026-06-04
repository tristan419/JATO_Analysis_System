export interface LeaseOffer {
  offerId: string;
  countryCode: string;
  currency: string;
  brand: string;
  modelName: string;
  version?: string | null;
  powertrain?: string | null;
  segment?: string | null;
  leaseType: "private" | "fleet" | "financial";
  provider?: string | null;
  status: "draft" | "active" | "expired" | "archived" | "scenario";
  fxRateToEur?: number | null;
  fxRateDate?: string | null;
  fxSource?: string | null;
  fxLocked?: boolean;
  monthlyPayment?: number | null;
  monthlyPaymentEur?: number | null;
  effectiveMonthlyEur?: number | null;
  downPayment?: number | null;
  downPaymentEur?: number | null;
  upfrontAmount?: number | null;
  upfrontTreatment?: string | null;
  termMonths?: number | null;
  mileagePerYear?: number | null;
  capCost?: number | null;
  capCostEur?: number | null;
  residualValue?: number | null;
  residualValueEur?: number | null;
  residualValuePercent?: number | null;
  aprPercent?: number | null;
  moneyFactor?: number | null;
  aprSource?: string | null;
  rvGuaranteed?: boolean | null;
  serviceIncluded?: boolean | null;
  insuranceIncluded?: boolean | null;
  tyreIncluded?: boolean | null;
  vatIncluded?: boolean | null;
  depositRequired?: boolean | null;
  depositRefundable?: boolean | null;
  sourceType?: string | null;
  sourceUrl?: string | null;
  effectiveDate?: string | null;
  expiryDate?: string | null;
  totalContractCostEur?: number | null;
  riskLevel?: string | null;
  notes?: string | null;
  createdBy?: string | null;
  updatedBy?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
  rowVersion?: number;
  versions?: LeaseOfferVersion[];
}

export interface LeaseOfferVersion {
  versionId: string;
  versionNo: number;
  changedBy?: string | null;
  changeReason?: string | null;
  changedAt?: string | null;
}

export interface LeaseCompareSet {
  compareId: string;
  name: string;
  countryCode?: string | null;
  selectedOfferIds: string[];
  createdBy?: string | null;
  createdAt?: string | null;
}

export interface SolveRequest {
  solveFor: "monthly_payment" | "money_factor" | "cap_cost" | "residual_value";
  monthlyPayment?: number;
  capCost?: number;
  residualValue?: number;
  termMonths?: number;
  moneyFactor?: number;
}

export interface SolveResult {
  monthlyPayment?: number;
  moneyFactor?: number;
  aprPercent?: number;
  capCost?: number;
  residualValue?: number;
  residualValuePercent?: number;
  aprSource?: string;
}
