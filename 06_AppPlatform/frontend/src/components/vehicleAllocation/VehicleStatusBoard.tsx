import type { PiVehicleUnit, VehicleStatusFlowConfig, VehicleStatusFlowStep } from "../../types/orderGeniusVehicle";

interface VehicleStatusBoardProps {
  scopeLabel: string;
  vehicles: PiVehicleUnit[];
  statusFlow: VehicleStatusFlowConfig | null;
}

interface VehicleStatusCount {
  key: string;
  labelEn: string;
  labelZh: string;
  color: string;
  count: number;
}

function countByStatus(
  steps: VehicleStatusFlowStep[],
  vehicles: PiVehicleUnit[],
  field: "allocationStatus" | "logisticsStatus",
): VehicleStatusCount[] {
  return steps.map((step) => ({
    key: step.key,
    labelEn: step.labelEn,
    labelZh: step.labelZh,
    color: step.color,
    count: vehicles.filter((vehicle) => vehicle[field] === step.key).length,
  }));
}

function flowScopeLabel(statusFlow: VehicleStatusFlowConfig | null): string {
  if (!statusFlow) {
    return "Flow default · -";
  }
  const parts = [
    `Flow ${statusFlow.source}`,
    statusFlow.countryCode ?? "-",
    statusFlow.orderingAccountCode,
  ].filter((item): item is string => Boolean(item));
  return parts.join(" · ");
}

export function VehicleStatusBoard({
  scopeLabel,
  vehicles,
  statusFlow,
}: VehicleStatusBoardProps) {
  const logistics = statusFlow ? countByStatus(statusFlow.logistics, vehicles, "logisticsStatus") : [];
  const allocation = statusFlow ? countByStatus(statusFlow.allocation, vehicles, "allocationStatus") : [];
  const noVin = vehicles.filter((vehicle) => !vehicle.vin).length;

  return (
    <section className="va-tool-card">
      <div className="va-tool-card-head">
        <span>Status board</span>
        <strong>{scopeLabel}</strong>
      </div>
      <p>{flowScopeLabel(statusFlow)}</p>
      <div className="va-tool-status-grid">
        <div><span>Vehicles</span><strong>{vehicles.length}</strong></div>
        <div><span>No VIN</span><strong>{noVin}</strong></div>
      </div>
      <div className="va-tool-pill-grid">
        {logistics.map((item) => (
          <span
            key={`logistics-${item.key}`}
            className={item.count === 0 ? "is-empty" : undefined}
            style={{ borderColor: item.color }}
            aria-label={`Logistics ${item.labelEn} ${item.count} vehicles`}
          >
            <span>{item.labelEn} · {item.labelZh}</span>
            <strong>{item.count}</strong>
          </span>
        ))}
        {allocation.map((item) => (
          <span
            key={`allocation-${item.key}`}
            className={item.count === 0 ? "is-empty" : undefined}
            style={{ borderColor: item.color }}
            aria-label={`Allocation ${item.labelEn} ${item.count} vehicles`}
          >
            <span>{item.labelEn} · {item.labelZh}</span>
            <strong>{item.count}</strong>
          </span>
        ))}
        {logistics.length === 0 && allocation.length === 0 ? (
          <span>No selected PI scope or status flow config.</span>
        ) : null}
      </div>
    </section>
  );
}
