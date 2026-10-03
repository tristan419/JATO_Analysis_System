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
      <p title={flowScopeLabel(statusFlow)}>{scopeLabel} · {vehicles.length} vehicles / 台 · {noVin} awaiting VIN / 待录 VIN</p>
      <div className="va-status-counts">
        {[{ label: "Logistics / 物流", name: "Logistics", items: logistics }, { label: "Allocation / 分配", name: "Allocation", items: allocation }].map((group) => (
          <section key={group.name} aria-label={group.label}><h4>{group.label}</h4>
            {group.items.map((item) => <div className={item.count === 0 ? "is-empty" : undefined} key={item.key} aria-label={`${group.name} ${item.labelEn} ${item.count} vehicles`}>
              <span>{item.labelEn} · {item.labelZh}</span><strong>{item.count}</strong>
              <div className="va-count-track"><span style={{ width: `${vehicles.length ? item.count / vehicles.length * 100 : 0}%`, background: item.color }} /></div>
            </div>)}
          </section>
        ))}
        {logistics.length === 0 && allocation.length === 0 ? (
          <span>No selected PI scope or status flow config.</span>
        ) : null}
      </div>
    </section>
  );
}
