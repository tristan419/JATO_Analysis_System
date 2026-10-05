import { useState, type FormEvent } from "react";
import { CommandSelect, type CommandSelectOption } from "./CommandSelect";
import type { AllocationStatus, LogisticsStatus, PiVehicleUnit, UpdateVehiclePayload } from "../types/orderGeniusVehicle";

type TextField = "vin" | "productionDate" | "etd" | "eta" | "actualDepartureDate" | "actualArrivalDate" | "readyForPickupDate" | "shipName" | "dealerCode" | "dealerName" | "customerRef" | "remark";
const FIELDS: Array<{ key: TextField; label: string; date?: boolean }> = [
  { key: "vin", label: "VIN" }, { key: "productionDate", label: "Production", date: true },
  { key: "etd", label: "ETD", date: true }, { key: "eta", label: "ETA", date: true },
  { key: "actualDepartureDate", label: "Actual departure", date: true },
  { key: "actualArrivalDate", label: "Actual arrival", date: true },
  { key: "readyForPickupDate", label: "Ready for pickup", date: true },
  { key: "shipName", label: "Ship" }, { key: "dealerCode", label: "Dealer code" },
  { key: "dealerName", label: "Dealer name" }, { key: "customerRef", label: "Customer ref" },
  { key: "remark", label: "Note / 备注" },
];
interface Props {
  vehicles: PiVehicleUnit[];
  readOnly: boolean;
  busy: boolean;
  allocationOptions: Array<CommandSelectOption<AllocationStatus>>;
  logisticsOptions: Array<CommandSelectOption<LogisticsStatus>>;
  onSave: (patch: UpdateVehiclePayload) => Promise<void>;
}
function commonValue(vehicles: PiVehicleUnit[], key: keyof UpdateVehiclePayload): string | undefined {
  const values = vehicles.map((vehicle) => key === "rowVersion" ? "" : String(vehicle[key] ?? ""));
  return values.every((value) => value === values[0]) ? values[0] ?? "" : undefined;
}
export function VehicleAllocationEditor({ vehicles, readOnly, busy, allocationOptions, logisticsOptions, onSave }: Props) {
  const [patch, setPatch] = useState<UpdateVehiclePayload>({});
  const [message, setMessage] = useState("");
  function keep(key: keyof UpdateVehiclePayload): void {
    setPatch((current) => { const next = { ...current }; delete next[key]; return next; });
  }
  function value(key: keyof UpdateVehiclePayload): string {
    return key in patch ? String(patch[key] ?? "") : commonValue(vehicles, key) ?? "";
  }
  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (busy || readOnly || !vehicles.length) return;
    if (!Object.keys(patch).length) { setMessage("Change a field first / 请先修改字段"); return; }
    if ([patch.freightEur, patch.insuranceEur].some((cost) => cost != null && (!Number.isFinite(cost) || cost < 0 || cost >= 10 ** 12 || Math.abs(cost * 100 - Math.round(cost * 100)) > 0.0001))) {
      setMessage("Use non-negative EUR amounts with up to 2 decimals / EUR 金额须非负且最多两位小数"); return;
    }
    setMessage("");
    await onSave(patch);
  }
  return <form id="vehicle-edit-form" className="va-editor" onSubmit={(event) => void submit(event)}>
    <p>{vehicles.length === 1 ? vehicles[0].carCode : `${vehicles.length} selected vehicles / 勾选车辆`}</p>
    <p className="va-edit-help">Only changed fields are saved. Blank input keeps the original; use Clear explicitly. / 仅保存修改字段；空白保留原值，清空须明确操作。</p>
    {message ? <p role="alert">{message}</p> : null}
    <fieldset disabled={readOnly || busy}><legend>Status / 状态</legend>
      <label>Allocation<CommandSelect<AllocationStatus> disabled={readOnly || busy} value={allocationOptions.find((option) => option.value === value("allocationStatus"))?.value ?? ""} options={allocationOptions} placeholder="Multiple values / Keep" onChange={(next) => next ? setPatch((current) => ({ ...current, allocationStatus: next })) : keep("allocationStatus")} />{"allocationStatus" in patch ? <button type="button" className="btn-secondary" onClick={() => keep("allocationStatus")}>Keep original</button> : null}</label>
      <label>Logistics<CommandSelect<LogisticsStatus> disabled={readOnly || busy} value={logisticsOptions.find((option) => option.value === value("logisticsStatus"))?.value ?? ""} options={logisticsOptions} placeholder="Multiple values / Keep" onChange={(next) => next ? setPatch((current) => ({ ...current, logisticsStatus: next })) : keep("logisticsStatus")} />{"logisticsStatus" in patch ? <button type="button" className="btn-secondary" onClick={() => keep("logisticsStatus")}>Keep original</button> : null}</label>
    </fieldset>
    <fieldset disabled={readOnly || busy}><legend>Vehicle & dates / 车辆与日期</legend>
      {FIELDS.filter((field) => field.key !== "vin" || vehicles.length === 1).map(({ key, label, date }) =>
        <label key={key} className={key === "remark" ? "va-wide" : ""}>{label}
          {key === "remark" ? <textarea aria-label={label} readOnly={readOnly} value={value(key)} placeholder={commonValue(vehicles, key) === undefined ? "Multiple values / Keep" : "Keep / 保留"} onChange={(event) => event.target.value ? setPatch((current) => ({ ...current, [key]: event.target.value })) : keep(key)} /> :
            <input aria-label={label} type={date ? "date" : "text"} readOnly={readOnly} value={value(key)} placeholder={commonValue(vehicles, key) === undefined ? "Multiple values / Keep" : "Keep / 保留"} onChange={(event) => event.target.value ? setPatch((current) => ({ ...current, [key]: key === "vin" ? event.target.value.toUpperCase() : event.target.value })) : keep(key)} />}
          <span className="va-edit-actions">
            {commonValue(vehicles, key) === undefined && !(key in patch) ? <small>Multiple values / Keep</small> : null}
            {key !== "vin" ? <button type="button" className="btn-secondary" onClick={() => setPatch((current) => ({ ...current, [key]: null }))}>Clear</button> : <small>Clear VIN using import preview / 清 VIN 请用导入预览</small>}
            {key in patch ? <button type="button" className="btn-secondary" onClick={() => keep(key)}>Keep original</button> : null}
          </span>
        </label>)}
    </fieldset>
    <fieldset disabled={readOnly || busy}><legend>Costs / 运保费 (EUR)</legend>
      {(["freightEur", "insuranceEur"] as const).map((key) => <label key={key}>{key === "freightEur" ? "Freight / 运费 (EUR)" : "Insurance / 保费 (EUR)"}
        <input aria-label={key === "freightEur" ? "Freight / 运费 (EUR)" : "Insurance / 保费 (EUR)"} type="number" min="0" step="0.01" readOnly={readOnly} value={value(key)} placeholder={commonValue(vehicles, key) === undefined ? "Multiple values / Keep" : "Keep / 保留"} onChange={(event) => event.target.value === "" ? keep(key) : setPatch((current) => ({ ...current, [key]: Number(event.target.value) }))} />
        <span className="va-edit-actions"><button type="button" className="btn-secondary" onClick={() => setPatch((current) => ({ ...current, [key]: null }))}>Clear</button>{key in patch ? <button type="button" className="btn-secondary" onClick={() => keep(key)}>Keep original</button> : null}</span>
      </label>)}
    </fieldset>
  </form>;
}
