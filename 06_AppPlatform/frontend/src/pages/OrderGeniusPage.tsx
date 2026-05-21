import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type DragEvent,
  type KeyboardEvent,
} from "react";

import { api } from "../api/client";
import type {
  CountryPaymentTerm,
  MaterialSkuMatrixRow,
  MaterialUploadPreview,
  MatrixResponse,
  OrderGeniusOptions,
  PublishBaselineResponse,
  QuantityCellUpdate,
} from "../types/orderGenius";

const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB
const MONTHS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

function formatOrderGeniusFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function OrderGeniusPage() {
  // ── Filter state ──────────────────────────────────────────────────
  const [countries, setCountries] = useState<CountryPaymentTerm[]>([]);
  const [selectedCountry, setSelectedCountry] = useState("");
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [brandFilter, setBrandFilter] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [powertrainFilter, setPowertrainFilter] = useState("");
  const [versionFilter, setVersionFilter] = useState("");
  const [colourFilter, setColourFilter] = useState("");
  const [materialSearch, setMaterialSearch] = useState("");

  const [options, setOptions] = useState<OrderGeniusOptions | null>(null);
  const [matrix, setMatrix] = useState<MatrixResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // ── Upload state ──────────────────────────────────────────────────
  const [showUpload, setShowUpload] = useState(false);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState("");
  const [uploadSessionId, setUploadSessionId] = useState("");
  const [uploadStatus, setUploadStatus] = useState("");
  const [uploadPreview, setUploadPreview] = useState<MaterialUploadPreview | null>(null);
  const [uploadDragActive, setUploadDragActive] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const [publishResult, setPublishResult] = useState<PublishBaselineResponse | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ── Quantity editing ──────────────────────────────────────────────
  const [editingCells, setEditingCells] = useState<Record<string, string>>({});
  const [savingCells, setSavingCells] = useState<Set<string>>(new Set());
  const [cellErrors, setCellErrors] = useState<Record<string, string>>({});

  // ── Load countries ────────────────────────────────────────────────
  useEffect(() => {
    api.getOrderGeniusCountries()
      .then((res) => {
        setCountries(res.items);
        if (res.items.length > 0 && !selectedCountry) {
          setSelectedCountry(res.items[0].countryCode);
        }
      })
      .catch(() => setError("Failed to load countries"));
  }, []);

  // ── Load options ──────────────────────────────────────────────────
  useEffect(() => {
    if (!selectedCountry) return;
    setLoading(true);
    setError("");
    api
      .getOrderGeniusOptions({
        country: selectedCountry,
        brand: brandFilter || undefined,
        model: modelFilter || undefined,
        powertrain: powertrainFilter || undefined,
        version: versionFilter || undefined,
        colour: colourFilter || undefined,
      })
      .then(setOptions)
      .catch((e: unknown) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [selectedCountry, brandFilter, modelFilter, powertrainFilter, versionFilter, colourFilter]);

  // ── Load matrix ───────────────────────────────────────────────────
  const loadMatrix = useCallback(() => {
    if (!selectedCountry) return;
    setLoading(true);
    setError("");
    api
      .getOrderGeniusMatrix({
        country: selectedCountry,
        year: selectedYear,
        brand: brandFilter || undefined,
        model: modelFilter || undefined,
        powertrain: powertrainFilter || undefined,
        version: versionFilter || undefined,
        colour: colourFilter || undefined,
        materialCodeSearch: materialSearch || undefined,
      })
      .then(setMatrix)
      .catch((e: unknown) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [
    selectedCountry, selectedYear, brandFilter, modelFilter,
    powertrainFilter, versionFilter, colourFilter, materialSearch,
  ]);

  useEffect(() => {
    loadMatrix();
  }, [loadMatrix]);

  // ── Upload handlers ───────────────────────────────────────────────

  function selectUploadFile(file: File): void {
    setUploadFile(file);
    setUploadSessionId("");
    setUploadStatus("");
    setUploadPreview(null);
    setPublishResult(null);
    setUploadProgress("");
    setError("");
  }

  const handleFileSelect = (e: ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (f) selectUploadFile(f);
  };

  const handleUploadDragState = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setUploadDragActive(event.type !== "dragleave");
  };

  const handleUploadDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setUploadDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (file) selectUploadFile(file);
  };

  const handleUploadDropzoneKeyboard = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      fileInputRef.current?.click();
    }
  };

  const handleUpload = async () => {
    if (!uploadFile) return;
    setUploadProgress("Initiating...");
    setUploadStatus("");
    setError("");
    try {
      const session = await api.initiateMaterialMasterUpload(
        uploadFile.name,
        uploadFile.size,
      );
      setUploadSessionId(session.uploadId);
      const totalChunks = session.totalChunks;
      const chunkSize = session.chunkSize || CHUNK_SIZE;

      for (let i = 0; i < totalChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, uploadFile.size);
        const blob = uploadFile.slice(start, end);
        setUploadProgress(
          `Uploading chunk ${i + 1}/${totalChunks} (${Math.round(
            (i / totalChunks) * 100,
          )}%)`,
        );
        await api.uploadMaterialMasterChunk(session.uploadId, i, blob);
      }

      setUploadProgress("Assembling file...");
      await api.completeMaterialMasterUpload(session.uploadId);

      setUploadProgress("Parsing...");
      const parseResult = await api.parseMaterialMasterUpload(session.uploadId);
      setUploadStatus(`Parsed: ${(parseResult as Record<string, unknown>).totalRows || 0} rows`);

      setUploadProgress("Loading preview...");
      const preview = await api.getMaterialMasterPreview(session.uploadId);
      setUploadPreview(preview);
      setUploadProgress("");
    } catch (err) {
      setError(getErrorMessage(err));
      setUploadProgress("");
    }
  };

  const handlePublish = async () => {
    if (!uploadSessionId) return;
    setPublishing(true);
    setError("");
    try {
      const result = await api.publishMaterialMaster(uploadSessionId);
      setPublishResult(result);
      setUploadStatus("Published!");
      loadMatrix();
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setPublishing(false);
    }
  };

  const clearUpload = () => {
    setUploadFile(null);
    setUploadSessionId("");
    setUploadStatus("");
    setUploadPreview(null);
    setPublishResult(null);
    setUploadProgress("");
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  // ── Quantity cell editing ─────────────────────────────────────────

  const cellKey = (materialCode: string, month: number) =>
    `${materialCode}_${month}`;

  const startEdit = (materialCode: string, month: number) => {
    setEditingCells((prev) => ({ ...prev, [cellKey(materialCode, month)]: "" }));
  };

  const handleCellChange = (materialCode: string, month: number, v: string) => {
    setEditingCells((prev) => ({
      ...prev,
      [cellKey(materialCode, month)]: v,
    }));
  };

  const handleCellSave = async (
    materialCode: string,
    month: number,
    currentVersion: number,
  ) => {
    const raw = editingCells[cellKey(materialCode, month)];
    if (raw === "") {
      setEditingCells((prev) => {
        const next = { ...prev };
        delete next[cellKey(materialCode, month)];
        return next;
      });
      return;
    }
    const qty = parseInt(raw, 10);
    if (isNaN(qty) || qty < 0) {
      setCellErrors((prev) => ({
        ...prev,
        [cellKey(materialCode, month)]: "Invalid number",
      }));
      return;
    }
    const key = cellKey(materialCode, month);
    setSavingCells((prev) => new Set(prev).add(key));
    setCellErrors((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });

    const payload: QuantityCellUpdate = {
      countryCode: selectedCountry,
      orderYear: selectedYear,
      orderMonth: month,
      materialCode,
      quantity: qty,
      rowVersion: currentVersion,
    };

    try {
      await api.updateQuantityCell(payload);
      setEditingCells((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      loadMatrix();
    } catch (err) {
      setCellErrors((prev) => ({ ...prev, [key]: getErrorMessage(err) }));
    } finally {
      setSavingCells((prev) => {
        const next = new Set(prev);
        next.delete(key);
        return next;
      });
    }
  };

  // ── Export ─────────────────────────────────────────────────────────

  const handleExport = async () => {
    try {
      const blob = await api.exportOrderGenius(selectedCountry, selectedYear);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `Order_Genius_${selectedCountry}-${selectedYear}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(`Export failed: ${getErrorMessage(err)}`);
    }
  };

  // ── Derived ────────────────────────────────────────────────────────

  const selectedPaymentTerm = useMemo(
    () =>
      countries.find((c) => c.countryCode === selectedCountry)
        ?.paymentTermCode ?? null,
    [countries, selectedCountry],
  );

  return (
    <section className="crud-shell">
      <header className="crud-hero">
        <h1>Order Genius</h1>
        <p>
          Country order matrix with FOB pricing, monthly quantity editing, and
          Excel export.
        </p>
      </header>

      {error ? (
        <div className="alert alert-error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}

      {/* ── Filter bar ─────────────────────────────────────────────── */}
      <div
        style={{
          display: "flex",
          gap: 12,
          flexWrap: "wrap",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <select
          value={selectedCountry}
          onChange={(e) => {
            setSelectedCountry(e.target.value);
            setBrandFilter("");
            setModelFilter("");
            setPowertrainFilter("");
            setVersionFilter("");
            setColourFilter("");
          }}
          style={{ minWidth: 100 }}
        >
          <option value="">-- Country --</option>
          {countries.map((c) => (
            <option key={c.countryCode} value={c.countryCode}>
              {c.countryName} ({c.paymentTermCode})
            </option>
          ))}
        </select>

        <select
          value={selectedYear}
          onChange={(e) => setSelectedYear(Number(e.target.value))}
          style={{ minWidth: 80 }}
        >
          {[selectedYear - 1, selectedYear, selectedYear + 1].map((y) => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>

        {selectedPaymentTerm ? (
          <span style={{ fontSize: 13, color: "#64748b" }}>
            Payment: {selectedPaymentTerm}
          </span>
        ) : null}

        {options?.brands ? (
          <select
            value={brandFilter}
            onChange={(e) => { setBrandFilter(e.target.value); setModelFilter(""); setPowertrainFilter(""); setVersionFilter(""); setColourFilter(""); }}
          >
            <option value="">All Brands</option>
            {options.brands.map((b) => (<option key={b} value={b}>{b}</option>))}
          </select>
        ) : null}

        {options?.models ? (
          <select value={modelFilter} onChange={(e) => { setModelFilter(e.target.value); setPowertrainFilter(""); setVersionFilter(""); setColourFilter(""); }}>
            <option value="">All Models</option>
            {options.models.map((m) => (<option key={m} value={m}>{m}</option>))}
          </select>
        ) : null}

        {options?.powertrains ? (
          <select value={powertrainFilter} onChange={(e) => { setPowertrainFilter(e.target.value); setVersionFilter(""); setColourFilter(""); }}>
            <option value="">All Powertrains</option>
            {options.powertrains.map((p) => (<option key={p} value={p}>{p}</option>))}
          </select>
        ) : null}

        {options?.versions ? (
          <select value={versionFilter} onChange={(e) => { setVersionFilter(e.target.value); setColourFilter(""); }}>
            <option value="">All Versions</option>
            {options.versions.map((v) => (<option key={v} value={v}>{v}</option>))}
          </select>
        ) : null}

        {options?.colours ? (
          <select value={colourFilter} onChange={(e) => setColourFilter(e.target.value)}>
            <option value="">All Colours</option>
            {options.colours.map((c) => (<option key={c} value={c}>{c}</option>))}
          </select>
        ) : null}

        <input
          type="text"
          placeholder="Material code..."
          value={materialSearch}
          onChange={(e) => setMaterialSearch(e.target.value)}
          style={{ minWidth: 140 }}
        />

        <button type="button" className="btn btn-sm btn-primary" onClick={loadMatrix}>
          Refresh
        </button>
        <button type="button" className="btn btn-sm btn-ghost" onClick={handleExport}
                disabled={!matrix || matrix.totalRows === 0}>
          Export XLSX
        </button>
        <button type="button" className="btn btn-sm btn-ghost"
                onClick={() => setShowUpload(!showUpload)}>
          {showUpload ? "Hide Upload" : "Upload Material Master"}
        </button>
      </div>

      {/* ── Upload panel ───────────────────────────────────────────── */}
      {showUpload ? (
        <div className="card crud-card" style={{ padding: 16, marginBottom: 16 }}>
          <h3 style={{ marginTop: 0 }}>Material Master Upload</h3>
          <input
            ref={fileInputRef}
            type="file"
            accept=".xlsx,.xlsm,.xls"
            onChange={handleFileSelect}
            className="monthly-update-file-input"
          />
          <div
            className={`monthly-update-dropzone${uploadDragActive ? " is-dragging" : ""}${uploadFile ? " has-file" : ""}`}
            role="button"
            tabIndex={0}
            onClick={() => fileInputRef.current?.click()}
            onKeyDown={handleUploadDropzoneKeyboard}
            onDragEnter={handleUploadDragState}
            onDragOver={handleUploadDragState}
            onDragLeave={handleUploadDragState}
            onDrop={handleUploadDrop}
          >
            <strong>
              {uploadFile ? uploadFile.name : "拖拽 Material Master Excel 到这里，或点击选择文件"}
            </strong>
            <span>
              {uploadFile
                ? `${formatOrderGeniusFileSize(uploadFile.size)} · 上传后会分片解析并生成发布预览。`
                : "支持 .xlsx / .xlsm / .xls；适用于 OMODA&JAECOO Order Material Codes 文件。"}
            </span>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 12 }}>
            <button
              type="button"
              className="btn btn-sm btn-primary"
              disabled={!uploadFile || !!uploadProgress}
              onClick={handleUpload}
            >
              Upload &amp; Parse
            </button>
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              onClick={clearUpload}
            >
              Clear
            </button>
          </div>
          {uploadProgress ? (
            <div style={{ marginTop: 8, fontSize: 13, color: "#64748b" }}>
              {uploadProgress}
            </div>
          ) : null}
          {uploadStatus ? (
            <div style={{ marginTop: 4, fontSize: 13, color: "#16a34a" }}>
              {uploadStatus}
            </div>
          ) : null}

          {/* Preview */}
          {uploadPreview ? (
            <div style={{ marginTop: 12 }}>
              <div style={{ display: "flex", gap: 16, marginBottom: 8, fontSize: 13 }}>
                <span>Total rows: <strong>{uploadPreview.totalRows}</strong></span>
                <span>New: <strong style={{ color: "#16a34a" }}>{uploadPreview.newSkus}</strong></span>
                <span>Existing: <strong style={{ color: "#d97706" }}>{uploadPreview.existingSkus}</strong></span>
              </div>
              {uploadPreview.warnings.length > 0 ? (
                <div style={{ maxHeight: 120, overflow: "auto", fontSize: 12, color: "#d97706", marginBottom: 8 }}>
                  {uploadPreview.warnings.slice(0, 20).map((w, i) => (
                    <div key={i}>{w}</div>
                  ))}
                </div>
              ) : null}
              <div style={{ overflowX: "auto", maxHeight: 300, marginBottom: 8 }}>
                <table className="data-table" style={{ fontSize: 12 }}>
                  <thead>
                    <tr>
                      <th>#</th><th>Sheet</th><th>Brand</th><th>Model</th>
                      <th>Version</th><th>Colour</th><th>Code</th>
                      <th>Material</th><th>FOB</th><th>Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {uploadPreview.rows.slice(0, 50).map((r) => (
                      <tr key={r.rowIndex}>
                        <td>{r.rowIndex}</td><td>{r.sheetName}</td>
                        <td>{r.brand}</td><td>{r.modelName}</td>
                        <td>{r.version}</td><td>{r.exteriorColorName}</td>
                        <td>{r.exteriorColorCode}</td>
                        <td>{r.materialCode}</td>
                        <td>{r.baseFobEur ?? "-"}</td>
                        <td>{r.exteriorColorType}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <button
                type="button"
                className="btn btn-sm btn-primary"
                disabled={publishing || !uploadSessionId}
                onClick={handlePublish}
              >
                {publishing ? "Publishing..." : "Publish Baseline"}
              </button>
              {publishResult ? (
                <span style={{ marginLeft: 12, fontSize: 13, color: "#16a34a" }}>
                  Published {publishResult.baselineName} — {publishResult.skuCount} SKUs,{" "}
                  {publishResult.fobCount} FOBs
                </span>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}

      {/* ── Matrix grid ────────────────────────────────────────────── */}
      {loading ? (
        <div style={{ padding: 32, textAlign: "center", color: "#64748b" }}>
          Loading...
        </div>
      ) : matrix && matrix.totalRows > 0 ? (
        <div style={{ overflowX: "auto", maxHeight: "70vh" }}>
          <table className="data-table" style={{ fontSize: 12 }}>
            <thead>
              <tr>
                <th style={{ position: "sticky", left: 0, background: "#f1f5f9", zIndex: 2 }}>
                  Model
                </th>
                <th style={{ position: "sticky", left: 80, background: "#f1f5f9", zIndex: 2 }}>
                  Version
                </th>
                <th style={{ position: "sticky", left: 180, background: "#f1f5f9", zIndex: 2 }}>
                  Colour
                </th>
                <th style={{ position: "sticky", left: 280, background: "#f1f5f9", zIndex: 2 }}>
                  Material Code
                </th>
                <th style={{ position: "sticky", left: 410, background: "#f1f5f9", zIndex: 2 }}>
                  FOB(EUR)
                </th>
                {MONTHS.map((m) => (<th key={m}>{m}</th>))}
                <th>TTL</th>
              </tr>
            </thead>
            <tbody>
              {matrix.rows.map((row) => (
                <OrderGeniusRow
                  key={row.materialCode}
                  row={row}
                  editingCells={editingCells}
                  savingCells={savingCells}
                  cellErrors={cellErrors}
                  onStartEdit={startEdit}
                  onCellChange={handleCellChange}
                  onCellSave={handleCellSave}
                />
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={{ padding: 32, textAlign: "center", color: "#64748b" }}>
          {selectedCountry
            ? "No data. Upload a Material Master file to get started."
            : "Select a country to view the order matrix."}
        </div>
      )}
    </section>
  );
}

// ── Row component ──────────────────────────────────────────────────────

function OrderGeniusRow({
  row,
  editingCells,
  savingCells,
  cellErrors,
  onStartEdit,
  onCellChange,
  onCellSave,
}: {
  row: MaterialSkuMatrixRow;
  editingCells: Record<string, string>;
  savingCells: Set<string>;
  cellErrors: Record<string, string>;
  onStartEdit: (materialCode: string, month: number) => void;
  onCellChange: (materialCode: string, month: number, value: string) => void;
  onCellSave: (materialCode: string, month: number, version: number) => void;
}) {
  const isHistorical = row.lifecycleStatus === "historical";
  const textStyle: React.CSSProperties = isHistorical
    ? { textDecoration: "line-through", color: "#9ca3af" }
    : {};

  return (
    <tr style={isHistorical ? { backgroundColor: "#f9fafb" } : undefined}>
      <td style={{ ...textStyle, position: "sticky", left: 0, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap" }}>
        {row.modelName}
      </td>
      <td style={{ ...textStyle, position: "sticky", left: 80, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap" }}>
        {row.version}
      </td>
      <td style={{ ...textStyle, position: "sticky", left: 180, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap" }}>
        {row.colour}
      </td>
      <td style={{ ...textStyle, position: "sticky", left: 280, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap", fontFamily: "monospace" }}>
        {row.materialCode}
      </td>
      <td style={{ ...textStyle, position: "sticky", left: 410, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap", textAlign: "right" }}>
        {row.fobEur != null ? row.fobEur.toLocaleString() : "-"}
      </td>
      {MONTHS.map((_, idx) => {
        const month = idx + 1;
        const key = `${row.materialCode}_${month}`;
        const monthData = row.months[String(month)];
        const qty = monthData?.quantity ?? 0;
        const isEditing = key in editingCells;
        const isSaving = savingCells.has(key);
        const errMsg = cellErrors[key];

        return (
          <td
            key={month}
            style={{
              textAlign: "center",
              minWidth: 50,
              cursor: row.editable ? "pointer" : "default",
            }}
            onClick={() => {
              if (row.editable && !isEditing) {
                onStartEdit(row.materialCode, month);
              }
            }}
          >
            {isEditing ? (
              <input
                type="number"
                min={0}
                style={{
                  width: 48,
                  textAlign: "center",
                  border: errMsg ? "1px solid #dc2626" : "1px solid #3b82f6",
                  borderRadius: 4,
                }}
                defaultValue={qty}
                autoFocus
                onBlur={(e) => {
                  onCellChange(row.materialCode, month, e.target.value);
                  onCellSave(row.materialCode, month, 1);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    onCellSave(row.materialCode, month, 1);
                  }
                  if (e.key === "Escape") {
                    onCellChange(row.materialCode, month, "");
                    onCellSave(row.materialCode, month, 1);
                  }
                }}
                onChange={(e) =>
                  onCellChange(row.materialCode, month, e.target.value)
                }
              />
            ) : (
              <span
                style={{
                  color: errMsg ? "#dc2626" : isSaving ? "#3b82f6" : undefined,
                }}
                title={errMsg}
              >
                {qty}
              </span>
            )}
          </td>
        );
      })}
      <td style={{ fontWeight: 600, textAlign: "center" }}>{row.ttl}</td>
    </tr>
  );
}
