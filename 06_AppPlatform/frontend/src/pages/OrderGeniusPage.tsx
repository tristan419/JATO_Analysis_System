import {
  Fragment,
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
import { useAuth } from "../contexts/AuthContext";
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
  const { user } = useAuth();
  // ── Filter state ──────────────────────────────────────────────────
  const [countries, setCountries] = useState<CountryPaymentTerm[]>([]);
  const [selectedCountry, setSelectedCountry] = useState("");
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [selectedMonth, setSelectedMonth] = useState<number | null>(null); // null = all months
  const [visibleColumns, setVisibleColumns] = useState({
    months: true, amount: true, ttlQty: true, ttlAmount: true, fob: true, materialCode: true, remark: true,
  });
  const [brandFilter, setBrandFilter] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [powertrainFilter, setPowertrainFilter] = useState("");
  const [versionFilter, setVersionFilter] = useState("");
  const [colourFilter, setColourFilter] = useState("");
  const [materialSearch, setMaterialSearch] = useState("");
  const [groupByProduct, setGroupByProduct] = useState(true);
  const [showPtAdmin, setShowPtAdmin] = useState(false);

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
          const preferred = user?.primaryCountry
            && res.items.some((item) => item.countryCode === user.primaryCountry)
            ? user.primaryCountry
            : null;
          const anonymousDefault = res.items.find((item) => item.countryCode === "SE");
          setSelectedCountry(preferred ?? anonymousDefault?.countryCode ?? res.items[0].countryCode);
        }
      })
      .catch(() => setError("Failed to load countries"));
  }, [selectedCountry, user?.primaryCountry]);

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
      const result = await api.updateQuantityCell(payload);
      // Patch local matrix state with new quantity + rowVersion (no full reload)
      setMatrix((prev) => {
        if (!prev) return prev;
        const rows = prev.rows.map((r) => {
          if (r.materialCode !== materialCode) return r;
          const months = { ...r.months };
          months[String(month)] = {
            quantity: qty,
            isEditable: true,
            rowVersion: result.rowVersion,
          };
          const newTtl = Object.values(months).reduce(
            (sum, m) => sum + m.quantity, 0,
          );
          return { ...r, months, ttl: newTtl };
        });
        return { ...prev, rows };
      });
      setEditingCells((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
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

        <select
          value={selectedMonth ?? ""}
          onChange={(e) => setSelectedMonth(e.target.value ? Number(e.target.value) : null)}
          style={{ minWidth: 100 }}
        >
          <option value="">All months</option>
          {MONTHS.map((m, i) => (
            <option key={m} value={i + 1}>{m}</option>
          ))}
        </select>

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
          list="material-suggestions"
          placeholder="Material code..."
          value={materialSearch}
          onChange={(e) => setMaterialSearch(e.target.value)}
          style={{ minWidth: 160 }}
        />
        <datalist id="material-suggestions">
          {matrix?.rows.map((r) => (
            <option key={r.materialCode} value={r.materialCode}>
              {r.remark ? `${r.materialCode} (${r.remark})` : r.materialCode}
            </option>
          ))}
        </datalist>

        <label style={{ cursor: "pointer", fontSize: 12, color: "#475569", display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={groupByProduct} onChange={(e) => setGroupByProduct(e.target.checked)} />
          Group by product
        </label>

        <button type="button" className="btn btn-sm btn-ghost"
                onClick={() => setShowPtAdmin(!showPtAdmin)}
                style={showPtAdmin ? { background: "#0f766e", color: "#fff" } : undefined}>
          {showPtAdmin ? "Hide PT Admin" : "Payment Terms"}
        </button>

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

      {/* ── Column visibility ───────────────────────────────────────── */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 8, fontSize: 12 }}>
        {(["months","amount","ttlQty","ttlAmount","fob","materialCode","remark"] as const).map((col) => (
          <label key={col} style={{ cursor: "pointer", color: "#475569" }}>
            <input
              type="checkbox"
              checked={visibleColumns[col]}
              onChange={() => setVisibleColumns((v) => ({ ...v, [col]: !v[col] }))}
              style={{ marginRight: 4 }}
            />
            {{ months: "Months", amount: "Amount", ttlQty: "TTL Qty", ttlAmount: "TTL Amt", fob: "FOB", materialCode: "Material", remark: "Remark" }[col]}
          </label>
        ))}
      </div>

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
                <th style={{ position: "sticky", left: 0, background: "#f1f5f9", zIndex: 2 }}>Model</th>
                <th style={{ position: "sticky", left: 80, background: "#f1f5f9", zIndex: 2 }}>Version</th>
                <th style={{ position: "sticky", left: 180, background: "#f1f5f9", zIndex: 2 }}>Colour</th>
                {visibleColumns.materialCode && (
                  <th style={{ position: "sticky", left: 280, background: "#f1f5f9", zIndex: 2 }}>Material Code</th>
                )}
                {visibleColumns.fob && (
                  <th style={{ position: "sticky", left: 410, background: "#f1f5f9", zIndex: 2 }}>FOB (EUR)</th>
                )}
                {visibleColumns.months && MONTHS
                  .filter((_, i) => selectedMonth == null || i + 1 === selectedMonth)
                  .map((m) => (<th key={m}>{m}</th>))
                }
                {visibleColumns.amount && MONTHS
                  .filter((_, i) => selectedMonth == null || i + 1 === selectedMonth)
                  .map((m) => (<th key={`amt-${m}`} style={{ color: "#0f766e" }}>{m} €</th>))
                }
                {visibleColumns.ttlQty && <th>TTL</th>}
                {visibleColumns.ttlAmount && <th style={{ color: "#0f766e" }}>TTL €</th>}
                {visibleColumns.remark && <th style={{ minWidth: 160 }}>Remark</th>}
              </tr>
            </thead>
            <OrderGeniusBody
              rows={matrix.rows}
              groupByProduct={groupByProduct}
              editingCells={editingCells}
              savingCells={savingCells}
              cellErrors={cellErrors}
              onStartEdit={startEdit}
              onCellChange={handleCellChange}
              onCellSave={handleCellSave}
              visibleColumns={visibleColumns}
              selectedMonth={selectedMonth}
            />
          </table>
        </div>
      ) : (
        <div style={{ padding: 32, textAlign: "center", color: "#64748b" }}>
          {selectedCountry
            ? "No data. Upload a Material Master file to get started."
            : "Select a country to view the order matrix."}
        </div>
      )}

      {/* ── Payment Terms Admin ────────────────────────────────────── */}
      {showPtAdmin && <PaymentTermAdminPanel />}
    </section>
  );
}

// ── Row component ──────────────────────────────────────────────────────

const VISIBLE_COLS_DEFAULTS = { months: true, amount: true, ttlQty: true, ttlAmount: true, fob: true, materialCode: true, remark: true };

function OrderGeniusRow({
  row, editingCells, savingCells, cellErrors,
  onStartEdit, onCellChange, onCellSave,
  visibleColumns, selectedMonth,
}: {
  row: MaterialSkuMatrixRow;
  editingCells: Record<string, string>;
  savingCells: Set<string>;
  cellErrors: Record<string, string>;
  onStartEdit: (materialCode: string, month: number) => void;
  onCellChange: (materialCode: string, month: number, value: string) => void;
  onCellSave: (materialCode: string, month: number, version: number) => void;
  visibleColumns: typeof VISIBLE_COLS_DEFAULTS;
  selectedMonth: number | null;
}) {
  const isHistorical = row.lifecycleStatus === "historical";
  const fob = row.fobEur ?? 0;
  const activeMonths = MONTHS.map((_, i) => i + 1).filter((m) => selectedMonth == null || m === selectedMonth);
  const monthTotal = activeMonths.reduce((sum, m) => sum + (row.months[String(m)]?.quantity ?? 0), 0);
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
        {row.colour}{row.colourCode ? <span style={{ color: "#94a3b8", fontSize: 10, marginLeft: 4 }}>{row.colourCode}</span> : null}
      </td>
      {visibleColumns.materialCode && (
        <td style={{ ...textStyle, position: "sticky", left: 280, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap", fontFamily: "monospace" }}>
          <div>{row.materialCode}</div>
          {(row.effectiveFrom || row.effectiveTo) ? (
            <div style={{ fontSize: 9, color: isHistorical ? "#9ca3af" : "#64748b" }}>
              {row.effectiveFrom ?? "?"} → {row.effectiveTo || "至今"}
            </div>
          ) : row.lifecycleStatus !== "active" ? (
            <div style={{ fontSize: 9, color: "#9ca3af" }}>Historical</div>
          ) : null}
        </td>
      )}
      {visibleColumns.fob && (
        <td style={{ ...textStyle, position: "sticky", left: 410, background: isHistorical ? "#f9fafb" : "#fff", whiteSpace: "nowrap", textAlign: "right" }}>
          {row.fobEur != null ? row.fobEur.toLocaleString() : "-"}
        </td>
      )}
      {visibleColumns.months && activeMonths.map((month) => {
        const key = `${row.materialCode}_${month}`;
        const monthData = row.months[String(month)];
        const qty = monthData?.quantity ?? 0;
        const isEditing = key in editingCells;
        const isSaving = savingCells.has(key);
        const errMsg = cellErrors[key];
        return (
          <td key={month} style={{ textAlign: "center", minWidth: 50, cursor: row.editable ? "pointer" : "default" }}
            onClick={() => { if (row.editable && !isEditing) onStartEdit(row.materialCode, month); }}
          >
            {isEditing ? (
              <input type="number" min={0} style={{ width: 48, textAlign: "center", border: errMsg ? "1px solid #dc2626" : "1px solid #3b82f6", borderRadius: 4 }}
                defaultValue={qty} autoFocus
                onBlur={(e) => { onCellChange(row.materialCode, month, e.target.value); onCellSave(row.materialCode, month, monthData?.rowVersion ?? 1); }}
                onKeyDown={(e) => { if (e.key === "Enter") onCellSave(row.materialCode, month, monthData?.rowVersion ?? 1); if (e.key === "Escape") { onCellChange(row.materialCode, month, ""); onCellSave(row.materialCode, month, monthData?.rowVersion ?? 1); } }}
                onChange={(e) => onCellChange(row.materialCode, month, e.target.value)}
              />
            ) : <span style={{ color: errMsg ? "#dc2626" : isSaving ? "#3b82f6" : undefined }} title={errMsg}>{qty}</span>}
          </td>
        );
      })}
      {visibleColumns.amount && activeMonths.map((month) => {
        const qty = row.months[String(month)]?.quantity ?? 0;
        return <td key={`amt-${month}`} style={{ textAlign: "right", color: "#0f766e", ...textStyle }}>{(qty * fob).toLocaleString()}</td>;
      })}
      {visibleColumns.ttlQty && <td style={{ fontWeight: 700, textAlign: "center", ...textStyle }}>{monthTotal || "-"}</td>}
      {visibleColumns.ttlAmount && <td style={{ fontWeight: 700, textAlign: "right", color: "#0f766e", ...textStyle }}>{(monthTotal * fob).toLocaleString()}</td>}
      {visibleColumns.remark && (
        <td style={{ ...textStyle, fontSize: 11, color: "#64748b", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis" }}>{row.remark || ""}</td>
      )}
    </tr>
  );
}

// ── Powertrain colour map ────────────────────────────────────────────────

// Powertrain family colors — must match powertrain_normalizer.py POWERTRAIN_COLORS
const PT_COLORS: Record<string, string> = {
  EV: "#16a34a", BEV: "#16a34a", HEV: "#d97706", PHEV: "#2563eb", SHS: "#2563eb",
  MHEV: "#ca8a04", ICE: "#4b5563", LPG: "#6b7280", REEV: "#0d9488", FCV: "#0891b2",
};
function ptColor(pt: string | null): string { return PT_COLORS[pt ?? ""] ?? "#9ca3af"; }

// ── Body component with optional product grouping ──────────────────────

function OrderGeniusBody({
  rows, groupByProduct, editingCells, savingCells, cellErrors,
  onStartEdit, onCellChange, onCellSave, visibleColumns, selectedMonth,
}: {
  rows: MaterialSkuMatrixRow[];
  groupByProduct: boolean;
  editingCells: Record<string, string>;
  savingCells: Set<string>;
  cellErrors: Record<string, string>;
  onStartEdit: (code: string, m: number) => void;
  onCellChange: (code: string, m: number, v: string) => void;
  onCellSave: (code: string, m: number, ver: number) => void;
  visibleColumns: typeof VISIBLE_COLS_DEFAULTS;
  selectedMonth: number | null;
}) {
  if (!groupByProduct) {
    return (
      <tbody>
        {rows.map((r) => (
          <OrderGeniusRow key={r.materialCode} row={r}
            editingCells={editingCells} savingCells={savingCells} cellErrors={cellErrors}
            onStartEdit={onStartEdit} onCellChange={onCellChange} onCellSave={onCellSave}
            visibleColumns={visibleColumns} selectedMonth={selectedMonth} />
        ))}
      </tbody>
    );
  }

  // Group by brand+model+version+powertrain
  const key = (r: MaterialSkuMatrixRow) => `${r.brand}|${r.modelName}|${r.version}|${r.powertrain ?? ""}`;
  const groups = new Map<string, MaterialSkuMatrixRow[]>();
  for (const r of rows) { const k = key(r); if (!groups.has(k)) groups.set(k, []); groups.get(k)!.push(r); }
  const sorted = [...groups.entries()].sort(([, a], [, b]) =>
    (a.some((r) => r.lifecycleStatus === "active") ? 0 : 1) - (b.some((r) => r.lifecycleStatus === "active") ? 0 : 1)
  );

  const groupTtl = (grp: MaterialSkuMatrixRow[]) => {
    const activeMonths = MONTHS.map((_, i) => i + 1).filter((m) => selectedMonth == null || m === selectedMonth);
    return activeMonths.reduce((s, m) => s + grp.reduce((sum, r) => sum + (r.months[String(m)]?.quantity ?? 0), 0), 0);
  };

  return (
    <tbody>
      {sorted.map(([groupKey, grp]) => {
        const first = grp[0];
        const color = ptColor(first.powertrain);
        const gTtl = groupTtl(grp);
        const gapRows = grp.sort((a, b) => {
          if (a.lifecycleStatus !== b.lifecycleStatus) return a.lifecycleStatus === "active" ? -1 : 1;
          return a.colour.localeCompare(b.colour);
        });
        return (
          <Fragment key={groupKey}>
            <tr style={{ backgroundColor: `${color}15`, borderTop: `2px solid ${color}` }}>
              <td colSpan={3} style={{ padding: "6px 8px", fontWeight: 700, color }}>
                {first.brand} {first.modelName} {first.version}
                <span style={{ fontWeight: 400, color: "#64748b", marginLeft: 8 }}>
                  {first.powertrain} · {grp.length} colours · {gTtl.toLocaleString()} units
                </span>
              </td>
              <td colSpan={16} style={{ padding: "6px 8px" }}>
                {first.remark ? <span style={{ fontSize: 11, color: "#64748b" }}>📝 {first.remark}</span> : null}
              </td>
            </tr>
            {gapRows.map((r) => (
              <OrderGeniusRow key={r.materialCode} row={r}
                editingCells={editingCells} savingCells={savingCells} cellErrors={cellErrors}
                onStartEdit={onStartEdit} onCellChange={onCellChange} onCellSave={onCellSave}
                visibleColumns={visibleColumns} selectedMonth={selectedMonth} />
            ))}
          </Fragment>
        );
      })}
    </tbody>
  );
}

// ── Payment Terms Admin Panel ──────────────────────────────────────────

function PaymentTermAdminPanel() {
  const [pts, setPts] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<Record<string, string>>({});
  const [impact, setImpact] = useState<Record<string, any> | null>(null);
  const [confirmMsg, setConfirmMsg] = useState("");

  const authHeaders = (): Record<string, string> => {
    const t = localStorage.getItem("jato_auth_token");
    return t ? { "X-Auth-Token": t } : {};
  };

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/v1/order-genius/payment-terms/countries", { headers: authHeaders() });
      if (res.ok) setPts((await res.json()).items || []);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const startEdit = (row: any) => {
    setEditingId(row.id);
    setEditForm({ paymentTermCode: row.paymentTermCode, validFrom: row.validFrom || "", validTo: row.validTo || "", remark: row.remark || "" });
    setImpact(null);
    setConfirmMsg("");
  };

  const cancelEdit = () => { setEditingId(null); setImpact(null); setConfirmMsg(""); };

  const saveEdit = async (row: any) => {
    const isCorrect = row.validFrom && row.validFrom < "2026-07";
    if (isCorrect && !confirmMsg) {
      try {
        const params = new URLSearchParams({ country: row.countryCode, oldPaymentTerm: row.paymentTermCode, newPaymentTerm: editForm.paymentTermCode || row.paymentTermCode, validFrom: editForm.validFrom || row.validFrom || "", validTo: editForm.validTo || row.validTo || "" });
        const res = await fetch("/v1/order-genius/payment-terms/countries/impact?" + params, { headers: authHeaders() });
        if (res.ok) {
          const imp = await res.json();
          setImpact(imp);
          if (imp.fobRows > 0 || imp.orderMonths > 0) {
            setConfirmMsg(imp.message || "This change affects existing data. Confirm?");
            return;
          }
        }
      } catch { /* */ }
    }
    try {
      const res = await fetch("/v1/order-genius/payment-terms/countries/" + row.id, {
        method: "PATCH", headers: { ...authHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify({ ...editForm, correction: isCorrect }),
      });
      if (!res.ok) throw new Error(await res.text());
      cancelEdit();
      load();
    } catch (e) { alert(getErrorMessage(e)); }
  };

  const confirmSave = () => { setConfirmMsg(""); const row = pts.find((r: any) => r.id === editingId); if (row) saveEdit(row); };

  if (loading) return <div style={{ padding: 16, color: "#64748b" }}>Loading payment terms...</div>;

  return (
    <div className="card crud-card" style={{ padding: 16, marginTop: 16 }}>
      <h3 style={{ margin: "0 0 12px" }}>Payment Terms Admin</h3>
      <p style={{ fontSize: 12, color: "#64748b", marginBottom: 12 }}>
        Modifications to historical periods show impact but do NOT recalculate existing order snapshots.
      </p>
      {confirmMsg ? (
        <div style={{ background: "#fef3c7", border: "1px solid #f59e0b", padding: 12, marginBottom: 12, borderRadius: 6 }}>
          <strong>⚠️ Impact Warning</strong>
          <p style={{ fontSize: 13, margin: "4px 0" }}>{confirmMsg}</p>
          {impact ? <p style={{ fontSize: 12, color: "#64748b" }}>FOB rows: {impact.fobRows} · Order months: {impact.orderMonths}</p> : null}
          <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
            <button className="btn btn-sm btn-primary" onClick={confirmSave}>Confirm & Save</button>
            <button className="btn btn-sm btn-ghost" onClick={cancelEdit}>Cancel</button>
          </div>
        </div>
      ) : null}
      <div style={{ overflowX: "auto" }}>
        <table className="data-table" style={{ fontSize: 12 }}>
          <thead>
            <tr>
              <th>Country</th><th>Term</th><th>Valid From</th><th>Valid To</th><th>Status</th><th>Remark</th><th></th>
            </tr>
          </thead>
          <tbody>
            {pts.map((row: any) => {
              const isEditing = editingId === row.id;
              return (
                <tr key={row.id} style={!row.isActive ? { opacity: 0.6 } : undefined}>
                  <td>{row.countryCode} {row.countryName}</td>
                  <td>
                    {isEditing ? (
                      <select value={editForm.paymentTermCode} onChange={(e) => setEditForm({ ...editForm, paymentTermCode: e.target.value })} style={{ width: 80 }}>
                        {["TT","LC60","LC90","LC120"].map((pt) => <option key={pt} value={pt}>{pt}</option>)}
                      </select>
                    ) : row.paymentTermCode}
                  </td>
                  <td>
                    {isEditing ? (
                      <input type="text" value={editForm.validFrom} onChange={(e) => setEditForm({ ...editForm, validFrom: e.target.value })} style={{ width: 80 }} placeholder="YYYY-MM" />
                    ) : (row.validFrom || "—")}
                  </td>
                  <td>
                    {isEditing ? (
                      <input type="text" value={editForm.validTo} onChange={(e) => setEditForm({ ...editForm, validTo: e.target.value })} style={{ width: 80 }} placeholder="YYYY-MM or blank" />
                    ) : (row.validTo || "至今")}
                  </td>
                  <td style={{ color: row.isActive ? "#16a34a" : "#9ca3af" }}>{row.isActive ? "Active" : "Inactive"}</td>
                  <td style={{ maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis" }}>
                    {isEditing ? (
                      <input type="text" value={editForm.remark} onChange={(e) => setEditForm({ ...editForm, remark: e.target.value })} style={{ width: 120 }} />
                    ) : (row.remark || "")}
                  </td>
                  <td>
                    {isEditing ? (
                      <div style={{ display: "flex", gap: 4 }}>
                        <button className="btn btn-sm btn-primary" onClick={() => saveEdit(row)}>Save</button>
                        <button className="btn btn-sm btn-ghost" onClick={cancelEdit}>Cancel</button>
                      </div>
                    ) : (
                      <button className="btn btn-sm btn-ghost" onClick={() => startEdit(row)} disabled={!row.isActive && !row.validFrom}>Edit</button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
