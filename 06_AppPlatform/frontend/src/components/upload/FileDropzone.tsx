import {
  useCallback,
  useRef,
  useState,
  type CSSProperties,
  type DragEvent,
} from "react";

interface FileDropzoneProps {
  accept: string;
  label: string;
  hint: string;
  file: File | null;
  onFile: (file: File) => void;
  onClear?: () => void;
  className?: string;
  minHeight?: number;
}

export function FileDropzone({
  accept,
  label,
  hint,
  file,
  onFile,
  onClear,
  className,
  minHeight = 110,
}: FileDropzoneProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const handleDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      setDragging(false);
      const nextFile = event.dataTransfer.files[0];
      if (nextFile) onFile(nextFile);
    },
    [onFile],
  );

  return (
    <div
      className={["dropzone", file ? "has-file" : "", dragging ? "dragover" : "", className].filter(Boolean).join(" ")}
      onClick={() => inputRef.current?.click()}
      onDragOver={(event) => {
        event.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      style={{
        ...dropzoneStyle,
        minHeight,
        borderColor: file ? "#16a34a" : dragging ? "#2563eb" : "#d1d5db",
        padding: file ? "22px 38px 18px 14px" : "18px 14px",
        background: file ? "#f0fdf4" : dragging ? "#eff6ff" : "#fafafa",
      }}
    >
      {file && onClear ? (
        <button
          type="button"
          aria-label={`清除${label}`}
          title="清除文件"
          onClick={(event) => {
            event.stopPropagation();
            onClear();
          }}
          style={dropzoneClearButtonStyle}
        >
          ×
        </button>
      ) : null}
      <strong style={{ color: file ? "#15803d" : "#111827", fontSize: 14 }}>{label}</strong>
      <span style={{ color: "#64748b", fontSize: 12 }}>{file ? file.name : hint}</span>
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        hidden
        onChange={(event) => {
          const nextFile = event.target.files?.[0];
          if (nextFile) onFile(nextFile);
          event.target.value = "";
        }}
      />
    </div>
  );
}

const dropzoneStyle: CSSProperties = {
  position: "relative",
  border: "2px dashed #d1d5db",
  borderRadius: 8,
  display: "grid",
  alignContent: "center",
  gap: 6,
  cursor: "pointer",
};

const dropzoneClearButtonStyle: CSSProperties = {
  position: "absolute",
  top: 8,
  right: 8,
  width: 24,
  height: 24,
  border: "1px solid #86efac",
  borderRadius: 999,
  background: "#ffffff",
  color: "#15803d",
  cursor: "pointer",
  fontSize: 18,
  lineHeight: "20px",
  fontWeight: 700,
};
