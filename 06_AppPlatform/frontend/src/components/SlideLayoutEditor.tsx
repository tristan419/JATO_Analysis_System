import type { SlideLayoutSettings } from "../utils/slideLayout";
import { SLIDE_LAYOUT_LIMITS } from "../utils/slideLayout";

const CONTROL_CONFIG: Array<{
  key: keyof SlideLayoutSettings;
  label: string;
  hint: string;
}> = [
  { key: "paddingX", label: "左右边距", hint: "控制画布左右留白" },
  { key: "paddingY", label: "上下边距", hint: "控制画布上下留白" },
  { key: "frameGap", label: "头身间距", hint: "标题区与正文区之间的距离" },
  { key: "headGap", label: "标题内部间距", hint: "标题文案与右侧标签的间距" },
  { key: "bodyGap", label: "正文主间距", hint: "指标区与内容区之间的距离" },
  { key: "contentGap", label: "内容块间距", hint: "正文内部模块之间的距离" },
];

export function SlideLayoutEditor({
  value,
  onChange,
  onReset,
}: {
  value: SlideLayoutSettings;
  onChange: (patch: Partial<SlideLayoutSettings>) => void;
  onReset: () => void;
}) {
  return (
    <section className="slide-layout-editor">
      <div className="slide-layout-editor-head">
        <div>
          <strong>Layout Edit</strong>
          <p>仅调整 presentation layer 的固定画布版式参数，不改内容密度规则。</p>
        </div>
        <button type="button" className="btn btn-ghost btn-sm" onClick={onReset}>
          Reset layout
        </button>
      </div>
      <div className="slide-layout-editor-grid">
        {CONTROL_CONFIG.map((control) => {
          const bounds = SLIDE_LAYOUT_LIMITS[control.key];
          const currentValue = value[control.key];
          return (
            <label key={control.key} className="slide-layout-control">
              <span className="slide-layout-control-head">
                <strong>{control.label}</strong>
                <span>{currentValue}px</span>
              </span>
              <input
                type="range"
                min={bounds.min}
                max={bounds.max}
                step={1}
                value={currentValue}
                onChange={(event) => onChange({ [control.key]: Number(event.target.value) })}
              />
              <small>{control.hint}</small>
            </label>
          );
        })}
      </div>
    </section>
  );
}
