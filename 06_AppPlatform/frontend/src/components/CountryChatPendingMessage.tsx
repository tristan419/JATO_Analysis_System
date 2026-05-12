import { useEffect, useState } from "react";

const FUN_WAITING_PHRASES = [
  "翻阅销量档案中...",
  "比对动力×驱动×渠道交叉数据...",
  "匹配合适的分析模型...",
  "检查各国政策与碳税规则...",
  "DeepSeek 正在逐字生成报告...",
  "马上就好，数据量有点大...",
  "正在整理证据链...",
  "核对 MSRP 价格数据中...",
];

export function CountryChatPendingMessage({
  question,
  streamingContent = "",
  compact = false,
}: {
  question: string;
  streamingContent?: string;
  compact?: boolean;
}) {
  const [phraseIndex, setPhraseIndex] = useState(0);

  useEffect(() => {
    if (streamingContent) return;
    const timer = window.setInterval(() => {
      setPhraseIndex((i) => (i + 1) % FUN_WAITING_PHRASES.length);
    }, 2000);
    return () => window.clearInterval(timer);
  }, [streamingContent]);

  // Show actual streaming content if available
  if (streamingContent) {
    const lines = streamingContent.split("\n").filter(Boolean);
    const preview = lines.slice(-3).join("\n");
    return (
      <div className={`copilot-loading${compact ? " is-compact" : ""}`}>
        <div className="copilot-loading-kicker">DeepSeek 正在生成</div>
        <div className="copilot-loading-current" style={{ maxHeight: 120, overflow: "hidden", whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.5 }}>
          {preview.length > 300 ? preview.slice(-300) : preview || "..."}
        </div>
      </div>
    );
  }

  // Otherwise show waiting phrases (during snapshot building)
  return (
    <div className={`copilot-loading${compact ? " is-compact" : ""}`}>
      <div className="copilot-loading-kicker">准备数据中</div>
      <div className="copilot-loading-current">{FUN_WAITING_PHRASES[phraseIndex]}</div>
      <div className="copilot-loading-steps">
        {FUN_WAITING_PHRASES.slice(0, compact ? 3 : 5).map((phrase, i) => (
          <span
            key={phrase}
            className={[
              "copilot-loading-step",
              i < phraseIndex ? "is-done" : "",
              i === phraseIndex ? "is-active" : "",
            ].filter(Boolean).join(" ")}
          >
            {phrase}
          </span>
        ))}
      </div>
    </div>
  );
}
