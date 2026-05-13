import { useEffect, useState } from "react";
import { renderMarkdown } from "../contexts/countryChatHelpers";

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
  question: _question,
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

  if (streamingContent) {
    const html = renderMarkdown(streamingContent);
    return (
      <div className={`copilot-loading${compact ? " is-compact" : ""}`}>
        <div className="copilot-loading-kicker">DeepSeek 正在生成</div>
        <div
          className="copilot-loading-current"
          style={{ maxHeight: 300, overflow: "hidden", fontSize: 13, lineHeight: 1.6 }}
          dangerouslySetInnerHTML={{ __html: html }}
        />
      </div>
    );
  }

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
