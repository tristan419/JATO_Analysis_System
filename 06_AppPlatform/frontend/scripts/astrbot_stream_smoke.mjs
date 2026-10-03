import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function parseArgs(argv) {
  const result = {
    baseUrl: process.env.ASTRBOT_STREAM_BASE_URL || "http://127.0.0.1:5176",
    country: process.env.ASTRBOT_STREAM_COUNTRY || "Sweden",
    question: process.env.ASTRBOT_STREAM_QUESTION || "用一句话回答：瑞典汽车市场分析为什么需要证据来源？",
    headed: false,
    timeoutMs: 60000,
  };
  for (const arg of argv) {
    if (arg.startsWith("--base-url=")) {
      result.baseUrl = arg.slice("--base-url=".length).replace(/\/+$/, "");
    } else if (arg.startsWith("--country=")) {
      result.country = arg.slice("--country=".length);
    } else if (arg.startsWith("--question=")) {
      result.question = arg.slice("--question=".length);
    } else if (arg.startsWith("--timeout-ms=")) {
      result.timeoutMs = Math.max(5000, Number(arg.slice("--timeout-ms=".length)) || result.timeoutMs);
    } else if (arg === "--headed") {
      result.headed = true;
    }
  }
  return result;
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const artifactDir = path.join(frontendRoot, "artifacts", "astrbot-stream-smoke", runId);
  await mkdir(artifactDir, { recursive: true });

  const browser = await chromium.launch({ headless: !options.headed });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const networkEvents = [];
  page.on("response", response => {
    if (response.url().includes("/v1/astrbot/agent/stream")) {
      networkEvents.push({
        url: response.url(),
        status: response.status(),
        contentType: response.headers()["content-type"] || "",
        at: Date.now(),
      });
    }
  });

  try {
    await page.goto(`${options.baseUrl}/astrbot`, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
    await page.evaluate(() => {
      localStorage.setItem("jato_user_name", "codex-stream-smoke");
    });

    const bodyText = await page.locator("body").innerText({ timeout: 10000 });
    if (/404\s*not\s*found/i.test(bodyText)) {
      throw new Error("/astrbot showed 404 Not Found");
    }

    const userSurface = page.locator(".astrbot-agent-surface.is-user");
    await userSurface.locator("textarea").fill(options.question, { timeout: 10000 });
    const countryInput = userSurface.locator(".astrbot-agent-fields label").filter({ hasText: "Country" }).locator("input");
    await countryInput.fill(options.country, { timeout: 10000 });

    await page.evaluate(() => {
      const latestAssistantSnapshot = () => {
        const assistantMessages = [...document.querySelectorAll(".astrbot-chat-message.is-assistant")];
        const latest = assistantMessages[assistantMessages.length - 1];
        const text = latest?.querySelector(".astrbot-chat-answer p")?.textContent?.trim() || "";
        return {
          assistantCount: assistantMessages.length,
          text,
          textLength: text.length,
          streamingCount: document.querySelectorAll(".astrbot-chat-message.is-assistant.is-streaming").length,
          streamingTextCount: document.querySelectorAll(".astrbot-chat-message.is-assistant .is-streaming-text").length,
        };
      };
      window.__astrbotStreamSmoke = {
        startedAt: Date.now(),
        samples: [],
      };
      const record = () => {
        const snapshot = latestAssistantSnapshot();
        window.__astrbotStreamSmoke.samples.push({
          at: Date.now(),
          ...snapshot,
        });
      };
      record();
      window.__astrbotStreamSmoke.observer = new MutationObserver(record);
      window.__astrbotStreamSmoke.observer.observe(document.body, {
        childList: true,
        subtree: true,
        characterData: true,
      });
    });

    await page.getByRole("button", { name: "Send", exact: true }).click({ timeout: 10000 });

    await page.waitForFunction(() => {
      const latestAssistantSnapshot = () => {
        const assistantMessages = [...document.querySelectorAll(".astrbot-chat-message.is-assistant")];
        const latest = assistantMessages[assistantMessages.length - 1];
        const text = latest?.querySelector(".astrbot-chat-answer p")?.textContent?.trim() || "";
        return {
          assistantCount: assistantMessages.length,
          text,
          textLength: text.length,
          streamingCount: document.querySelectorAll(".astrbot-chat-message.is-assistant.is-streaming").length,
          streamingTextCount: document.querySelectorAll(".astrbot-chat-message.is-assistant .is-streaming-text").length,
        };
      };
      const state = window.__astrbotStreamSmoke;
      if (!state) return false;
      const latest = latestAssistantSnapshot();
      const sawStreaming = state.samples.some(sample => sample.streamingCount > 0 || sample.streamingTextCount > 0);
      return sawStreaming && latest.assistantCount > 0 && latest.streamingCount === 0 && latest.textLength > 0;
    }, null, { timeout: options.timeoutMs });

    await page.evaluate(() => {
      window.__astrbotStreamSmoke?.observer?.disconnect?.();
    });

    const result = await page.evaluate(() => {
      const latestAssistantSnapshot = () => {
        const assistantMessages = [...document.querySelectorAll(".astrbot-chat-message.is-assistant")];
        const latest = assistantMessages[assistantMessages.length - 1];
        const text = latest?.querySelector(".astrbot-chat-answer p")?.textContent?.trim() || "";
        return {
          assistantCount: assistantMessages.length,
          text,
          textLength: text.length,
          streamingCount: document.querySelectorAll(".astrbot-chat-message.is-assistant.is-streaming").length,
          streamingTextCount: document.querySelectorAll(".astrbot-chat-message.is-assistant .is-streaming-text").length,
        };
      };
      const state = window.__astrbotStreamSmoke || { samples: [], startedAt: Date.now() };
      const samples = state.samples || [];
      const streamingSamples = samples.filter(sample => sample.streamingCount > 0 || sample.streamingTextCount > 0);
      const tokenSamples = streamingSamples.filter(sample => sample.textLength > 0);
      const uniqueStreamingLengths = new Set(tokenSamples.map(sample => sample.textLength));
      const finalSnapshot = latestAssistantSnapshot();
      const finalTextLooksLikeError = /^error\\s*:/i.test(finalSnapshot.text)
        || /UNEXPECTED_EOF|Connection refused|Traceback|Exception/i.test(finalSnapshot.text);
      const firstStreamingAt = streamingSamples[0]?.at || 0;
      const firstTokenAt = tokenSamples[0]?.at || 0;
      const finalAt = samples[samples.length - 1]?.at || Date.now();
      const answerLimitations = [...document.querySelectorAll("details.astrbot-answer-limitations")];
      const evidenceGaps = [...document.querySelectorAll("details.astrbot-evidence-gap-panel")];
      return {
        sampleCount: samples.length,
        streamingSampleCount: streamingSamples.length,
        tokenSampleCount: tokenSamples.length,
        uniqueStreamingTokenLengths: uniqueStreamingLengths.size,
        streamingObserved: streamingSamples.length > 0,
        tokenObservedWhileStreaming: tokenSamples.length > 0,
        tokenGrowthObserved: uniqueStreamingLengths.size > 1,
        firstStreamingDelayMs: firstStreamingAt ? firstStreamingAt - state.startedAt : null,
        firstTokenDelayMs: firstTokenAt ? firstTokenAt - state.startedAt : null,
        totalDurationMs: finalAt - state.startedAt,
        finalTextLength: finalSnapshot.textLength,
        finalTextPreview: finalSnapshot.text.slice(0, 240),
        finalTextLooksLikeError,
        answerLimitationsPanelCount: answerLimitations.length,
        answerLimitationsOpenCount: answerLimitations.filter(item => item.open).length,
        evidenceGapPanelCount: evidenceGaps.length,
        evidenceGapOpenCount: evidenceGaps.filter(item => item.open).length,
      };
    });

    const screenshotPath = path.join(artifactDir, "astrbot_stream_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: false });

    const summary = {
      createdAt: new Date().toISOString(),
      appUrl: `${options.baseUrl}/astrbot`,
      country: options.country,
      question: options.question,
      networkEvents,
      screenshotPath,
      ...result,
      passed: Boolean(
        networkEvents.some(event => event.status === 200 && event.contentType.includes("text/event-stream"))
        && result.streamingObserved
        && result.tokenObservedWhileStreaming
        && result.finalTextLength > 0
        && !result.finalTextLooksLikeError
        && result.answerLimitationsOpenCount === 0
        && result.evidenceGapOpenCount === 0
      ),
    };
    const summaryPath = path.join(artifactDir, "astrbot_stream_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ artifactDir, summaryPath, ...summary }, null, 2));
    if (!summary.passed) {
      process.exitCode = 1;
    }
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
