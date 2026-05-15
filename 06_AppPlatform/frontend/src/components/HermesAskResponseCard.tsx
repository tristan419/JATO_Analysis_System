import type { HermesChatResponse, HermesChatSuggestedAction, HermesReplyType } from "../types/hermes";

interface Props {
  response: HermesChatResponse;
  onDismiss: () => void;
  onSuggestedAction: (action: HermesChatSuggestedAction) => void;
}

const REPLY_COLORS: Record<HermesReplyType, string> = {
  direct_answer: "#3b82f6", run_created: "#22c55e", clarification_needed: "#f59e0b", blocked_by_policy: "#ef4444",
};
const REPLY_LABELS: Record<HermesReplyType, string> = {
  direct_answer: "Answer", run_created: "Run Created", clarification_needed: "Clarify", blocked_by_policy: "Blocked",
};

export function HermesAskResponseCard({ response, onDismiss, onSuggestedAction }: Props) {
  const color = REPLY_COLORS[response.replyType] || "#94a3b8";
  const label = REPLY_LABELS[response.replyType] || response.replyType;

  return (
    <div className="hermes-chat-response" style={{marginTop:8,padding:14,background:"#fff",borderRadius:8,border:`1px solid ${color}20`,borderLeft:`3px solid ${color}`,position:"relative"}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          <span style={{fontSize:10,fontWeight:700,color,background:`${color}15`,padding:"1px 8px",borderRadius:4,textTransform:"uppercase"}}>{label}</span>
          <span style={{fontSize:10,color:"#94a3b8"}}>{response.intent}{response.confidence > 0 ? ` · ${Math.round(response.confidence * 100)}%` : ""}</span>
        </div>
        <button className="btn btn-sm btn-ghost" style={{fontSize:11}} onClick={onDismiss}>Dismiss</button>
      </div>
      <div style={{fontSize:13,lineHeight:1.5,color:"#1e293b",marginBottom:10}}>{response.answer}</div>
      {response.replyType === "run_created" && response.runId && (
        <div className="hermes-run-card" style={{padding:"10px 14px",background:"#f0fdf4",borderRadius:6,border:"1px solid #bbf7d0",marginBottom:8,fontSize:12}}>
          <div style={{fontWeight:600,color:"#166534",marginBottom:4}}>Run ID: {response.runId}</div>
          {response.command && <div style={{color:"#475569",marginBottom:4}}>Command: <code>{response.command}</code></div>}
          {response.tasks && response.tasks.length > 0 && (
            <div style={{color:"#475569"}}>Tasks: {response.tasks.map((t,i)=>(<span key={t}>{i>0&&" → "}<span style={{background:"#e2e8f0",borderRadius:3,padding:"1px 6px",fontSize:10}}>{t}</span></span>))}</div>
          )}
        </div>
      )}
      {response.dataRefs.length > 0 && (
        <div style={{display:"flex",flexWrap:"wrap",gap:4,marginBottom:8}}>
          {response.dataRefs.filter(Boolean).map(ref=><code key={ref} style={{fontSize:10,background:"#f1f5f9",padding:"2px 6px",borderRadius:3,color:"#475569"}}>{ref}</code>)}
        </div>
      )}
      {response.suggestedActions.length > 0 && (
        <div style={{display:"flex",flexWrap:"wrap",gap:6}}>
          {response.suggestedActions.map((a,i)=><button key={i} className="btn btn-sm btn-primary" style={{fontSize:11}} onClick={()=>onSuggestedAction(a)}>{a.label}</button>)}
        </div>
      )}
      {response.sessionId && <div style={{marginTop:8,fontSize:9,color:"#94a3b8"}}>Session: {response.sessionId.slice(0,30)}...</div>}
    </div>
  );
}
