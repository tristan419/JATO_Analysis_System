import { useCountryChat } from "../contexts/CountryChatContext";

interface CountryChatModelSelectProps {
  compact?: boolean;
}

export function CountryChatModelSelect({
  compact = false,
}: CountryChatModelSelectProps) {
  const {
    availableChatModels,
    selectedChatModel,
    sending,
    setSelectedChatModel,
  } = useCountryChat();

  if (availableChatModels.length <= 1) {
    return null;
  }

  const select = (
    <select
      className={compact ? "ccw-country-select" : undefined}
      value={selectedChatModel}
      onChange={(event) => setSelectedChatModel(event.target.value)}
      disabled={sending}
    >
      {availableChatModels.map((item) => (
        <option key={item.id} value={item.id}>
          {item.label}
        </option>
      ))}
    </select>
  );

  if (compact) {
    return select;
  }

  return (
    <label className="copilot-field">
      <span>模型</span>
      {select}
    </label>
  );
}
