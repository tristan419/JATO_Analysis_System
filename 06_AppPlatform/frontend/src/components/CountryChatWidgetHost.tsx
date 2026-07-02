import { CountryChatProvider } from "../contexts/CountryChatContext";
import { CountryChatWidget } from "./CountryChatWidget";
import "../countryCopilot.css";

export function CountryChatWidgetHost() {
  return (
    <CountryChatProvider>
      <CountryChatWidget />
    </CountryChatProvider>
  );
}
