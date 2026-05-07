import { api } from "../api/client";
import { CustomerInsightsPage } from "./CustomerInsightsPage";


export function NordicHevInsightsPage() {
  return (
    <CustomerInsightsPage
      deckLoader={api.nordicHevCustomerDeck}
      modeOptions={["benchmark"]}
      slideCode="11"
      exportFilePrefix="customer-hev"
      errorTitle="瑞典 HEV 用户调研加载失败"
      benchmarkCopy={{
        chipLabel: "Curated benchmark sample",
        loadingLabel: "正在整理瑞典 HEV 车主画像",
        loadingDetail: "从独立瑞典 HEV benchmark workbook 聚合家庭画像、购车用途与典型 persona。",
        occupationSubtitle: "职业以专业白领、教育、医疗、公共服务与本地管理岗位为主。",
        powertrainSubtitle: "明确偏好 HEV：核心是省油省心，但对 ADAS、车机和科技配置短板也非常敏感。",
        philosophySubtitle: "购买逻辑更偏向全年省心、保值、冬季稳定和家庭长期成本控制，同时不接受科技配置明显落后。",
        useCasesSubtitle: "通勤、接娃、滑雪和家庭长途并存，强调全年低负担与不改习惯。",
        decisionFactorsSubtitle: "油耗、可靠性、冬季稳定、空间，以及 ADAS / 车机配置是否跟得上，都是前列因素。",
        personaSubtitle: "把瑞典 HEV 家庭用户翻译成可直接用于产品和营销讨论的画像。",
      }}
    />
  );
}