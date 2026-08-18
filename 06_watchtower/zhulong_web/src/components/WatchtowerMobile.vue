<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import * as echarts from 'echarts'

import { useAxios } from '../composables/useAxios'
import { useSessionStore } from '../stores/session'

const { get, post, baseURL } = useAxios()
const session = useSessionStore()

const tabs = [
  { id: 'today', label: '今日标的' },
  { id: 'touchstone', label: '验金石' },
  { id: 'shadow', label: '模拟盘' },
  { id: 'quota', label: 'AI消耗' },
]

const activeTab = ref('today')
const heartbeatTimer = ref(null)
const chartEl = ref(null)
const showDisagreements = ref(false)
const selectedAuditSymbol = ref('')
const selectedShadowSymbol = ref('')
const showShadowHistory = ref(false)
const touchstoneKeyStorage = 'watchtower_touchstone_key'
const touchstoneHistoryStorage = 'watchtower_touchstone_history'
const touchstoneAuthorized = ref(false)
const touchstoneHistory = ref([])
let pnlChart = null

const heartbeat = reactive({ ok: false, latency: 0, timestamp: '', error: '', loading: false, latestTradeDate: '', stockCount: 0, dbSizeMb: 0 })
const today = reactive({ loading: false, error: '', tradeDate: '', state: '', message: '', funnel: null, approved: [], disagreements: null })
const touchstone = reactive({
  code: '',
  busy: false,
  error: '',
  result: null,
  steps: [
    { id: 'l1', label: '行情快照', state: 'idle' },
    { id: 'l2', label: '快速风控', state: 'idle' },
    { id: 'l4', label: '云端审查', state: 'idle' },
  ],
})
const shadow = reactive({ loading: false, error: '', data: null })
const quota = reactive({ loading: false, error: '', data: null })

const channelLabel = computed(() => {
  if (baseURL.includes('api.example.com')) return '公网通道'
  if (baseURL.includes('192.168.')) return '局域网通道'
  if (baseURL.includes('127.0.0.1') || baseURL.includes('localhost')) return '本机通道'
  return '备用通道'
})
const channelTone = computed(() => {
  if (!heartbeat.ok) return 'veto'
  if (heartbeat.latency >= 1800) return 'warn'
  if (heartbeat.latency >= 800) return 'slow'
  return 'pass'
})
const channelStatusText = computed(() => {
  if (heartbeat.loading) return '检测中'
  if (!heartbeat.ok) return '连接异常'
  if (heartbeat.latency >= 1800) return '通道稍慢'
  if (heartbeat.latency >= 800) return '通道正常'
  return '通道顺畅'
})
const channelHint = computed(() => {
  if (heartbeat.loading) return '正在检测云端入口。'
  if (!heartbeat.ok) return '公网 API 暂不可用；同一局域网内可尝试使用内网地址访问。'
  if (heartbeat.latency >= 1800) return '公网链路可用但延迟偏高，刷新和验金石可能需要多等几秒。'
  if (heartbeat.latency >= 800) return '公网链路正常，当前延迟略高。'
  return '公网链路稳定，数据同步正常。'
})

const auditTone = computed(() => {
  if (today.error) return 'danger'
  if (today.approved.length > 0) return 'pass'
  if (today.funnel?.is_empty_position) return 'warn'
  return 'muted'
})

const passRate = computed(() => {
  const l1 = Number(today.funnel?.l1 || 0)
  const l4 = Number(today.funnel?.l4 || 0)
  return l1 > 0 ? ((l4 / l1) * 100).toFixed(2) : '0.00'
})

const todaySummary = computed(() => {
  if (today.message === 'No audits today yet. Next run at 21:00.') return '今天暂无审计记录，下一次日终审计预计 21:00 启动。'
  if (today.message) return localizeText(today.message)
  if (today.funnel?.is_empty_position) return '未有标的进入模拟持仓池。'
  return '审计快照已同步。'
})

const localDateText = () => new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Shanghai' }).format(new Date())
const tradeDateHint = computed(() => {
  if (!today.tradeDate) return ''
  const currentDate = localDateText()
  if (currentDate > today.tradeDate) return '当前为非交易日或当日审计尚未完成，沿用上一交易日审计结果。'
  return '展示最近一个已完成交易日的漏斗结果。'
})

const emptyReasons = computed(() => {
  const reason = today.funnel?.empty_reason || {}
  return [
    { label: '中层审计拦截', value: Number(reason.l3_intercept || 0), key: 'l3' },
    { label: '云端终审否决', value: Number(reason.l4_judge_veto || 0), key: 'l4' },
    { label: '公证层硬否', value: Number(reason.notary_hard_veto || 0), key: 'notary' },
    { label: '快速风控终止', value: Number(reason.l2_terminal || 0), key: 'l2' },
  ].filter((item) => item.value > 0)
})
const emptyPositionInsight = computed(() => {
  const funnel = today.funnel || {}
  const reason = funnel.empty_reason || {}
  const runRows = Number(reason.run_rows || 0)
  const l1 = Number(funnel.l1 || 0)
  const l2 = Number(funnel.l2 || 0)
  const l3 = Number(funnel.l3 || 0)
  const l4 = Number(funnel.l4 || 0)
  const topReason = [...emptyReasons.value].sort((a, b) => b.value - a.value)[0]

  if (!runRows && !l2 && !l3 && !l4) {
    return {
      title: '日终审计尚未完成',
      summary: '当前还没有拿到有效审计结果，不能把空仓理解为系统否定市场。',
      action: '等待晚间日终审计完成后再看；如果只是非交易日，会沿用最近一个已完成交易日。',
    }
  }
  if (l1 > 0 && !l2) {
    return {
      title: '快速风控没有放行标的',
      summary: '行情池已有数据，但没有标的通过第一轮快速风控，说明当日基础形态整体不理想。',
      action: '这种空仓更偏防守，不建议为了交易感强行找票。',
    }
  }
  if (topReason?.key === 'l2') {
    return {
      title: '主要卡在快速风控',
      summary: '候选在基础风险检查阶段被终止，通常代表波动、流动性或形态质量不够稳定。',
      action: '优先等待下一次行情结构改善，不急着进入单票深挖。',
    }
  }
  if (topReason?.key === 'l3') {
    return {
      title: '主要卡在中层审计',
      summary: '快速筛选能找到候选，但中层逻辑没有确认足够清晰的交易理由。',
      action: '可以关注分歧面板，但不宜直接把被拦截标的拿去模拟盘。',
    }
  }
  if (topReason?.key === 'l4') {
    return {
      title: '主要卡在云端终审',
      summary: '前面层级有候选通过，但云端复核认为最终质量或风险收益不够好。',
      action: '这种空仓说明系统偏谨慎，适合保留观察而不是追高补票。',
    }
  }
  if (topReason?.key === 'notary') {
    return {
      title: '公证层触发硬性否决',
      summary: '候选触碰了更严格的风险规则，因此没有进入通过池。',
      action: '尊重硬否结果，除非后续数据明显修复。',
    }
  }
  return {
    title: '没有形成有效标的',
    summary: '漏斗完成后没有标的进入通过池，说明今天缺少足够稳的交易证据。',
    action: '保持空仓或轻仓观察，等待下一次完整审计。',
  }
})

const disagreementStats = computed(() => {
  const summary = today.disagreements?.summary || {}
  return [
    { label: 'L3放行/L4否决', value: Number(summary.l3_pass_l4_veto || 0), tone: 'veto' },
    { label: 'L3观望/L4通过', value: Number(summary.l3_hold_l4_pass || 0), tone: 'pass' },
    { label: 'L3否决/L4通过', value: Number(summary.l3_veto_l4_pass || 0), tone: 'warn' },
    { label: 'L2高风险/L3放行', value: Number(summary.l2_high_risk_l3_pass || 0), tone: 'warn' },
    { label: 'L2通过/L3否决', value: Number(summary.l2_pass_l3_veto || 0), tone: 'veto' },
  ].filter((item) => item.value > 0)
})

const phraseMap = {
  'Phase 3 in progress': '第三阶段审计进行中',
  'stocks processed': '只标的已处理',
  'reached L4': '只进入 L4',
  'Audit complete': '审计已完成',
  'processed': '已处理',
  'reached L4 verdict': '只完成 L4 终审',
  'Audit in progress': '审计进行中',
  'in pipeline': '只已进入流水线',
  'awaiting L4': '等待 L4 终审',
  'Strategic alignment confirmed.': '战略方向已确认。',
  'above dynamic stop': '现价高于动态止损线',
  'strong unrealized gain; trailing stop active': '浮盈较强，移动止盈已启用',
  'new intraday high; profit floor active': '盘中新高，利润保护线已上移',
}

const localizeText = (value) => {
  let text = String(value || '').trim()
  if (!text) return ''
  text = text.replaceAll('跑批', '日终审计')
  text = text
    .replace(/final\s+score\s+too\s+low\s*[（(](\d+(?:\.\d+)?)[）)]/ig, '终审评分偏低，当前仅 $1 分')
    .replace(/score\s+too\s+low\s*[（(](\d+(?:\.\d+)?)[）)]/ig, '综合评分偏低，当前仅 $1 分')
    .replace(/final\s+score\s+too\s+low/ig, '终审评分偏低')
    .replace(/score\s+too\s+low/ig, '综合评分偏低')
    .replace(/risk\s+score\s+too\s+high\s*[（(](\d+(?:\.\d+)?)[）)]/ig, '风险评分偏高，当前为 $1 分')
    .replace(/risk\s+too\s+high/ig, '风险偏高')
    .replace(/data\s+insufficient/ig, '数据不足')
    .replace(/insufficient\s+data/ig, '数据不足')
    .replace(/no\s+data/ig, '暂无数据')
    .replace(/low\s+confidence/ig, '置信度偏低')
    .replace(/veto/ig, '否决')
    .replace(/hold/ig, '观望')
    .replace(/pass/ig, '通过')
  Object.entries(phraseMap).forEach(([en, cn]) => { text = text.replaceAll(en, cn) })
  return text
}

const reasonText = (code) => ({
  L3_PASS_L4_VETO: 'L3 放行 / L4 否决',
  L3_HOLD_L4_PASS: 'L3 观望 / L4 通过',
  L3_VETO_L4_PASS: 'L3 否决 / L4 通过',
  L2_HIGH_RISK_L3_PASS: 'L2 高风险 / L3 放行',
  L2_PASS_L3_VETO: 'L2 通过 / L3 否决',
}[String(code || '').toUpperCase()] || localizeText(code) || '模型分歧')

const disagreementTitle = (item) => Array.isArray(item?.reasons) && item.reasons.length ? item.reasons.map(reasonText).join(' / ') : '模型分歧'
const looksLikePromptText = (value) => {
  const text = String(value || '')
  return /其他建议|输入数据|TASK|请严格按照|不得出现|###|SYM:|量价配合分析|system prompt/i.test(text)
}

const disagreementSummary = (item) => {
  const reasons = Array.isArray(item?.reasons) ? item.reasons.map((r) => String(r).toUpperCase()) : []
  const l2 = item?.l2_risk_score ?? '--'
  const l3 = verdictText(item?.l3_verdict)
  const l4 = verdictText(item?.l4_final_verdict)
  if (reasons.includes('L3_PASS_L4_VETO')) return 'L3 已放行，但 L4 云端终审识别到更高风险，当前不进入通过池。'
  if (reasons.includes('L2_HIGH_RISK_L3_PASS')) return `L2 风险分 ${l2} 偏高，但 L3 仍给出${l3}，需要复核风险是否被低估。`
  if (reasons.includes('L3_HOLD_L4_PASS')) return 'L3 仅建议观望，但 L4 终审给出通过，属于偏积极分歧。'
  if (reasons.includes('L3_VETO_L4_PASS')) return 'L3 否决而 L4 通过，需重点复核数据与裁决依据。'
  if (reasons.includes('L2_PASS_L3_VETO')) return 'L2 快检通过，但 L3 战略审计否决，说明中层逻辑发现风险。'
  return `当前链路裁决为 L3 ${l3} / L4 ${l4}，建议保留复核。`
}

const disagreementDetailText = (item) => {
  const vetoReason = String(item?.l4_veto_reason || '').trim()
  if (vetoReason && !looksLikePromptText(vetoReason)) return localizeText(vetoReason)
  return disagreementSummary(item)
}
const auditReasonText = (item) => localizeText(item?.l4_veto_reason || item?.status || '等待复核')
const shadowStateText = (value) => ({
  ACTIVE_HOLD: '正常持有',
  PULLBACK_WATCH: '回撤观察',
  DANGER_WATCH: '风险观察',
  STOP_NEAR: '接近止损',
  PROFIT_PROTECTED: '利润保护',
  CLOSED_PROFIT: '止盈结束',
  CLOSED_LOSS: '止损结束',
  TAKE_PROFIT_WATCH: '止盈观察',
  HOLD_NO_REALTIME: '无实时行情',
  RAISE_STOP: '上移止损',
  SELL_STOP: '触发止损',
}[String(value || '').toUpperCase()] || localizeText(value) || '--')
const shadowQualityText = (value) => ({
  PENDING: '待观察',
  RISK_NEEDS_WATCH: '需盯风险',
  WIN_RUNNING: '盈利延续',
  BUY_VALIDATED_RUNNING: '买点验证中',
  BUY_WEAK_RUNNING: '买点偏弱',
  SELL_PROTECTED_PROFIT: '保护性止盈',
  STOP_LOSS_CONFIRMED: '止损确认',
}[String(value || '').toUpperCase()] || localizeText(value) || '--')
const shadowEmptyText = computed(() => {
  const message = String(shadow.data?.message || '')
  if (!message || /no active shadow positions/i.test(message)) return '暂无模拟持仓，资金处于等待状态。'
  return localizeText(message)
})
const recentShadowHistory = computed(() => {
  const rows = Array.isArray(shadow.data?.history_trades) ? shadow.data.history_trades : []
  return rows.slice(0, 5)
})
const toggleShadowDetail = (pos) => {
  selectedShadowSymbol.value = selectedShadowSymbol.value === pos.symbol ? '' : pos.symbol
}
const shadowDecisionTone = (pos) => {
  const state = String(pos?.decision_state || '').toUpperCase()
  if (['DANGER_WATCH', 'STOP_NEAR', 'SELL_STOP'].includes(state)) return 'veto'
  if (['PULLBACK_WATCH', 'TAKE_PROFIT_WATCH', 'HOLD_NO_REALTIME'].includes(state)) return 'warn'
  return 'pass'
}
const shadowPositionSummary = (pos) => {
  const state = shadowStateText(pos?.decision_state)
  const pnl = signedPct(pos?.pnl_ratio)
  if (asNumber(pos?.pnl_ratio) > 0.03) return `当前浮盈 ${pnl}，状态为${state}，优先保护利润并观察趋势延续。`
  if (asNumber(pos?.pnl_ratio) < -0.02) return `当前浮亏 ${pnl}，状态为${state}，需要重点盯住止损线。`
  return `当前盈亏 ${pnl}，状态为${state}，仍处于模拟验证阶段。`
}
const shadowSourceText = (value) => ({
  L4_PASS: '云端终审通过后进入模拟盘',
  L3_PASS: '中层审计通过后进入模拟盘',
  MANUAL: '人工登记进入模拟盘',
  SYSTEM: '系统调度进入模拟盘',
}[String(value || '').toUpperCase()] || '模拟盘执行记录')
const shadowSellRuleText = (value) => ({
  TAKE_PROFIT_8_NORMAL: '普通票 8% 分批止盈',
  TAKE_PROFIT_8_STRONG: '强势票 8% 轻减仓',
  TAKE_PROFIT_12: '12% 主升兑现',
  TRAILING_RUNNER_STOP: '移动止盈退出',
  WRONG_PICK_STOP: '选票错误止损',
  TIME_STOP: '时间效率退出',
  HARD_STOP: '硬止损',
  FORCE_SELL: '强制风控卖出',
}[String(value || '').toUpperCase()] || localizeText(value) || '卖出记录')
const shadowStrategyText = (value) => {
  const text = String(value || '')
  if (!text) return ''
  const score = text.match(/final_score=([0-9.]+)/i)?.[1]
  if (/PASS/i.test(text) && score) return `策略记录显示该仓位来自通过池，终审参考分为 ${score}。`
  if (/PASS/i.test(text)) return '策略记录显示该仓位来自通过池。'
  return '策略记录已留存，供后续复盘使用。'
}
const shadowPositionReasons = (pos) => {
  const reasons = []
  if (pos?.source) reasons.push(`入场来源：${shadowSourceText(pos.source)}。`)
  if (pos?.entry_score !== null && pos?.entry_score !== undefined) reasons.push(`入场参考分为 ${pos.entry_score}，属于系统允许跟踪的候选。`)
  const strategyText = shadowStrategyText(pos?.strategy_tag)
  if (strategyText) reasons.push(strategyText)
  if (pos?.qty) reasons.push(`模拟持股 ${fmtInt(pos.qty)} 股，入场成本 ${pos.entry_price}。`)
  return reasons.length ? reasons : ['该持仓来自模拟盘执行记录，等待更多复盘数据沉淀。']
}
const shadowPositionRisks = (pos) => {
  const risks = []
  const pnl = asNumber(pos?.pnl_ratio)
  if (pnl < 0) risks.push(`当前处于浮亏，亏损比例为 ${signedPct(pos.pnl_ratio)}。`)
  if (pos?.dynamic_stop_price) risks.push(`动态止损线为 ${pos.dynamic_stop_price}，现价接近时应优先复核。`)
  if (String(pos?.decision_state || '').toUpperCase().includes('DANGER')) risks.push('持仓状态已进入风险观察，需要降低主观加仓冲动。')
  if (String(pos?.quality_label || '').toUpperCase().includes('RISK')) risks.push('质量标签提示需盯风险，说明买点还没有完全验证。')
  return risks.length ? risks : ['当前未触发显著风险标签，继续按模拟盘规则跟踪。']
}
const shadowPositionAction = (pos) => {
  const state = String(pos?.decision_state || '').toUpperCase()
  if (['DANGER_WATCH', 'STOP_NEAR', 'SELL_STOP'].includes(state)) return '优先观察止损线和次日走势，不做主动加仓假设。'
  if (['PULLBACK_WATCH', 'TAKE_PROFIT_WATCH'].includes(state)) return '继续持有观察，重点看回撤是否收敛，盈利仓位注意保护。'
  if (asNumber(pos?.pnl_ratio) > 0) return '保持跟踪，若趋势延续可等待系统上移保护线。'
  return '按模拟盘纪律观察，不因短期波动提前得出结论。'
}

const quotaCloudCost = computed(() => {
  const buckets = Array.isArray(quota.data?.buckets) ? quota.data.buckets : []
  return buckets.filter((item) => !String(item.provider || '').toLowerCase().includes('ollama'))
})

const normalizeSymbol = (raw) => {
  const text = String(raw || '').trim().replace(/\s+/g, '')
  if (!text) return ''
  const upper = text.toUpperCase()
  if (/^\d{6}$/.test(upper)) return /^[69]/.test(upper) ? `${upper}.SH` : `${upper}.SZ`
  if (/^\d{6}\.(SH|SZ)$/.test(upper)) return upper
  if (/^(SH|SZ)\d{6}$/.test(upper)) return `${upper.slice(2)}.${upper.slice(0, 2)}`
  return text
}

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const asNumber = (value) => Number.isFinite(Number(value)) ? Number(value) : 0
const fmtInt = (value) => Math.round(asNumber(value)).toLocaleString('zh-CN')
const fmtMoney = (value) => asNumber(value).toLocaleString('zh-CN', { maximumFractionDigits: 0 })
const fmtPct = (value) => `${(asNumber(value) * 100).toFixed(2)}%`
const signedPct = (value) => `${asNumber(value) >= 0 ? '+' : ''}${fmtPct(value)}`
const signedMoney = (value) => `${asNumber(value) >= 0 ? '+' : ''}${fmtMoney(value)}`
const positionTitle = (pos) => pos?.name ? `${pos.name} ${pos.symbol}` : pos?.symbol || '--'
const historyTradeTitle = (trade) => trade?.name ? `${trade.name} ${trade.symbol}` : trade?.symbol || '--'
const auditTitle = (item) => item?.name || item?.symbol || '--'
const auditFullTitle = (item) => item?.name ? `${item.name} ${item.symbol}` : item?.symbol || '--'
const auditScore = (item) => item?.l4_final_score ?? item?.l4_blue_score ?? item?.final_score ?? item?.l3_audit_score ?? '--'
const selectedAudit = computed(() => today.approved.find((item) => item.symbol === selectedAuditSymbol.value) || null)
const selectedAuditShadow = computed(() => {
  const symbol = selectedAudit.value?.symbol
  const positions = Array.isArray(shadow.data?.positions) ? shadow.data.positions : []
  return positions.find((item) => item.symbol === symbol) || null
})
const toggleAuditDetail = (item) => {
  selectedAuditSymbol.value = selectedAuditSymbol.value === item.symbol ? '' : item.symbol
}
const auditDetailSummary = (item) => {
  const score = auditScore(item)
  const l2 = item?.l2_risk_score ?? '--'
  const l3 = verdictText(item?.l3_verdict)
  const l4 = verdictText(item?.l4_final_verdict)
  if (String(item?.l4_final_verdict || '').toUpperCase() === 'PASS') return `快检已通过，风险分为 ${l2}；中层审计给出${l3}，云端终审给出${l4}，当前参考分为 ${score}。`
  return `当前链路结果为中层审计${l3}、云端终审${l4}，参考分为 ${score}。`
}
const auditDetailReasons = (item) => {
  const reasons = []
  const l2 = Number(item?.l2_risk_score)
  if (item?.l2_passed) reasons.push(`快速风控已通过，风险分为 ${Number.isFinite(l2) ? l2 : '--'}。`)
  if (String(item?.l3_verdict || '').toUpperCase() === 'PASS') reasons.push(`中层审计给出通过，审计分为 ${item?.l3_audit_score ?? '--'}。`)
  if (String(item?.l4_final_verdict || '').toUpperCase() === 'PASS') reasons.push('云端终审没有触发硬性否决，允许进入通过池。')
  const text = auditReasonText(item)
  if (text && text !== 'L4_DONE' && text !== '等待复核' && !reasons.includes(text)) reasons.push(text)
  return reasons.length ? reasons : ['已通过当日漏斗筛选，等待后续模拟盘或人工复核。']
}
const auditDetailRisks = (item) => {
  const risks = []
  const l2 = Number(item?.l2_risk_score)
  const score = Number(auditScore(item))
  if (Number.isFinite(l2) && l2 >= 60) risks.push(`快检风险分 ${l2} 偏高，说明短线结构仍需盯紧。`)
  if (Number.isFinite(score) && score < 70) risks.push(`参考分 ${score} 不算高，只代表通过底线，不代表强确定性。`)
  if (item?.data_quality_warning) risks.push(localizeText(item.data_quality_warning))
  if (item?.l4_notary_fatal_flag) risks.push('公证层出现硬性风险标记，需要优先复核。')
  return risks.length ? risks : ['当前没有暴露明显硬性风险，但仍需接受模拟盘验证。']
}
const auditShadowText = (pos) => {
  if (!pos) return '尚未在模拟盘持仓中出现，可能还在等待仓位、调度或后续确认。'
  return `已进入模拟盘，当前${shadowStateText(pos.decision_state)}，浮动盈亏 ${signedPct(pos.pnl_ratio)}，持有 ${pos.hold_days ?? 0} 天。`
}
const touchstoneTitle = computed(() => {
  const result = touchstone.result || {}
  const name = result.name || result.resolved_symbol?.name || result.snapshot?.name || ''
  return name ? `${name} ${result.symbol || ''}`.trim() : result.symbol || '--'
})
const stepStateText = (state) => ({ idle: '待命', active: '运行中', done: '已完成', error: '异常' }[state] || state)
const verdictText = (value) => ({ PASS: '通过', HOLD: '观望', VETO: '否决', UNKNOWN: '未知', SKIPPED: '已跳过' }[String(value || '').toUpperCase()] || value || '--')
const patternText = (value) => ({
  strong_momentum: '强势动量',
  volume_breakout: '放量突破',
  weak_followthrough: '跟随偏弱',
  overheated_distribution: '高换手派发',
  UNKNOWN: '未知形态',
}[String(value || '')] || value || '--')
const touchstoneModeText = (mode) => ({
  L1_L2_L4_REFERENCE: '已完成快检与云端复核',
  L2_GATE_BLOCKED: '快检未通过，未进入云端复核',
  DATA_UNAVAILABLE: '数据不足，暂不判断',
}[String(mode || '')] || '验金石参考')
const touchstoneVerdictTone = computed(() => {
  const result = touchstone.result
  if (!result) return 'muted'
  const verdict = String(result.l4?.final_verdict || result.l3?.verdict || '').toUpperCase()
  if (result.mode === 'DATA_UNAVAILABLE') return 'warn'
  if (result.mode === 'L2_GATE_BLOCKED' || verdict === 'VETO') return 'veto'
  if (verdict === 'PASS') return 'pass'
  return 'warn'
})
const touchstoneScore = computed(() => {
  const result = touchstone.result
  if (!result) return '--'
  return result.l4?.final_score ?? result.l3?.audit_score ?? result.l2?.risk_score ?? '--'
})
const touchstoneDecision = computed(() => {
  const result = touchstone.result
  if (!result) return null
  const verdict = String(result.l4?.final_verdict || result.l3?.verdict || '').toUpperCase()
  const risk = result.l2?.risk_score ?? '--'
  const score = touchstoneScore.value
  const pattern = patternText(result.l2?.pattern)
  const l4Reason = localizeText(result.l4?.veto_reason || result.l4?.recommendation || '').trim()
  const sparse = Boolean(result.snapshot?.data_sparse)
  const reasons = []
  const risks = []
  let title = '建议先观察'
  let summary = '当前信号还不够清晰，适合放入观察列表，等待完整审计或模拟盘验证。'
  let action = '暂不急于动作，后续用今日标的和模拟盘结果交叉验证。'

  if (result.mode === 'DATA_UNAVAILABLE') {
    title = '暂不判断'
    summary = '没有拿到足够的行情快照，验金石不能给出有效参考。'
    reasons.push('本地快照中没有找到这个标的，可能是代码、名称或数据同步范围问题。')
    risks.push('这不是质量否决，也不能据此判断标的好坏。')
    action = '先核对名称或六位代码；等数据同步后再测一次。'
  } else if (result.mode === 'L2_GATE_BLOCKED') {
    title = '暂不深入'
    summary = '快检层已经发现明显问题，没有必要消耗云端复核额度。'
    reasons.push(`快检风险分为 ${risk}，形态判断为「${pattern}」。`)
    reasons.push('当前没有通过前置闸门，因此没有进入云端审查。')
    risks.push('短线结构或基础数据不够理想，继续分析容易形成误判。')
    action = '只保留观察，不进入模拟盘候选。'
  } else if (verdict === 'PASS') {
    title = '可以继续观察'
    summary = '快检与云端复核都没有给出硬性否决，可以作为候选继续跟踪。'
    reasons.push(`快检风险分为 ${risk}，形态判断为「${pattern}」。`)
    reasons.push(`云端复核参考分为 ${score}，当前倾向通过。`)
    risks.push('这是单票即时参考，不等同于正式审计入选。')
    action = '可放入观察池，等待今日标的或模拟盘进一步验证。'
  } else if (verdict === 'VETO') {
    title = '建议回避'
    summary = '云端复核给出否决或高风险信号，当前不适合继续推进。'
    reasons.push(`快检风险分为 ${risk}，形态判断为「${pattern}」。`)
    reasons.push(l4Reason || `云端复核参考分为 ${score}，结论偏负面。`)
    risks.push('继续追踪容易受到单点消息或情绪驱动影响。')
    action = '暂不进入观察池，除非后续完整审计出现新的正向证据。'
  } else {
    title = '谨慎观望'
    summary = '系统没有给出明确通过，优势还不够稳定。'
    reasons.push(`快检风险分为 ${risk}，形态判断为「${pattern}」。`)
    reasons.push(`云端复核参考分为 ${score}，当前更偏观望。`)
    risks.push('信号强度不足，容易出现看似有机会但胜率不清晰的情况。')
    action = '先不进入模拟盘，等放量、趋势或完整审计确认。'
  }

  if (sparse) risks.push('本地日频快照不完整，本次参考置信度需要打折。')
  if (l4Reason && !reasons.includes(l4Reason) && verdict !== 'VETO') reasons.push(l4Reason)
  return { title, summary, reasons, risks, action }
})

const setSteps = (states) => {
  touchstone.steps = touchstone.steps.map((step) => ({ ...step, state: states[step.id] || step.state }))
}

const refreshHeartbeat = async () => {
  const start = performance.now()
  heartbeat.loading = true
  try {
    const data = await get('/health')
    heartbeat.ok = String(data?.status || '').toLowerCase() === 'alive'
    heartbeat.timestamp = data?.timestamp || ''
    heartbeat.latestTradeDate = data?.latest_trade_date || ''
    heartbeat.stockCount = data?.stock_count || 0
    heartbeat.dbSizeMb = data?.db_size_mb || 0
    heartbeat.error = ''
  } catch (error) {
    heartbeat.ok = false
    heartbeat.error = error.message
  } finally {
    heartbeat.latency = Math.round(performance.now() - start)
    heartbeat.loading = false
  }
}

const watchtowerAuthConfig = (promptIfMissing = true) => {
  const key = promptIfMissing ? getTouchstoneKey() : getSavedTouchstoneKey()
  return key ? { headers: { 'X-Watchtower-Key': key } } : null
}

const fetchToday = async () => {
  today.loading = true
  today.error = ''
  try {
    const auth = watchtowerAuthConfig()
    if (!auth) throw new Error('需要访问口令后才能查看烛龙终端。')
    const status = await get('/api/v1/system/status', auth)
    const tradeDate = status?.latest_audit_date || new Date().toISOString().slice(0, 10)
    const [funnel, approved, disagreements, portfolio] = await Promise.all([
      get('/api/stats/funnel', { ...auth, params: { trade_date: tradeDate } }),
      get('/api/v1/audits/latest', { ...auth, params: { trade_date: tradeDate, verdict: 'PASS', limit: 20 } }),
      get('/api/v1/audits/disagreements', { ...auth, params: { trade_date: tradeDate, limit: 12 } }),
      get('/api/v1/shadow/portfolio', auth).catch(() => null),
    ])
    today.tradeDate = tradeDate
    today.state = status?.state || ''
    today.message = status?.message || ''
    today.funnel = funnel
    today.approved = Array.isArray(approved) ? approved : []
    today.disagreements = disagreements
    if (portfolio) shadow.data = portfolio
    if (selectedAuditSymbol.value && !today.approved.some((item) => item.symbol === selectedAuditSymbol.value)) selectedAuditSymbol.value = ''
  } catch (error) {
    if (/口令|401/.test(error.message)) {
      window.localStorage.removeItem(touchstoneKeyStorage)
      touchstoneAuthorized.value = false
    }
    today.error = error.message
  } finally {
    today.loading = false
  }
}

const getSavedTouchstoneKey = () => window.localStorage.getItem(touchstoneKeyStorage) || ''
const loadTouchstoneHistory = () => {
  try {
    const rows = JSON.parse(window.localStorage.getItem(touchstoneHistoryStorage) || '[]')
    touchstoneHistory.value = Array.isArray(rows) ? rows.slice(0, 8) : []
  } catch {
    touchstoneHistory.value = []
  }
}
const saveTouchstoneHistory = () => {
  window.localStorage.setItem(touchstoneHistoryStorage, JSON.stringify(touchstoneHistory.value.slice(0, 8)))
}
const addTouchstoneHistory = (result) => {
  const item = {
    symbol: result?.symbol || '',
    name: result?.name || result?.resolved_symbol?.name || '',
    title: touchstoneDecision.value?.title || touchstoneModeText(result?.mode),
    score: touchstoneScore.value,
    time: new Date().toLocaleString('zh-CN', { hour12: false }),
  }
  if (!item.symbol) return
  touchstoneHistory.value = [item, ...touchstoneHistory.value.filter((row) => row.symbol !== item.symbol)].slice(0, 8)
  saveTouchstoneHistory()
}
const useTouchstoneHistory = (item) => {
  touchstone.code = item?.name || item?.symbol || ''
  activeTab.value = 'touchstone'
}
const getTouchstoneKey = () => {
  const saved = getSavedTouchstoneKey()
  if (saved) {
    touchstoneAuthorized.value = true
    return saved
  }
  const typed = window.prompt('请输入烛龙终端访问口令') || ''
  const key = typed.trim()
  if (key) {
    window.localStorage.setItem(touchstoneKeyStorage, key)
    touchstoneAuthorized.value = true
  }
  return key
}

const runTouchstone = async () => {
  const symbol = normalizeSymbol(touchstone.code)
  if (!symbol) {
    touchstone.error = '请输入中文名或 6 位代码，例如 贵州茅台、600519。'
    return
  }
  const accessKey = getTouchstoneKey()
  if (!accessKey) {
    touchstone.error = '需要访问口令后才能使用验金石。'
    return
  }
  touchstone.busy = true
  touchstone.error = ''
  touchstone.result = null
  setSteps({ l1: 'active', l2: 'idle', l4: 'idle' })
  try {
    const request = post('/api/v2/touchstone/audit', { symbol }, { headers: { 'X-Watchtower-Key': accessKey } })
    await wait(300)
    setSteps({ l1: 'done', l2: 'active' })
    await wait(300)
    setSteps({ l2: 'done', l4: 'active' })
    touchstone.result = await request
    addTouchstoneHistory(touchstone.result)
    setSteps({ l4: 'done' })
  } catch (error) {
    if (/口令|401/.test(error.message)) {
      window.localStorage.removeItem(touchstoneKeyStorage)
      touchstoneAuthorized.value = false
    }
    touchstone.error = error.message
    setSteps({ l1: 'error', l2: 'error', l4: 'error' })
  } finally {
    touchstone.busy = false
  }
}

const fetchShadow = async () => {
  shadow.loading = true
  shadow.error = ''
  try {
    const auth = watchtowerAuthConfig()
    if (!auth) throw new Error('需要访问口令后才能查看模拟盘。')
    shadow.data = await get('/api/v1/shadow/portfolio', auth)
    await nextTick()
    drawPnlChart()
  } catch (error) {
    if (/口令|401/.test(error.message)) {
      window.localStorage.removeItem(touchstoneKeyStorage)
      touchstoneAuthorized.value = false
    }
    shadow.error = error.message
  } finally {
    shadow.loading = false
  }
}

const fetchQuota = async () => {
  quota.loading = true
  quota.error = ''
  try {
    const auth = watchtowerAuthConfig()
    if (!auth) throw new Error('需要访问口令后才能查看 AI 消耗。')
    quota.data = await get('/api/v1/telemetry/tokens', auth)
  } catch (error) {
    if (/口令|401/.test(error.message)) {
      window.localStorage.removeItem(touchstoneKeyStorage)
      touchstoneAuthorized.value = false
    }
    quota.error = error.message
  } finally {
    quota.loading = false
  }
}

const drawPnlChart = () => {
  if (!chartEl.value || activeTab.value !== 'shadow') return
  if (!pnlChart) pnlChart = echarts.init(chartEl.value, null, { renderer: 'canvas' })
  const positions = Array.isArray(shadow.data?.positions) ? shadow.data.positions : []
  pnlChart.setOption({
    animationDuration: 250,
    grid: { left: 6, right: 6, top: 8, bottom: 8 },
    xAxis: { type: 'category', show: false, data: positions.map(positionTitle) },
    yAxis: { type: 'value', show: false },
    series: [{ type: 'bar', data: positions.map((item) => Number((item.pnl_ratio * 100).toFixed(2))), barWidth: 18, itemStyle: { color: (params) => params.value >= 0 ? '#10B981' : '#E11D48', borderRadius: [4, 4, 0, 0] } }],
    tooltip: { trigger: 'axis', valueFormatter: (value) => `${value}%` },
  })
}

const refreshActive = async () => {
  if (activeTab.value === 'today') await fetchToday()
  if (activeTab.value === 'shadow') await fetchShadow()
  if (activeTab.value === 'quota') await fetchQuota()
}

watch(activeTab, refreshActive)

onMounted(async () => {
  session.setSession('mobile_observer', 'command_bridge')
  touchstoneAuthorized.value = Boolean(getSavedTouchstoneKey())
  loadTouchstoneHistory()
  await refreshHeartbeat()
  heartbeatTimer.value = setInterval(refreshHeartbeat, 15000)
  await fetchToday()
  window.addEventListener('resize', drawPnlChart)
})

onBeforeUnmount(() => {
  if (heartbeatTimer.value) clearInterval(heartbeatTimer.value)
  window.removeEventListener('resize', drawPnlChart)
  if (pnlChart) pnlChart.dispose()
})
</script>

<template>
  <main class="terminal-shell bg-abyss text-porcelain">
    <header class="terminal-header px-4 pt-4 pb-3">
      <div class="flex items-center justify-between gap-3">
        <div class="min-w-0 w-full px-16">
          <h1 class="terminal-title">烛龙量化终端</h1>
        </div>
        <button class="terminal-refresh" @click="refreshActive">刷新</button>
      </div>
      <div class="terminal-status mt-3 grid grid-cols-[1fr_auto] items-center gap-3 px-3 py-2">
        <div class="min-w-0">
          <p class="text-xs text-titan">{{ channelLabel }} {{ heartbeat.ok ? '在线' : '异常' }}</p>
          <p class="truncate text-[11px] text-titan/80">{{ channelHint }}</p>
        </div>
        <div class="flex items-center gap-2 text-xs" :class="channelTone === 'pass' ? 'text-pass' : channelTone === 'veto' ? 'text-veto' : 'text-amber-300'">
          <span class="h-2.5 w-2.5 rounded-full" :class="channelTone === 'pass' ? 'bg-pass' : channelTone === 'veto' ? 'bg-veto critical-beacon' : 'bg-amber-300'" />
          <span class="text-right"><span class="block font-semibold">{{ channelStatusText }}</span><span class="block text-[10px] text-titan">{{ heartbeat.loading ? '--' : `${heartbeat.latency}ms` }}</span></span>
        </div>
      </div>
    </header>


    <section class="terminal-content px-4 py-4">
      <Transition name="soft-slide" mode="out-in">
        <article v-if="activeTab === 'today'" key="today" class="space-y-3">
          <section class="panel-card p-4">
            <div class="flex items-start justify-between gap-3">
              <div>
                <p class="text-xs text-titan">最近交易日 {{ today.tradeDate || '--' }}</p>
                <h2 class="mt-1 text-lg font-semibold">今日标的</h2>
              </div>
              <span class="rounded-md px-2 py-1 text-xs" :class="auditTone === 'pass' ? 'bg-pass/15 text-pass' : auditTone === 'warn' ? 'bg-amber-400/15 text-amber-300' : auditTone === 'danger' ? 'bg-veto/15 text-veto' : 'bg-white/10 text-titan'">
                {{ today.loading ? '同步中' : today.approved.length ? '有标的' : '空仓' }}
              </span>
            </div>
            <p v-if="today.error" class="mt-3 text-sm text-veto">{{ today.error }}</p>
            <template v-else>
              <div class="mt-4 grid grid-cols-4 gap-2 text-center">
                <div class="metric"><span>L1</span><strong>{{ fmtInt(today.funnel?.l1) }}</strong></div>
                <div class="metric"><span>L2</span><strong>{{ fmtInt(today.funnel?.l2) }}</strong></div>
                <div class="metric"><span>L3</span><strong>{{ fmtInt(today.funnel?.l3) }}</strong></div>
                <div class="metric accent"><span>L4</span><strong>{{ fmtInt(today.funnel?.l4) }}</strong></div>
              </div>
              <div class="mt-3 flex items-center justify-between text-sm"><span class="text-titan">通过率</span><strong>{{ passRate }}%</strong></div>
              <p class="mt-2 text-sm leading-relaxed text-titan">{{ todaySummary }}</p>
              <p v-if="tradeDateHint" class="mt-2 rounded-lg border border-gold/25 bg-gold/10 px-3 py-2 text-xs leading-relaxed text-gold">{{ tradeDateHint }}</p>
            </template>
          </section>

          <section v-if="today.approved.length" class="space-y-2">
            <div v-for="item in today.approved" :key="item.task_id || item.symbol" class="panel-card p-3">
              <button class="w-full text-left" @click="toggleAuditDetail(item)">
                <div class="flex items-start justify-between gap-3">
                  <div><p class="font-semibold">{{ auditTitle(item) }}</p><p class="text-xs text-titan">{{ item.symbol }} &middot; {{ item.trade_date }}</p></div>
                  <div class="text-right"><p class="text-pass font-semibold">{{ auditScore(item) }}</p><p class="text-[11px] text-titan">{{ selectedAuditSymbol === item.symbol ? '收起' : '详情' }}</p></div>
                </div>
                <p class="mt-2 line-clamp-2 text-sm leading-relaxed text-titan">{{ auditDetailSummary(item) }}</p>
              </button>
              <div v-if="selectedAuditSymbol === item.symbol" class="mt-3 space-y-3 border-t border-white/10 pt-3">
                <div class="grid grid-cols-3 gap-2 text-xs">
                  <div class="metric"><span>快检风险</span><strong>{{ item.l2_risk_score ?? '--' }}</strong></div>
                  <div class="metric"><span>中层审计</span><strong>{{ item.l3_audit_score ?? '--' }}</strong></div>
                  <div class="metric accent"><span>参考分</span><strong>{{ auditScore(item) }}</strong></div>
                </div>
                <div>
                  <p class="text-xs font-semibold text-titan">通过依据</p>
                  <div class="mt-2 space-y-2"><p v-for="reason in auditDetailReasons(item)" :key="reason" class="rounded-lg bg-white/[0.04] px-3 py-2 text-sm leading-relaxed text-porcelain">{{ reason }}</p></div>
                </div>
                <div>
                  <p class="text-xs font-semibold text-titan">风险提醒</p>
                  <div class="mt-2 space-y-2"><p v-for="risk in auditDetailRisks(item)" :key="risk" class="rounded-lg bg-white/[0.04] px-3 py-2 text-sm leading-relaxed text-titan">{{ risk }}</p></div>
                </div>
                <div class="rounded-lg border border-gold/25 bg-gold/10 p-3">
                  <p class="text-xs font-semibold text-gold">模拟盘状态</p>
                  <p class="mt-1 text-sm leading-relaxed text-porcelain">{{ auditShadowText(selectedAuditShadow) }}</p>
                </div>
              </div>
            </div>
          </section>

          <section class="panel-card p-4">
            <button class="w-full text-left" @click="showDisagreements = !showDisagreements">
              <div class="flex items-center justify-between gap-3">
                <div>
                  <p class="text-xs text-titan">L2/L3/L4 校准</p>
                  <h3 class="mt-1 text-base font-semibold">分歧面板</h3>
                </div>
                <span class="rounded-md bg-white/10 px-2 py-1 text-xs text-titan">{{ showDisagreements ? '收起' : `${today.disagreements?.items?.length || 0} 条` }}</span>
              </div>
            </button>
            <template v-if="showDisagreements">
              <div v-if="disagreementStats.length" class="mt-3 grid grid-cols-2 gap-2">
                <div v-for="stat in disagreementStats" :key="stat.label" class="metric">
                  <span>{{ stat.label }}</span>
                  <strong :class="stat.tone === 'pass' ? 'text-pass' : stat.tone === 'veto' ? 'text-veto' : 'text-amber-300'">{{ stat.value }}</strong>
                </div>
              </div>
              <p v-else class="mt-3 text-sm leading-relaxed text-titan">最新审计未发现关键分歧，漏斗裁决方向一致。</p>
              <div v-if="today.disagreements?.items?.length" class="mt-3 space-y-2">
                <div v-for="item in today.disagreements.items.slice(0, 4)" :key="item.task_id || item.symbol" class="rounded-lg bg-white/[0.04] p-3">
                  <div class="flex items-start justify-between gap-2">
                    <div><p class="text-sm font-semibold">{{ auditFullTitle(item) }}</p><p class="text-[11px] text-titan">{{ disagreementTitle(item) }}</p></div>
                    <div class="text-right text-xs text-titan"><p>L2 {{ item.l2_risk_score ?? '--' }}</p><p>{{ verdictText(item.l3_verdict) }} / {{ verdictText(item.l4_final_verdict) }}</p></div>
                  </div>
                  <p class="mt-2 line-clamp-2 text-xs leading-relaxed text-titan">{{ disagreementDetailText(item) }}</p>
                </div>
              </div>
            </template>
          </section>

          <section v-if="!today.approved.length" class="panel-card p-4">
            <div class="flex items-start justify-between gap-3">
              <div>
                <p class="text-xs text-titan">空仓解释</p>
                <h3 class="mt-1 text-base font-semibold">{{ emptyPositionInsight.title }}</h3>
              </div>
              <span class="rounded-md bg-amber-400/15 px-2 py-1 text-xs text-amber-300">无标的</span>
            </div>
            <p class="mt-3 text-sm leading-relaxed text-titan">{{ emptyPositionInsight.summary }}</p>
            <div v-if="emptyReasons.length" class="mt-3 space-y-2">
              <div v-for="reason in emptyReasons" :key="reason.label" class="flex items-center justify-between rounded-lg bg-white/[0.04] px-3 py-2 text-sm"><span class="text-titan">{{ reason.label }}</span><strong>{{ reason.value }}</strong></div>
            </div>
            <div class="mt-3 rounded-lg border border-gold/25 bg-gold/10 p-3">
              <p class="text-xs font-semibold text-gold">操作建议</p>
              <p class="mt-1 text-sm leading-relaxed text-porcelain">{{ emptyPositionInsight.action }}</p>
            </div>
          </section>
        </article>

        <article v-else-if="activeTab === 'touchstone'" key="touchstone" class="space-y-3">
          <section class="panel-card p-4">
            <p class="text-xs text-titan">单票逻辑验证</p>
            <h2 class="mt-1 text-lg font-semibold">验金石</h2>
            <div class="mt-4 grid grid-cols-[1fr_auto] gap-2">
              <input v-model.trim="touchstone.code" inputmode="text" maxlength="20" placeholder="中文名或6位代码" class="terminal-input h-11 px-3 text-base outline-none" />
              <button class="terminal-primary h-11 px-4 text-sm disabled:opacity-40" :disabled="touchstone.busy" @click="runTouchstone">{{ touchstone.busy ? '审查中' : '验证' }}</button>
            </div>
            <p v-if="touchstone.error" class="mt-2 text-sm text-veto">{{ touchstone.error }}</p>
            <p v-else-if="touchstone.busy" class="mt-2 text-sm leading-relaxed text-titan">正在读取行情快照并做快速风控；通过后会进入云端复核，通常数秒完成，模型复核忙时会自动降级为快检。</p>
            <p v-else class="mt-2 text-xs leading-relaxed text-titan">验金石会触发云端复核，仅授权访问可用；其他看板不受影响。</p>
          </section>
          <section v-if="touchstoneHistory.length" class="panel-card p-4">
            <div class="flex items-center justify-between gap-3"><div><p class="text-xs text-titan">本机记录</p><h3 class="mt-1 text-base font-semibold">最近验金石</h3></div><span class="rounded-md bg-white/10 px-2 py-1 text-xs text-titan">{{ touchstoneHistory.length }} 条</span></div>
            <div class="mt-3 space-y-2"><button v-for="item in touchstoneHistory" :key="`${item.symbol}-${item.time}`" class="w-full rounded-lg bg-white/[0.04] px-3 py-2 text-left" @click="useTouchstoneHistory(item)"><div class="flex items-start justify-between gap-3"><div><p class="text-sm font-semibold">{{ item.name || item.symbol }}</p><p class="text-[11px] text-titan">{{ item.symbol }} · {{ item.time }}</p></div><div class="text-right"><p class="text-sm font-semibold text-gold">{{ item.score }}</p><p class="text-[11px] text-titan">{{ item.title }}</p></div></div></button></div>
          </section>
          <section class="panel-card p-4">
            <div class="space-y-2">
              <div v-for="step in touchstone.steps" :key="step.id" class="flex items-center justify-between rounded-lg border px-3 py-2 text-sm" :class="step.state === 'active' ? 'border-sky-300/50 bg-sky-400/10 text-sky-200' : step.state === 'done' ? 'border-pass/40 bg-pass/10 text-pass' : step.state === 'error' ? 'border-veto/40 bg-veto/10 text-veto' : 'border-white/10 bg-white/[0.04] text-titan'"><span>{{ step.label }}</span><span>{{ stepStateText(step.state) }}</span></div>
            </div>
          </section>
          <section v-if="touchstone.result && touchstoneDecision" class="panel-card p-4">
            <div class="flex items-start justify-between gap-3">
              <div class="min-w-0">
                <p class="text-xs text-titan">{{ touchstoneTitle }}</p>
                <h3 class="mt-1 text-xl font-semibold">{{ touchstoneDecision.title }}</h3>
                <p class="mt-1 text-xs text-titan">{{ touchstoneModeText(touchstone.result.mode) }}</p>
              </div>
              <div class="rounded-lg px-3 py-2 text-right" :class="touchstoneVerdictTone === 'pass' ? 'bg-pass/15 text-pass' : touchstoneVerdictTone === 'veto' ? 'bg-veto/15 text-veto' : 'bg-amber-400/15 text-amber-300'">
                <p class="text-[11px]">参考分</p>
                <strong class="text-2xl">{{ touchstoneScore }}</strong>
              </div>
            </div>

            <div class="mt-4 rounded-lg border border-gold/25 bg-gold/10 p-3">
              <p class="text-xs font-semibold text-gold">结论</p>
              <p class="mt-1 text-sm leading-relaxed text-porcelain">{{ touchstoneDecision.summary }}</p>
            </div>

            <div class="mt-3 grid grid-cols-2 gap-2 text-sm">
              <div class="metric"><span>快检形态</span><strong>{{ patternText(touchstone.result.l2?.pattern) }}</strong></div>
              <div class="metric"><span>云端意见</span><strong>{{ verdictText(touchstone.result.l4?.final_verdict || touchstone.result.l3?.verdict) }}</strong></div>
            </div>

            <div class="mt-4 space-y-3">
              <div>
                <p class="text-xs font-semibold text-titan">主要理由</p>
                <div class="mt-2 space-y-2"><p v-for="item in touchstoneDecision.reasons" :key="item" class="rounded-lg bg-white/[0.04] px-3 py-2 text-sm leading-relaxed text-porcelain">{{ item }}</p></div>
              </div>
              <div>
                <p class="text-xs font-semibold text-titan">风险提醒</p>
                <div class="mt-2 space-y-2"><p v-for="item in touchstoneDecision.risks" :key="item" class="rounded-lg bg-white/[0.04] px-3 py-2 text-sm leading-relaxed text-titan">{{ item }}</p></div>
              </div>
              <div class="rounded-lg border border-white/10 bg-white/[0.04] p-3">
                <p class="text-xs font-semibold text-titan">操作建议</p>
                <p class="mt-1 text-sm leading-relaxed text-porcelain">{{ touchstoneDecision.action }}</p>
              </div>
            </div>

            <div v-if="touchstone.result.diagnosis?.length" class="mt-3 space-y-2">
              <div v-for="item in touchstone.result.diagnosis" :key="item.code" class="rounded-lg bg-white/[0.04] p-3"><p class="text-sm font-medium">{{ item.title }}</p><p class="mt-1 text-xs leading-relaxed text-titan">{{ item.detail }}</p></div>
            </div>
          </section>
        </article>

        <article v-else-if="activeTab === 'shadow'" key="shadow" class="space-y-3">
          <section class="panel-card p-4">
            <div class="flex items-start justify-between gap-3"><div><p class="text-xs text-titan">模拟交易系统</p><h2 class="mt-1 text-lg font-semibold">模拟持仓与盈亏</h2></div><span class="rounded-md bg-white/10 px-2 py-1 text-xs text-titan">{{ shadow.data?.active_positions || 0 }}/{{ shadow.data?.max_positions || 4 }}</span></div>
            <p v-if="shadow.error" class="mt-3 text-sm text-veto">{{ shadow.error }}</p>
            <template v-else><div class="mt-4 grid grid-cols-2 gap-2"><div class="metric"><span>组合盈亏</span><strong :class="asNumber(shadow.data?.pnl_amount) >= 0 ? 'text-pass' : 'text-veto'">{{ signedMoney(shadow.data?.pnl_amount) }}</strong></div><div class="metric"><span>收益率</span><strong :class="asNumber(shadow.data?.pnl_ratio) >= 0 ? 'text-pass' : 'text-veto'">{{ signedPct(shadow.data?.pnl_ratio) }}</strong></div><div class="metric"><span>持仓市值</span><strong>{{ fmtMoney(shadow.data?.market_value) }}</strong></div><div class="metric"><span>现金余额</span><strong>{{ fmtMoney(shadow.data?.cash_reserve) }}</strong></div></div><div ref="chartEl" class="mt-4 h-24 rounded-lg border border-white/10 bg-black/20" /></template>
          </section>
          <section v-if="shadow.data?.positions?.length" class="space-y-2">
            <div v-for="pos in shadow.data.positions" :key="`${pos.symbol}-${pos.trade_date}`" class="panel-card p-3">
              <button class="w-full text-left" @click="toggleShadowDetail(pos)">
                <div class="flex items-start justify-between gap-3">
                  <div><p class="font-semibold">{{ positionTitle(pos) }}</p><p class="text-xs text-titan">{{ pos.trade_date }} 入场 / {{ pos.hold_days ?? 0 }} 天</p></div>
                  <div class="text-right"><p class="font-semibold" :class="pos.pnl_ratio >= 0 ? 'text-pass' : 'text-veto'">{{ signedPct(pos.pnl_ratio) }}</p><p class="text-[11px] text-titan">{{ selectedShadowSymbol === pos.symbol ? '收起' : '详情' }}</p></div>
                </div>
                <p class="mt-2 text-sm leading-relaxed text-titan">{{ shadowPositionSummary(pos) }}</p>
              </button>
              <div v-if="selectedShadowSymbol === pos.symbol" class="mt-3 space-y-3 border-t border-white/10 pt-3">
                <div class="grid grid-cols-3 gap-2 text-xs">
                  <div class="metric"><span>持股</span><strong>{{ fmtInt(pos.qty) }}</strong></div>
                  <div class="metric"><span>成本</span><strong>{{ pos.entry_price }}</strong></div>
                  <div class="metric accent"><span>现价</span><strong>{{ pos.current_price }}</strong></div>
                </div>
                <div class="grid grid-cols-2 gap-2 text-xs">
                  <div class="metric"><span>持仓状态</span><strong :class="shadowDecisionTone(pos) === 'veto' ? 'text-veto' : shadowDecisionTone(pos) === 'warn' ? 'text-amber-300' : 'text-pass'">{{ shadowStateText(pos.decision_state) }}</strong></div>
                  <div class="metric"><span>质量标签</span><strong>{{ shadowQualityText(pos.quality_label) }}</strong></div>
                </div>
                <div>
                  <p class="text-xs font-semibold text-titan">入场依据</p>
                  <div class="mt-2 space-y-2"><p v-for="item in shadowPositionReasons(pos)" :key="item" class="rounded-lg bg-white/[0.04] px-3 py-2 text-sm leading-relaxed text-porcelain">{{ item }}</p></div>
                </div>
                <div>
                  <p class="text-xs font-semibold text-titan">风险提醒</p>
                  <div class="mt-2 space-y-2"><p v-for="item in shadowPositionRisks(pos)" :key="item" class="rounded-lg bg-white/[0.04] px-3 py-2 text-sm leading-relaxed text-titan">{{ item }}</p></div>
                </div>
                <div class="rounded-lg border border-gold/25 bg-gold/10 p-3">
                  <p class="text-xs font-semibold text-gold">下一步建议</p>
                  <p class="mt-1 text-sm leading-relaxed text-porcelain">{{ shadowPositionAction(pos) }}</p>
                </div>
              </div>
            </div>
          </section>
          <section v-else class="panel-card p-4 text-sm text-titan">{{ shadowEmptyText }}</section>
          <section v-if="shadow.data?.performance" class="panel-card p-4">
            <div class="flex items-start justify-between gap-3">
              <div>
                <p class="text-xs text-titan">L4 入场后验证</p>
                <h3 class="mt-1 text-base font-semibold">历史战绩回顾</h3>
              </div>
              <span class="rounded-md bg-white/10 px-2 py-1 text-xs text-titan">{{ shadow.data.performance.closed_trades || 0 }} 笔已平仓</span>
            </div>
            <div class="mt-4 grid grid-cols-2 gap-2">
              <div class="metric accent"><span>平仓胜率</span><strong>{{ fmtPct(shadow.data.performance.win_rate) }}</strong></div>
              <div class="metric"><span>总建仓</span><strong>{{ fmtInt(shadow.data.performance.total_positions) }}</strong></div>
              <div class="metric"><span>已实现盈亏</span><strong :class="asNumber(shadow.data.performance.realized_pnl_amount) >= 0 ? 'text-pass' : 'text-veto'">{{ signedMoney(shadow.data.performance.realized_pnl_amount) }}</strong></div>
              <div class="metric"><span>总浮动结果</span><strong :class="asNumber(shadow.data.performance.total_pnl_amount) >= 0 ? 'text-pass' : 'text-veto'">{{ signedMoney(shadow.data.performance.total_pnl_amount) }}</strong></div>
            </div>
            <div class="mt-3 grid grid-cols-2 gap-2 text-xs">
              <div class="metric"><span>盈利 / 亏损</span><strong>{{ fmtInt(shadow.data.performance.win_trades) }} / {{ fmtInt(shadow.data.performance.loss_trades) }}</strong></div>
              <div class="metric"><span>买入 / 卖出</span><strong>{{ fmtInt(shadow.data.performance.buy_count) }} / {{ fmtInt(shadow.data.performance.sell_count) }}</strong></div>
              <div class="metric"><span>最佳单笔</span><strong class="text-pass">{{ shadow.data.performance.best_trade_symbol ? signedPct(shadow.data.performance.best_trade_pnl_ratio) : '--' }}</strong></div>
              <div class="metric"><span>最差单笔</span><strong class="text-veto">{{ shadow.data.performance.worst_trade_symbol ? signedPct(shadow.data.performance.worst_trade_pnl_ratio) : '--' }}</strong></div>
            </div>
            <div v-if="shadow.data?.history_trades?.length" class="mt-4">
              <button class="flex w-full items-center justify-between rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-left text-sm" @click="showShadowHistory = !showShadowHistory">
                <span class="font-semibold">最近交易明细</span>
                <span class="text-xs text-titan">{{ showShadowHistory ? '收起' : `查看最近 ${Math.min(5, shadow.data.history_trades.length)} 笔` }}</span>
              </button>
              <div v-if="showShadowHistory" class="mt-2 space-y-2">
                <div v-for="trade in recentShadowHistory" :key="`${trade.symbol}-${trade.entry_date}-${trade.exit_date}`" class="rounded-lg bg-white/[0.04] p-3">
                  <div class="flex items-start justify-between gap-3">
                    <div class="min-w-0">
                      <p class="text-sm font-semibold">{{ historyTradeTitle(trade) }}</p>
                      <p class="text-[11px] text-titan">{{ trade.entry_date }} 入场 / {{ trade.exit_date || '--' }} 出场</p>
                    </div>
                    <div class="text-right">
                      <p class="text-sm font-semibold" :class="asNumber(trade.pnl_amount) >= 0 ? 'text-pass' : 'text-veto'">{{ signedPct(trade.pnl_ratio) }}</p>
                      <p class="text-[11px] text-titan">{{ signedMoney(trade.pnl_amount) }}</p>
                    </div>
                  </div>
                  <div class="mt-2 grid grid-cols-3 gap-2 text-xs">
                    <div class="metric"><span>数量</span><strong>{{ fmtInt(trade.qty) }}</strong></div>
                    <div class="metric"><span>买入</span><strong>{{ trade.entry_price }}</strong></div>
                    <div class="metric"><span>卖出</span><strong>{{ trade.exit_price }}</strong></div>
                  </div>
                  <p class="mt-2 text-xs leading-relaxed text-titan">{{ shadowSellRuleText(trade.sell_rule) }}。{{ localizeText(trade.sell_reason) }}</p>
                </div>
              </div>
            </div>
            <p v-else class="mt-3 text-sm leading-relaxed text-titan">还没有已平仓交易，胜率需要等第一批完整买卖闭环后再评估。</p>
          </section>
        </article>

        <article v-else key="quota" class="space-y-3">
          <section class="panel-card p-4"><p class="text-xs text-titan">云端状态</p><h2 class="mt-1 text-lg font-semibold">服务健康</h2><div class="mt-4 grid grid-cols-2 gap-2"><div class="metric"><span>{{ channelLabel }}</span><strong :class="heartbeat.ok ? 'text-pass' : 'text-veto'">{{ channelStatusText }}</strong></div><div class="metric"><span>延迟</span><strong>{{ heartbeat.loading ? '--' : `${heartbeat.latency}ms` }}</strong></div><div class="metric"><span>最新交易日</span><strong>{{ heartbeat.latestTradeDate || '--' }}</strong></div><div class="metric"><span>快照规模</span><strong>{{ fmtInt(heartbeat.stockCount) }}</strong></div></div><p class="mt-3 text-xs leading-relaxed text-titan">{{ channelHint }}</p></section>
          <section class="panel-card p-4"><p class="text-xs text-titan">估算口径</p><h2 class="mt-1 text-lg font-semibold">AI 消耗</h2><p v-if="quota.error" class="mt-3 text-sm text-veto">{{ quota.error }}</p><template v-else><div class="mt-4 grid grid-cols-2 gap-2"><div class="metric"><span>审计日</span><strong>{{ quota.data?.trade_date || '--' }}</strong></div><div class="metric"><span>总调用</span><strong>{{ touchstoneAuthorized ? fmtInt(quota.data?.total_calls) : '***' }}</strong></div><div class="metric accent"><span>估算消耗</span><strong>{{ touchstoneAuthorized ? `${quota.data?.total_est_cost_cny ?? 0} 元` : '***' }}</strong></div><div class="metric"><span>明细权限</span><strong :class="touchstoneAuthorized ? 'text-pass' : 'text-amber-300'">{{ touchstoneAuthorized ? '已授权' : '已隐藏' }}</strong></div></div><p v-if="!touchstoneAuthorized" class="mt-3 rounded-lg border border-gold/25 bg-gold/10 p-3 text-sm leading-relaxed text-gold">AI 消耗明细已隐藏。使用验金石并输入正确口令后，这里会自动显示。</p></template></section>
          <section v-if="touchstoneAuthorized" class="space-y-2"><div v-for="bucket in quotaCloudCost" :key="bucket.provider" class="panel-card p-3"><div class="flex items-center justify-between gap-3"><div class="min-w-0"><p class="truncate text-sm font-medium">{{ bucket.provider }}</p><p class="truncate text-xs text-titan">{{ bucket.model }}</p></div><div class="text-right"><p class="text-sm font-semibold">{{ bucket.est_cost_cny }} 元</p><p class="text-[11px] text-titan">{{ bucket.call_count }} 次调用</p></div></div></div></section>
        </article>
      </Transition>
    </section>
    <nav class="terminal-nav">
      <div class="nav-grid">
        <button v-for="tab in tabs" :key="tab.id" class="nav-button" :class="activeTab === tab.id ? 'active' : ''" @click="activeTab = tab.id">
          {{ tab.label }}
        </button>
      </div>
    </nav>
  </main>
</template>
