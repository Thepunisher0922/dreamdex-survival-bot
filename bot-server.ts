import dotenv from "dotenv";
dotenv.config({ override: true });

import { WebSocketServer, WebSocket } from "ws";
import * as sdk from "@somnia-chain/markets-sdk";
import { SomniaMarkets, isBinaryMarket, type PlaceOrderResult } from "@somnia-chain/markets-sdk";
import { createPublicClient, http, defineChain, erc20Abi } from "viem";
import { privateKeyToAccount } from "viem/accounts";

// --- 1. PRIVATE KEY & SHANNON TESTNET (50312) SETUP ---
function getNormalizedPrivateKey(): `0x${string}` | null {
  let raw = (process.env.PRIVATE_KEY || "").trim().replace(/["']/g, "");
  if (!raw || raw === "0x.." || raw.length < 64) return null;
  if (!raw.startsWith("0x")) raw = "0x" + raw;
  return raw.length === 66 ? (raw as `0x${string}`) : null;
}

export const somniaShannon = defineChain({
  id: 50312,
  name: "Somnia Shannon Testnet",
  nativeCurrency: { name: "Somnia Test Token", symbol: "STT", decimals: 18 },
  rpcUrls: {
    default: {
      http: [process.env.RPC_URL || "https://api.infra.testnet.somnia.network/"],
      webSocket: [process.env.WS_RPC_URL || "wss://api.infra.testnet.somnia.network/ws"],
    },
  },
  blockExplorers: {
    default: { name: "Shannon Explorer", url: "https://shannon-explorer.somnia.network" },
  },
  testnet: true,
});

const publicClient = createPublicClient({
  chain: somniaShannon,
  transport: http(somniaShannon.rpcUrls.default.http[0]),
});

// --- 2. GAME STATE & ACTIVE POSITION TRACKER ---
interface BotState {
  status: "ALIVE" | "STARVING" | "DEAD";
  brainMode: "HYBRID_LLM" | "LOCAL_QUANT";
  hpUsd: number;
  maxHpUsd: number;
  sttGas: number;
  hunger: number;
  winStreak: number;
  brierScore: number;
  activeMarket: string;
  lastThought: string;
  lastQuant: { upAsk: number; downAsk: number; obi: number; pUp: number; edge: number } | null;
  history: Array<{ time: string; hp: number; action: string; edge: number }>;
}

interface PendingPosition {
  marketId: `0x${string}`;
  symbol: string;
  outcomeSymbol: string;
  oppositeSymbol: string;
  side: "UP" | "DOWN";
  outcomeIndex: 0 | 1;
  stakeUsd: number;
  shares: number;
  entryPrice: number;
  prob: number;
  historyIndex: number;
  enteredAtMs: number;
}

const pendingPositions = new Map<`0x${string}`, PendingPosition>();
const tradedMarketIds = new Set<string>(); // Prevents re-trading the same 15m window multiple times!
const redeemedMarketIds = new Set<string>();
let isTickRunning = false; // Mutex lock: prevents overlapping ticks & double-buying!

const startingHp = Number(process.env.STARTING_HP || 500.0);
const isDryRun = process.env.DRY_RUN === "true";
const MIN_HP_ALIVE = 0.50;
const MAX_STAKE_USD = 5.00; // Hard cap: 5.00 tUSDC max per trade

const state: BotState = {
  status: "ALIVE",
  brainMode: "LOCAL_QUANT",
  hpUsd: startingHp,
  maxHpUsd: startingHp,
  sttGas: 0,
  hunger: 0,
  winStreak: 0,
  brierScore: 0.215,
  activeMarket: "Scanning 15m BTC Windows on Shannon...",
  lastThought: "Booting 15m Sniper (TP +25% > SL -15% True-Mid | Anti-Wick Protection)...",
  lastQuant: null,
  history: [
    { time: new Date().toLocaleTimeString(), hp: startingHp, action: "BOT SPAWNED (15m Anti-Wick Sniper)", edge: 0 },
  ],
};

let exchange: SomniaMarkets | null = null;
let botAddress: `0x${string}` | null = null;
let collateralAddress: `0x${string}` | null = null;
let collateralDecimals = 6;

// --- 3. WEBSOCKET SERVER (Port 8080) ---
const wss = new WebSocketServer({ port: 8080 });
console.log("✅ WebSocket HUD Server running on ws://localhost:8080");

function broadcastState(event: string, payload?: unknown) {
  const msg = JSON.stringify({ event, state, payload, ts: Date.now() });
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

wss.on("connection", (ws) => {
  ws.send(JSON.stringify({ event: "INIT", state, ts: Date.now() }));
  ws.on("message", async (raw) => {
    try {
      const cmd = JSON.parse(raw.toString());
      if (cmd.type === "SIMULATE_TICK") await runBotTick();
      if (cmd.type === "REVIVE") {
        await syncOnChainBalance();
        state.status = "ALIVE";
        state.hunger = 0;
        broadcastState("REVIVED");
      }
    } catch {}
  });
});

// --- 4. SYNC ON-CHAIN tUSDC BALANCE ---
async function syncOnChainBalance(): Promise<number> {
  if (!botAddress || !collateralAddress) return state.hpUsd;
  try {
    const [sttRaw, balRaw, dec] = await Promise.all([
      publicClient.getBalance({ address: botAddress }),
      publicClient.readContract({
        address: collateralAddress,
        abi: erc20Abi,
        functionName: "balanceOf",
        args: [botAddress],
      }),
      publicClient.readContract({
        address: collateralAddress,
        abi: erc20Abi,
        functionName: "decimals",
      }).catch(() => 6),
    ]);

    collateralDecimals = Number(dec);
    state.sttGas = Number((Number(sttRaw) / 1e18).toFixed(4));
    const realTusdc = Number((Number(balRaw) / Math.pow(10, collateralDecimals)).toFixed(2));

    state.hpUsd = realTusdc;
    state.maxHpUsd = Math.max(state.maxHpUsd, realTusdc);
    console.log(`📊 [Wallet] ${botAddress} | Liquid tUSDC: ${realTusdc} | Gas: ${state.sttGas} STT`);
    return realTusdc;
  } catch {
    return state.hpUsd;
  }
}

// --- 5. ON-CHAIN REDEEMER ---
async function redeemPositionOnChain(
  marketId: `0x${string}`,
  symbolOrRef: string,
  outcomeSymbol: string,
  outcomeIdx: number,
  sharesAmount: number
): Promise<boolean> {
  if (redeemedMarketIds.has(marketId)) return false;
  const ex: any = exchange;
  const client: any = exchange?.client;
  const trader: any = ex?.trader || client?.trader;
  const marketObj = ex?.markets?.[symbolOrRef];

  const attempts: Array<{ name: string; fn: () => Promise<any> }> = [
    { name: "ex.redeem(outcomeSymbol, shares)", fn: () => ex?.redeem?.(outcomeSymbol, sharesAmount) },
    { name: "ex.redeem(symbol, shares, {outcomeIdx})", fn: () => ex?.redeem?.(symbolOrRef, sharesAmount, { outcomeIdx }) },
    { name: "trader.redeem({marketId, outcomeIdx, amount, market})", fn: () => trader?.redeem?.({ marketId, outcomeIdx, amount: sharesAmount, market: marketObj }) },
    { name: "trader.redeemDirect(marketId, outcomeIdx)", fn: () => trader?.redeemDirect?.(marketId, outcomeIdx) },
  ];

  for (const a of attempts) {
    try {
      const res = await a.fn();
      if (res) {
        const tx = res?.transactionHash || res?.receipt?.transactionHash || res?.hash || "";
        console.log(`✅ CLAIMED WINNINGS for ${symbolOrRef} via ${a.name}! ${tx ? `Tx: ${tx}` : ""}`);
        redeemedMarketIds.add(marketId);
        await new Promise((r) => setTimeout(r, 2500));
        return true;
      }
    } catch {}
  }
  return false;
}

// --- 6. ANTI-WICK POSITION MANAGER (TP +25% > SL -15% TRUE MID) ---
async function manageOpenPositions() {
  if (!exchange || pendingPositions.size === 0) return;

  for (const [marketId, pos] of pendingPositions.entries()) {
    try {
      const onchain: any = await exchange.client.getMarketOnchain(marketId);

      // CASE A: Market still open -> Check True Value & Real Bid
      if (onchain.status === 1) {
        const [ourBook, oppBook, ta] = await Promise.all([
          exchange.fetchOrderBook(pos.outcomeSymbol, 5),
          exchange.fetchOrderBook(pos.oppositeSymbol, 5),
          fetchBinance15mTaAndLatency(),
        ]);

        const bestBid = ourBook?.bids?.[0]?.[0] || 0;
        const ourAsk = ourBook?.asks?.[0]?.[0] || 0;
        const oppAsk = oppBook?.asks?.[0]?.[0] || 0;

        // Calculate TRUE fair value of our token so a fake 0.25 bid gap NEVER tricks us into panic-selling!
        const impliedFromOpp = oppAsk > 0 ? Number((1 - oppAsk).toFixed(3)) : bestBid;
        const trueFairPrice = ourAsk > 0 && oppAsk > 0 ? Number(((ourAsk + impliedFromOpp) / 2).toFixed(3)) : impliedFromOpp;

        const trueRoiPct = ((trueFairPrice - pos.entryPrice) / pos.entryPrice) * 100;
        const bidRoiPct = bestBid > 0 ? ((bestBid - pos.entryPrice) / pos.entryPrice) * 100 : -100;
        const holdSeconds = Math.round((Date.now() - pos.enteredAtMs) / 1000);

        console.log(
          `   📈 Holding ${pos.outcomeSymbol} (${holdSeconds}s) | Entry: ${pos.entryPrice} | TrueFair: ${trueFairPrice} (${trueRoiPct >= 0 ? "+" : ""}${trueRoiPct.toFixed(1)}%) | BestBid: ${bestBid} (${bidRoiPct >= 0 ? "+" : ""}${bidRoiPct.toFixed(1)}%)`
        );

        // 1. TAKE-PROFIT: Real executable Bid gives >= +25% ROI (or Bid >= 0.78)
        const takeProfitHit = bestBid > 0 && (bidRoiPct >= 25 || bestBid >= 0.78);

        // 2. STOP-LOSS: Only after holding >= 30s, True Fair Price down <= -15%, BTC crossed strike against us,
        // AND bestBid is genuine (within 0.06 of trueFairPrice — NEVER sell into a 0.25 liquidity gap!)
        const btcReversedAgainstUs =
          ta !== null &&
          ((pos.side === "UP" && ta.strikeDeltaPct < -0.025 && ta.roc1mPct < 0) ||
            (pos.side === "DOWN" && ta.strikeDeltaPct > 0.025 && ta.roc1mPct > 0));

        const bidIsLiquidNotWick = bestBid >= 0.35 && Math.abs(trueFairPrice - bestBid) <= 0.06;
        const stopLossHit =
          holdSeconds >= 30 &&
          trueRoiPct <= -15 &&
          btcReversedAgainstUs &&
          bidIsLiquidNotWick;

        if (takeProfitHit || stopLossHit) {
          const exitReason = takeProfitHit ? "🎯 TAKE-PROFIT" : "🛡️ STOP-LOSS";
          console.log(`${exitReason} MID-WINDOW: Selling ${pos.shares} shares of ${pos.outcomeSymbol} @ ${bestBid}`);

          const beforeBal = state.hpUsd;
          try {
            const sellOrder = await exchange.createOrder(
              pos.outcomeSymbol,
              "limit",
              "sell",
              pos.shares,
              Number(Math.max(0.35, bestBid - 0.01).toFixed(2)),
              { timeInForce: "IOC" }
            );
            const { receipt } = sellOrder.info as PlaceOrderResult;
            console.log(`⛓️ Sell Tx: https://shannon-explorer.somnia.network/tx/${receipt.transactionHash}`);
          } catch {
            console.log(`ℹ️ Mid-window sell IOC did not fill, continuing to hold.`);
            continue;
          }

          const afterBal = await syncOnChainBalance();
          const proceeds = Number((afterBal - beforeBal).toFixed(2));
          if (proceeds <= 0.05) continue;

          const netPnl = Number((proceeds - pos.stakeUsd).toFixed(2));
          const won = netPnl >= 0;

          state.winStreak = won ? state.winStreak + 1 : 0;
          if (state.history[pos.historyIndex]) {
            state.history[pos.historyIndex].hp = afterBal;
            state.history[pos.historyIndex].action = `${exitReason} (${won ? "+" : ""}${netPnl} tUSDC) | ${pos.symbol} ${pos.side} (${pos.entryPrice} -> ${bestBid})`;
          }
          state.lastThought = `${exitReason} locked on ${pos.symbol} at ${bestBid} (${bidRoiPct.toFixed(1)}% ROI)!`;
          pendingPositions.delete(marketId);
          broadcastState(won ? "TRADE_WON" : "TRADE_LOST");
        }
        continue;
      }

      // CASE B: Market settled at expiry ($1.00 per share for winner!)
      const winningOutcome = Number(onchain.winningOutcome ?? onchain.result ?? -1);
      if (winningOutcome !== 0 && winningOutcome !== 1) continue;

      const won = winningOutcome === pos.outcomeIndex;
      if (won) {
        console.log(`🏆 WON ${pos.symbol} AT EXPIRY! Redeeming ${pos.shares} shares...`);
        await redeemPositionOnChain(marketId, pos.symbol, pos.outcomeSymbol, pos.outcomeIndex, pos.shares);
      }

      const newBalance = await syncOnChainBalance();
      const expectedProfit = Number((pos.shares - pos.stakeUsd).toFixed(2));

      state.winStreak = won ? state.winStreak + 1 : 0;
      const outcomeError = Math.pow(pos.prob - (won ? 1 : 0), 2);
      state.brierScore = Number((0.85 * state.brierScore + 0.15 * outcomeError).toFixed(3));

      if (state.history[pos.historyIndex]) {
        state.history[pos.historyIndex].hp = newBalance;
        state.history[pos.historyIndex].action = won
          ? `✅ WIN +${expectedProfit} tUSDC | ${pos.symbol} ${pos.side} @ ${pos.entryPrice}`
          : `❌ LOSS -${pos.stakeUsd.toFixed(2)} tUSDC | ${pos.symbol} ${pos.side} @ ${pos.entryPrice}`;
      }

      pendingPositions.delete(marketId);
      broadcastState(won ? "TRADE_WON" : "TRADE_LOST");
    } catch (err: any) {
      console.warn(`⚠️ Position check notice: ${err.message?.slice(0, 80)}`);
    }
  }
}

// --- 7. TRUE 15m-ONLY EXACT STRIKE Z-SCORE + TA ENGINE ---
interface QuantMetrics {
  upAsk: number;
  downAsk: number;
  orderBookImbalance: number;
  spread: number;
}

interface Ta15mSignal {
  livePrice: number;
  strike15mOpen: number;
  strikeDeltaPct: number;
  roc1mPct: number;
  roc3mPct: number;
  vol1mPct: number;
  ema5: number;
  ema13: number;
  rsi7: number;
}

function normalCdf(z: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp((-z * z) / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  return z > 0 ? 1 - p : p;
}

// Strictly matches clean 15-minute UTC boundary markets (:00, :15, :30, :45) without 5m/1m hex suffixes
function get15mMinutesLeft(symbol: string): number | null {
  const match = symbol.match(/^BTC-0-\d{2}[A-Z]{3}\d{2}-(\d{2})(\d{2})\/tUSDC$/);
  if (!match) return null; // Rejects -0573 duplicate 5m/1m pools!

  const expHour = Number(match[1]);
  const expMin = Number(match[2]);
  if (expMin % 15 !== 0) return null;

  const now = new Date();
  const expiryUtc = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), expHour, expMin, 0)
  );
  let diffMin = (expiryUtc.getTime() - now.getTime()) / 60000;
  if (diffMin < -720) diffMin += 1440;
  return Number(diffMin.toFixed(2));
}

function calcEMA(prices: number[], period: number): number {
  const k = 2 / (period + 1);
  let ema = prices[0];
  for (let i = 1; i < prices.length; i++) ema = prices[i] * k + ema * (1 - k);
  return ema;
}

function calcRSI(prices: number[], period = 7): number {
  if (prices.length <= period) return 50;
  let gains = 0;
  let losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const diff = prices[i] - prices[i - 1];
    if (diff >= 0) gains += diff;
    else losses -= diff;
  }
  if (losses === 0) return 95;
  const rs = gains / period / (losses / period);
  return Number((100 - 100 / (1 + rs)).toFixed(1));
}

async function fetchBinance15mTaAndLatency(): Promise<Ta15mSignal | null> {
  try {
    const [klines1mRes, klines15mRes, tickerRes] = await Promise.all([
      fetch("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=20"),
      fetch("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=2"),
      fetch("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
    ]);

    const klines1m = (await klines1mRes.json()) as any[];
    const klines15m = (await klines15mRes.json()) as any[];
    const ticker = (await tickerRes.json()) as { price: string };
    if (!klines1m || klines1m.length < 15 || !klines15m || klines15m.length < 1) return null;

    const closes = klines1m.map((k) => Number(k[4]));
    const livePrice = Number(ticker?.price || closes[closes.length - 1]);
    closes[closes.length - 1] = livePrice;

    const strike15mOpen = Number(klines15m[klines15m.length - 1][1]);

    const returns: number[] = [];
    for (let i = 1; i < closes.length; i++) {
      returns.push(((closes[i] - closes[i - 1]) / closes[i - 1]) * 100);
    }
    const meanRet = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((a, b) => a + Math.pow(b - meanRet, 2), 0) / returns.length;
    const vol1mPct = Math.max(0.012, Math.sqrt(variance));

    return {
      livePrice,
      strike15mOpen,
      strikeDeltaPct: Number((((livePrice - strike15mOpen) / strike15mOpen) * 100).toFixed(4)),
      roc1mPct: Number((((livePrice - closes[closes.length - 2]) / closes[closes.length - 2]) * 100).toFixed(4)),
      roc3mPct: Number((((livePrice - closes[closes.length - 4]) / closes[closes.length - 4]) * 100).toFixed(4)),
      vol1mPct: Number(vol1mPct.toFixed(4)),
      ema5: calcEMA(closes.slice(-10), 5),
      ema13: calcEMA(closes, 13),
      rsi7: calcRSI(closes, 7),
    };
  } catch {
    return null;
  }
}

function computeQuantMetrics(upBook: any, downBook: any): QuantMetrics | null {
  const upAsk = upBook?.asks?.[0]?.[0];
  const downAsk = downBook?.asks?.[0]?.[0];
  if (upAsk === undefined || downAsk === undefined) return null;

  const upBidVol = (upBook.bids || []).slice(0, 5).reduce((acc: number, b: number[]) => acc + b[1], 0);
  const upAskVol = (upBook.asks || []).slice(0, 5).reduce((acc: number, a: number[]) => acc + a[1], 0);
  const totalVol = upBidVol + upAskVol;
  const obi = totalVol > 0 ? (upBidVol - upAskVol) / totalVol : 0;

  return {
    upAsk: Number(upAsk.toFixed(3)),
    downAsk: Number(downAsk.toFixed(3)),
    orderBookImbalance: Number(obi.toFixed(3)),
    spread: Number(Math.abs(1 - (upAsk + downAsk)).toFixed(3)),
  };
}

async function query15mBrain(
  marketSymbol: string,
  quant: QuantMetrics,
  minutesLeft: number
): Promise<{ side: "UP" | "DOWN" | "PASS"; prob: number; thought: string }> {
  // 1. SWEET-SPOT ENTRY CLOCK: Wait until at least 4.5 minutes of the 15m candle have formed (3.0m to 10.5m left)!
  // Never enter at 12m left when the 15m trend hasn't established yet.
  if (minutesLeft < 3.0 || minutesLeft > 10.5) {
    return {
      side: "PASS",
      prob: quant.upAsk,
      thought: `[15m | ${minutesLeft}m left] ${marketSymbol} waiting for 3.0m–10.5m mid-window sweet spot.`,
    };
  }

  // 2. NEVER BUY EXPENSIVE / SKEWED ODDS (> 0.62)
  // Buying at 0.40–0.60 gives +65% to +150% payout at expiry and easy +25% mid-window Take-Profit!
  if (quant.upAsk < 0.35 || quant.upAsk > 0.62 || quant.downAsk < 0.35 || quant.downAsk > 0.62) {
    return {
      side: "PASS",
      prob: quant.upAsk,
      thought: `[15m | ${minutesLeft}m left] ${marketSymbol} (${quant.upAsk}/${quant.downAsk}) outside 0.35–0.62 value zone.`,
    };
  }

  const ta = await fetchBinance15mTaAndLatency();
  if (!ta) return { side: "PASS", prob: 0.5, thought: "Binance feed unreachable." };

  const effectiveHorizonVol = ta.vol1mPct * Math.pow(Math.max(0.5, minutesLeft), 0.38);
  const momentumDrift = 0.45 * ta.roc1mPct + 0.30 * ta.roc3mPct + (ta.ema5 > ta.ema13 ? 0.018 : -0.018);
  const zScore = (ta.strikeDeltaPct + momentumDrift) / effectiveHorizonVol;

  const pUp = Math.min(0.88, Math.max(0.12, Number(normalCdf(zScore).toFixed(3))));
  const pDown = Number((1 - pUp).toFixed(3));

  const upEdge = Number((pUp - quant.upAsk).toFixed(3));
  const downEdge = Number((pDown - quant.downAsk).toFixed(3));
  const minEdge = 0.05; // Require +5.0% clear edge

  state.lastQuant = {
    upAsk: quant.upAsk,
    downAsk: quant.downAsk,
    obi: quant.orderBookImbalance,
    pUp,
    edge: Math.max(upEdge, downEdge),
  };

  const emaTag = ta.ema5 > ta.ema13 ? "BULL" : "BEAR";
  const thought = `[15m|${minutesLeft}m] ${marketSymbol} | Z:${zScore.toFixed(2)} StrikeΔ:${ta.strikeDeltaPct >= 0 ? "+" : ""}${ta.strikeDeltaPct}% 1m:${ta.roc1mPct >= 0 ? "+" : ""}${ta.roc1mPct}% 3m:${ta.roc3mPct >= 0 ? "+" : ""}${ta.roc3mPct}% EMA:${emaTag} RSI:${ta.rsi7} | P(Up):${(pUp * 100).toFixed(1)}% vs Ask(${quant.upAsk}/${quant.downAsk}) [Edge:${(Math.max(upEdge, downEdge) * 100).toFixed(1)}%]`;

  // CONFIRM UP: Edge >= +5%, BTC decisively above 15m strike (> +0.02%), both 1m and 3m momentum non-negative, EMA BULL, RSI 42-72
  if (
    upEdge >= minEdge &&
    ta.strikeDeltaPct >= 0.02 &&
    ta.roc1mPct >= 0 &&
    ta.roc3mPct > 0 &&
    ta.ema5 > ta.ema13 &&
    ta.rsi7 >= 42 &&
    ta.rsi7 <= 72
  ) {
    return { side: "UP", prob: pUp, thought };
  }

  // CONFIRM DOWN: Edge >= +5%, BTC decisively below 15m strike (< -0.02%), both 1m and 3m momentum non-positive, EMA BEAR, RSI 28-58
  if (
    downEdge >= minEdge &&
    ta.strikeDeltaPct <= -0.02 &&
    ta.roc1mPct <= 0 &&
    ta.roc3mPct < 0 &&
    ta.ema5 < ta.ema13 &&
    ta.rsi7 >= 28 &&
    ta.rsi7 <= 58
  ) {
    return { side: "DOWN", prob: pDown, thought };
  }

  return { side: "PASS", prob: pUp, thought };
}

// --- 8. STRICT 5.00 tUSDC MAX STAKE SIZING ---
function calculateSurvivalStakeUsd(prob: number, askPrice: number): number {
  const edge = Math.max(0.05, prob - askPrice);
  const scaled = 3.5 + (edge - 0.05) * 20;
  return Number(Math.min(MAX_STAKE_USD, Math.max(3.5, scaled)).toFixed(2));
}

// --- 9. MUTEX-LOCKED 15m BOT LOOP ---
async function runBotTick() {
  if (!exchange || isTickRunning) return; // Prevents overlapping ticks & double orders!
  isTickRunning = true;

  try {
    await manageOpenPositions();

    if (pendingPositions.size >= 1) {
      state.lastThought = `Managing active 15m position (TP: +25% Real Bid | Anti-Wick Hold)...`;
      broadcastState("WAITING_SETTLEMENT");
      return;
    }

    const markets = Object.values(await exchange.loadMarkets(true));

    const candidate15mMarkets = markets
      .filter((m: any) => m.active && isBinaryMarket(m.info) && m.symbol.startsWith("BTC"))
      .map((m: any) => ({ market: m, minutesLeft: get15mMinutesLeft(m.symbol) }))
      .filter((item): item is { market: any; minutesLeft: number } =>
        item.minutesLeft !== null && item.minutesLeft > 0.5 && item.minutesLeft <= 15.0
      )
      .sort((a, b) => a.minutesLeft - b.minutesLeft);

    console.log(`\n⚡ 15m-Only Scan: ${candidate15mMarkets.length} clean 15m window | Open Holds: ${pendingPositions.size}/1`);

    for (const { market: m, minutesLeft } of candidate15mMarkets) {
      const marketId = (m.info as any).marketId as `0x${string}`;

      // Never re-enter a 15m window we already traded!
      if (pendingPositions.has(marketId) || tradedMarketIds.has(marketId)) {
        console.log(`   🔒 ${m.symbol} (${minutesLeft}m left): Already traded this 15m window. Waiting for next window.`);
        continue;
      }

      const onchain: any = await exchange.client.getMarketOnchain(marketId);
      if (onchain.status !== 1) continue;

      const upSymbol = m.outcomes?.[0]?.symbol;
      const downSymbol = m.outcomes?.[1]?.symbol;
      if (!upSymbol || !downSymbol) continue;

      const [upBook, downBook] = await Promise.all([
        exchange.fetchOrderBook(upSymbol, 5),
        exchange.fetchOrderBook(downSymbol, 5),
      ]);

      const quant = computeQuantMetrics(upBook, downBook);
      if (!quant) continue;

      state.activeMarket = `${m.symbol} (${minutesLeft}m left)`;
      const decision = await query15mBrain(m.symbol, quant, minutesLeft);
      state.lastThought = decision.thought;
      console.log(`   🧠 ${decision.thought} => ${decision.side}`);

      if (decision.side === "PASS") {
        state.hunger = Math.min(100, state.hunger + 5);
        broadcastState("WINDOW_PASSED");
        continue;
      }

      const targetSymbol = decision.side === "UP" ? upSymbol : downSymbol;
      const oppositeSymbol = decision.side === "UP" ? downSymbol : upSymbol;
      const targetAsk = decision.side === "UP" ? quant.upAsk : quant.downAsk;
      const limitPrice = Number(Math.min(0.64, targetAsk + 0.02).toFixed(2));

      const desiredUsdStake = calculateSurvivalStakeUsd(decision.prob, targetAsk);
      const sharesToBuy = Number((desiredUsdStake / limitPrice).toFixed(2));

      if (!isDryRun) {
        console.log(
          `🚀 15m SNIPE (${minutesLeft}m left): ~${desiredUsdStake} tUSDC (${sharesToBuy} shares @ ${limitPrice}) on ${targetSymbol}`
        );
        const beforeBal = state.hpUsd;
        try {
          const order = await exchange.createOrder(
            targetSymbol,
            "limit",
            "buy",
            sharesToBuy,
            limitPrice,
            { timeInForce: "IOC" }
          );
          const { receipt } = order.info as PlaceOrderResult;
          console.log(`⛓️ Tx Hash: https://shannon-explorer.somnia.network/tx/${receipt.transactionHash}`);
        } catch (orderErr: any) {
          console.log(`ℹ️ Order skipped (${orderErr.message?.slice(0, 60)})`);
          continue;
        }

        const afterBal = await syncOnChainBalance();
        const actualSpent = Number(Math.max(0, beforeBal - afterBal).toFixed(2));
        if (actualSpent <= 0.05) continue;

        // Lock this marketId so we only take 1 clean sniper shot per 15m window
        tradedMarketIds.add(marketId);
        state.hunger = 0;
        const historyIndex = state.history.length;
        state.history.push({
          time: new Date().toLocaleTimeString(),
          hp: afterBal,
          action: `⏳ OPEN 15m (${minutesLeft}m left): ${m.symbol} ${decision.side} @ ${targetAsk} (Spent: ${actualSpent} tUSDC)`,
          edge: Number((decision.prob - targetAsk).toFixed(3)),
        });

        pendingPositions.set(marketId, {
          marketId,
          symbol: m.symbol,
          outcomeSymbol: targetSymbol,
          oppositeSymbol,
          side: decision.side,
          outcomeIndex: decision.side === "UP" ? 0 : 1,
          stakeUsd: actualSpent,
          shares: sharesToBuy,
          entryPrice: targetAsk,
          prob: decision.prob,
          historyIndex,
          enteredAtMs: Date.now(),
        });

        broadcastState("LIVE_ORDER_SENT");
        return;
      }
    }
  } catch (err: any) {
    console.warn(`⚠️ Scan notice: ${err.message?.slice(0, 80)}`);
  } finally {
    isTickRunning = false;
  }
}

// --- 10. STARTUP ---
async function startBot() {
  console.log(`🧠 Brain Mode: 15m ANTI-WICK SNIPER (TP +25% > SL -15% TrueMid) | Max Stake: ${MAX_STAKE_USD} tUSDC`);
  const pk = getNormalizedPrivateKey();

  if (pk) {
    try {
      const account = privateKeyToAccount(pk);
      botAddress = account.address;

      const testnetAddrs = (sdk as any).SOMNIA_TESTNET_ADDRESSES || {};
      collateralAddress = (testnetAddrs.testUsdc || testnetAddrs.collateral || "0x70a86D8842FB63C4Ad2b7cdddF530eBf1BB25d8E") as `0x${string}`;

      await syncOnChainBalance();

      exchange = new SomniaMarkets({
        indexerUrl: process.env.INDEXER_URL || "https://dev.smk.somnia.host/v1/graphql",
        chain: somniaShannon as any,
        wsRpcUrl: process.env.WS_RPC_URL || "wss://api.infra.testnet.somnia.network/ws",
        addresses: testnetAddrs,
        privateKey: pk,
      });

      console.log("✅ SomniaMarkets SDK initialized on Shannon Testnet!");
      broadcastState("WALLET_CONNECTED");
    } catch (err: any) {
      console.error(`❌ Startup Error: ${err.message}`);
    }
  }

  await runBotTick();
  setInterval(() => {
    if (state.status !== "DEAD") runBotTick();
  }, 6000);
}

startBot();