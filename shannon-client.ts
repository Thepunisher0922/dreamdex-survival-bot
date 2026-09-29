import "dotenv/config";
import {
  createPublicClient,
  createWalletClient,
  http,
  parseGwei,
  defineChain,
  erc20Abi,
  formatUnits,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import * as sdk from "@somnia-chain/markets-sdk";

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

export const publicClient = createPublicClient({
  chain: somniaShannon,
  transport: http(somniaShannon.rpcUrls.default.http[0]),
});

/**
 * Wraps the viem WalletClient to override the SDK's default 10M gas * 60 gwei (0.6 STT)
 * requirement down to 2M gas * 12 gwei (0.024 STT).
 */
export function createShannonWallet(privateKey: `0x${string}`) {
  const account = privateKeyToAccount(privateKey);
  const baseWallet = createWalletClient({
    account,
    chain: somniaShannon,
    transport: http(somniaShannon.rpcUrls.default.http[0]),
  });

  const proxiedWallet = new Proxy(baseWallet, {
    get(target, prop, receiver) {
      const orig = Reflect.get(target, prop, receiver);
      if (prop === "writeContract" || prop === "sendTransaction") {
        return async (args: Record<string, unknown>) => {
          return (orig as Function).call(target, {
            ...args,
            gas: 2_000_000n,
            maxFeePerGas: parseGwei("12"),
            maxPriorityFeePerGas: parseGwei("2"),
          });
        };
      }
      return typeof orig === "function" ? orig.bind(target) : orig;
    },
  });

  return { account, walletClient: proxiedWallet };
}

/**
 * Reads live STT gas and tUSDC balance on Shannon Testnet (50312)
 */
export async function fetchBotBalances(address: `0x${string}`) {
  const chainId = await publicClient.getChainId();
  if (chainId !== 50312) {
    throw new Error(`SAFETY HALT: Expected Chain ID 50312 (Shannon), got ${chainId}`);
  }

  const sttRaw = await publicClient.getBalance({ address });
  const stt = Number(formatUnits(sttRaw, 18));

  // Discover tUSDC collateral address from SDK manifest
  const addrs = (sdk as any).SOMNIA_TESTNET_ADDRESSES || {};
  const collateralAddr = (addrs.testUsdc || addrs.collateral || addrs.usdc) as `0x${string}` | undefined;

  let tusdc = Number(process.env.STARTING_HP || 10.0);
  if (collateralAddr && process.env.DRY_RUN !== "true") {
    try {
      const [rawBal, decimals] = await Promise.all([
        publicClient.readContract({ address: collateralAddr, abi: erc20Abi, functionName: "balanceOf", args: [address] }),
        publicClient.readContract({ address: collateralAddr, abi: erc20Abi, functionName: "decimals" }),
      ]);
      tusdc = Number(formatUnits(rawBal, decimals));
    } catch {
      // Fallback to tracked HP if contract read fails
    }
  }

  return { stt, tusdc, collateralAddr };
}