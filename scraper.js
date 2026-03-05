import fetch from "node-fetch";
import zlib from "zlib";
import { promisify } from "util";
import ftp from "basic-ftp";
import { PassThrough } from "stream";

const gunzip = promisify(zlib.gunzip);

const CF_HEADERS = {
  "Authorization": `Bearer ${process.env.CF_API_TOKEN}`,
  "Content-Type": "application/json",
};
const KV_BASE = `https://api.cloudflare.com/client/v4/accounts/${process.env.CF_ACCOUNT_ID}/storage/kv/namespaces/${process.env.CF_KV_NAMESPACE_ID}/values`;
const UA = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36" };

// ── Stores ────────────────────────────────────────────────────────────────────
const STORES = [
  { vendor:"shufersal", storeId:"144",  chainId:"7290027600007", publisher:"shufersal" },
  { vendor:"tiv-taam",  storeId:"093",  chainId:"7290873255550", publisher:"publishedprices", ftpUser:"TivTaam" },
  { vendor:"osher-ad",  storeId:"031",  chainId:"7290103152017", publisher:"publishedprices", ftpUser:"osherad", storePrefix:"001" },
];

// ── Helpers ───────────────────────────────────────────────────────────────────
async function timedFetch(url, options = {}, ms = 10000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } catch (e) {
    if (e.name === "AbortError") throw new Error(`Timeout(${ms}ms): ${url}`);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// ── URL resolvers ─────────────────────────────────────────────────────────────

async function getUrl_shufersal({ storeId, chainId }) {
  const res = await timedFetch(
    `https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&storeId=${storeId}&sort=Time&sortdir=DESC&page=1`,
    { headers: UA }, 25000
  );
  const html = await res.text();
  const m1 = [...html.matchAll(/href="(https:\/\/pricesprodpublic\.blob\.core\.windows\.net\/pricefull\/PriceFull[^"]+)"/g)];
  if (m1.length) return m1[0][1].replace(/&amp;/g, "&");
  const m2 = [...html.matchAll(new RegExp(`href="([^"]*PriceFull[^"]*${chainId}-${storeId}[^"]*)"`, "gi"))];
  if (m2.length) {
    const href = m2[0][1].replace(/&amp;/g, "&");
    return href.startsWith("http") ? href : `https://prices.shufersal.co.il${href}`;
  }
  throw new Error("Shufersal: download URL not found");
}

// ── FTP download for publishedprices (TivTaam + OsherAd) ─────────────────────
// Cerberus engine uses: host=url.retail.publishedprices.co.il, password="" (empty)

async function downloadViaftp({ storeId, chainId, storePrefix, ftpUser }) {
  console.log(`  🔌 FTP connect as ${ftpUser}@url.retail.publishedprices.co.il...`);
  const client = new ftp.Client(15000); // 15s timeout
  client.ftp.verbose = false;
  try {
    await client.access({
      host: "url.retail.publishedprices.co.il",
      user: ftpUser,
      password: "",
      secure: false,
    });
    console.log(`  ✓ FTP connected`);

    const files = await client.list("/");
    const allNames = files.map(f => f.name);
    console.log(`  FTP root files (${allNames.length}): ${allNames.slice(0, 10).join(", ")}${allNames.length > 10 ? "..." : ""}`);

    // Match PriceFull file for this store
    const pat = storePrefix
      ? new RegExp(`PriceFull${chainId}-${storePrefix}-${storeId}-`, "i")
      : new RegExp(`PriceFull${chainId}-${storeId}-`, "i");

    const priceFiles = files
      .filter(f => pat.test(f.name) && /\.(gz|xml\.gz|xml)$/i.test(f.name))
      .sort((a, b) => b.name.localeCompare(a.name)); // latest first

    if (!priceFiles.length) {
      throw new Error(`FTP: no PriceFull file found for ${ftpUser} storeId=${storeId}`);
    }

    const filename = priceFiles[0].name;
    console.log(`  ✓ FTP file: ${filename}`);

    // Download file to memory using a PassThrough stream
    const chunks = [];
    const writable = new PassThrough();
    writable.on("data", chunk => chunks.push(chunk));
    await client.downloadTo(writable, `/${filename}`);
    const buffer = Buffer.concat(chunks);
    console.log(`  ✓ FTP downloaded ${buffer.length} bytes`);
    return buffer;
  } finally {
    client.close();
  }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────
async function getFreshDownloadUrl(store) {
  console.log(`📡 ${store.vendor} (store ${store.storeId})...`);
  switch (store.publisher) {
    case "shufersal": return getUrl_shufersal(store);
    default: throw new Error(`Unknown publisher: ${store.publisher}`);
  }
}

// ── Download & parse ──────────────────────────────────────────────────────────
async function downloadAndParse(url) {
  console.log(`⬇️  Downloading ${url.slice(-60)}...`);
  const res = await timedFetch(url, { headers: UA }, 60000);
  if (!res.ok) throw new Error(`Download failed: ${res.status}`);
  const buffer = Buffer.from(await res.arrayBuffer());
  return parseBuffer(buffer, res.headers.get("content-type"));
}

async function parseBuffer(buffer, contentType) {
  let xml;
  let gunzipped = false;
  try { xml = (await gunzip(buffer)).toString("utf-8"); gunzipped = true; }
  catch { xml = buffer.toString("utf-8"); }

  console.log(`  content-type: ${contentType} | gunzip: ${gunzipped} | size: ${buffer.length}b`);
  console.log(`  preview: ${xml.slice(0, 200).replace(/\s+/g, " ")}`);

  if (xml.trimStart().startsWith("<html") || xml.trimStart().startsWith("<!DOCTYPE")) {
    throw new Error("Server returned HTML page — likely requires authentication");
  }

  const products = parseXML(xml);
  if (products.length === 0) {
    console.log(`  ⚠️  0 products — XML snippet: ${xml.slice(0, 400).replace(/\s+/g, " ")}`);
    throw new Error("Parsed 0 products — file may be empty or wrong format");
  }
  console.log(`✅ Parsed ${products.length} products`);
  return products;
}

function parseXML(xml) {
  const products = [];
  const itemRegex = /<Item>([\s\S]*?)<\/Item>/g;
  let match;
  while ((match = itemRegex.exec(xml)) !== null) {
    const block = match[1];
    const get = tag => { const m = block.match(new RegExp(`<${tag}>([^<]*)<\\/${tag}>`)); return m ? m[1].trim() : ""; };
    const price = parseFloat(get("ItemPrice"));
    const name  = get("ItemName");
    if (!name || !price || price <= 0) continue;
    products.push({ barcode: get("ItemCode"), name, company: get("ManufacturerName"), price, unit: get("UnitOfMeasure"), quantity: get("Quantity"), unitQty: get("UnitQty") });
  }
  return products;
}

// ── Upload to Cloudflare KV ───────────────────────────────────────────────────
async function uploadToKV(vendor, storeId, products) {
  console.log(`☁️  Uploading ${vendor}:${storeId} (${products.length} products)...`);
  const CHUNK_SIZE = 500;
  const chunks = [];
  for (let i = 0; i < products.length; i += CHUNK_SIZE) chunks.push(products.slice(i, i + CHUNK_SIZE));
  for (let i = 0; i < chunks.length; i++) {
    const res = await fetch(`${KV_BASE}/${encodeURIComponent(`${vendor}:${storeId}:chunk:${i}`)}`, {
      method: "PUT", headers: CF_HEADERS, body: JSON.stringify(chunks[i]),
    });
    if (!res.ok) throw new Error(`KV chunk ${i}: ${res.status} – ${await res.text()}`);
    console.log(`  ✓ ${i + 1}/${chunks.length}`);
  }
  await fetch(`${KV_BASE}/${encodeURIComponent(`${vendor}:${storeId}:meta`)}`, {
    method: "PUT", headers: CF_HEADERS,
    body: JSON.stringify({ chunks: chunks.length, total: products.length, updatedAt: new Date().toISOString(), storeId }),
  });
  console.log(`✅ ${products.length} products uploaded`);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function run() {
  const errors = [];
  for (const store of STORES) {
    console.log(`\n── ${store.vendor} ──`);
    try {
      let products;
      if (store.publisher === "publishedprices") {
        // FTP download (Cerberus engine: url.retail.publishedprices.co.il, empty password)
        const buffer = await downloadViaftp(store);
        products = await parseBuffer(buffer, null);
      } else {
        const url = await getFreshDownloadUrl(store);
        products = await downloadAndParse(url);
      }
      await uploadToKV(store.vendor, store.storeId, products);
    } catch (err) {
      console.error(`❌ ${store.vendor}: ${err.message}`);
      errors.push(`${store.vendor}: ${err.message}`);
    }
  }
  if (errors.length) {
    console.error(`\n❌ ${errors.length} store(s) failed:\n${errors.join("\n")}`);
    process.exit(1);
  }
}

run();
