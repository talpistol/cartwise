// Cloudflare Worker — CartWise API
// Deploy: paste into Cloudflare Workers editor (or use wrangler)
// KV binding name: KV
// Required KV keys set by scraper: {vendor}:{storeId}:meta, {vendor}:{storeId}:chunk:N, vendors

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const CORS = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,PUT,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type,Authorization",
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: CORS });

    function json(data, status = 200) {
      return new Response(JSON.stringify(data), {
        status,
        headers: { ...CORS, "Content-Type": "application/json" },
      });
    }

    // ── GET /vendors ──────────────────────────────────────────────
    if (url.pathname === "/vendors" && request.method === "GET") {
      const raw = await env.KV.get("vendors");
      return json(raw ? JSON.parse(raw) : []);
    }

    // ── GET /search?q=&vendor=&store= ─────────────────────────────
    if (url.pathname === "/search" && request.method === "GET") {
      const q = url.searchParams.get("q")?.trim().toLowerCase() || "";
      const vendor = url.searchParams.get("vendor") || "";
      const store = url.searchParams.get("store") || "";
      if (q.length < 2 || !vendor || !store) return json([]);

      const metaRaw = await env.KV.get(`${vendor}:${store}:meta`);
      if (!metaRaw) return json([]);
      const { chunks } = JSON.parse(metaRaw);
      const tokens = q.split(/\s+/).filter(Boolean);
      const results = [];

      for (let i = 0; i < chunks && results.length < 10; i++) {
        const chunk = await env.KV.get(`${vendor}:${store}:chunk:${i}`);
        if (!chunk) continue;
        for (const p of JSON.parse(chunk)) {
          const hay = (p.name + " " + (p.company || "")).toLowerCase();
          if (tokens.every(t => hay.includes(t))) {
            results.push(p);
            if (results.length >= 10) break;
          }
        }
      }
      return json(results);
    }

    // ── GET /meta?vendor=&store= ──────────────────────────────────
    // Returns { chunks, total, updatedAt, storeId } or null
    if (url.pathname === "/meta" && request.method === "GET") {
      const vendor = url.searchParams.get("vendor") || "";
      const store = url.searchParams.get("store") || "";
      if (!vendor || !store) return json(null);
      const raw = await env.KV.get(`${vendor}:${store}:meta`);
      return json(raw ? JSON.parse(raw) : null);
    }

    // ── POST /compare ─────────────────────────────────────────────
    // Body: { items: [{barcode}], vendors: [{vendor, store}] }
    // Returns: { "vendor:store": { barcode: price } }
    if (url.pathname === "/compare" && request.method === "POST") {
      const body = await request.json().catch(() => null);
      if (!body?.items?.length || !body?.vendors?.length) return json({});

      const barcodes = new Set(body.items.map(i => i.barcode).filter(Boolean));
      if (!barcodes.size) return json({});

      const result = {};
      await Promise.all(body.vendors.map(async ({ vendor, store }) => {
        const key = `${vendor}:${store}`;
        result[key] = {};
        const metaRaw = await env.KV.get(`${vendor}:${store}:meta`);
        if (!metaRaw) return;
        const { chunks } = JSON.parse(metaRaw);
        // Fetch all chunks for this vendor in parallel
        const chunkData = await Promise.all(
          Array.from({ length: chunks }, (_, i) => env.KV.get(`${vendor}:${store}:chunk:${i}`))
        );
        for (const raw of chunkData) {
          if (!raw) continue;
          for (const p of JSON.parse(raw)) {
            if (barcodes.has(p.barcode)) result[key][p.barcode] = p.price;
          }
        }
      }));

      return json(result);
    }

    // ── POST /product — save a manually added product to KV ───────
    if (url.pathname === "/product" && request.method === "POST") {
      const body = await request.json().catch(() => null);
      if (!body?.vendor || !body?.store || !body?.name) return json({ error: "missing fields" }, 400);

      const product = {
        barcode: `manual-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        name: body.name.trim(),
        company: body.company?.trim() || "",
        price: parseFloat(body.price) || 0,
        unit: "", quantity: "", unitQty: "",
        manual: true,
      };

      const { vendor, store } = body;
      const metaRaw = await env.KV.get(`${vendor}:${store}:meta`);
      if (!metaRaw) {
        await env.KV.put(`${vendor}:${store}:chunk:0`, JSON.stringify([product]));
        await env.KV.put(`${vendor}:${store}:meta`, JSON.stringify({ chunks: 1, total: 1, updatedAt: new Date().toISOString(), storeId: store }));
        return json(product);
      }

      const meta = JSON.parse(metaRaw);
      const lastIdx = meta.chunks - 1;
      const lastRaw = await env.KV.get(`${vendor}:${store}:chunk:${lastIdx}`);
      const lastChunk = lastRaw ? JSON.parse(lastRaw) : [];
      if (lastChunk.length < 500) {
        lastChunk.push(product);
        await env.KV.put(`${vendor}:${store}:chunk:${lastIdx}`, JSON.stringify(lastChunk));
      } else {
        await env.KV.put(`${vendor}:${store}:chunk:${meta.chunks}`, JSON.stringify([product]));
        meta.chunks += 1;
      }
      meta.total = (meta.total || 0) + 1;
      await env.KV.put(`${vendor}:${store}:meta`, JSON.stringify(meta));
      return json(product);
    }

    // ── PUT /admin/vendors (requires Google ID token) ─────────────
    if (url.pathname === "/admin/vendors" && request.method === "PUT") {
      const auth = request.headers.get("Authorization") || "";
      const token = auth.replace("Bearer ", "");
      let email = "";
      try {
        // Decode JWT payload (base64url → base64)
        const payload = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
        email = payload.email || "";
      } catch {}
      if (email !== "talpistol@gmail.com") {
        return new Response("Unauthorized", { status: 401, headers: CORS });
      }
      const vendors = await request.json();
      await env.KV.put("vendors", JSON.stringify(vendors));
      return json({ ok: true });
    }

    // ── GET /manifest.json ────────────────────────────────────────
    if (url.pathname === "/manifest.json" && request.method === "GET") {
      return new Response(JSON.stringify({
        name: "CartWise – השוואת מחירים",
        short_name: "CartWise",
        description: "השווה מחירים בין סופרמרקטים ובנה רשימת קניות חכמה",
        start_url: ".",
        display: "standalone",
        background_color: "#1e3a5f",
        theme_color: "#1e3a5f",
        lang: "he",
        dir: "rtl",
        icons: [{ src: "icon.svg", sizes: "any", type: "image/svg+xml", purpose: "any" }],
      }), { headers: { ...CORS, "Content-Type": "application/manifest+json" } });
    }

    return new Response("Not Found", { status: 404, headers: CORS });
  },
};
