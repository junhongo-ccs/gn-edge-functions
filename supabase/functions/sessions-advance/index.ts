// supabase/functions/sessions-advance/index.ts
// deno-lint-ignore-file no-explicit-any
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.46.1";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY")!;
const SERVICE_ROLE = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const HOST_PIN = Deno.env.get("GN_HOST_PIN") || ""; // 司会PIN（任意）

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, apikey, content-type, x-gn-host-pin",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
};

function ok(body: unknown, code = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status: code,
    headers: { "Content-Type": "application/json; charset=utf-8", ...cors },
  });
}
function err(message: string, code = 400) {
  return ok({ ok: false, code, message }, code);
}

function supabaseAdmin() {
  const key = SERVICE_ROLE || ANON_KEY;
  return createClient(SUPABASE_URL, key, { auth: { persistSession: false } });
}

/**
 * 入力:
 * {
 *   session_id: string,
 *   expected_current_entry_id: string,
 *   action: "done" | "skipped",
 *   top_label?: string,
 *   top_tags?: string[]
 * }
 */
serve(async (req) => {
  try {
    if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
    if (req.method !== "POST") return err("405: POSTのみ対応", 405);

    const pin = req.headers.get("x-gn-host-pin") || "";
    if (HOST_PIN && pin !== HOST_PIN) return err("403: PINが一致しません。", 403);

    const supabase = supabaseAdmin();

    let payload: any = {};
    try { payload = await req.json(); } catch { return err("400: JSONが不正です", 400); }

    const {
      session_id,
      expected_current_entry_id,
      action,
      top_label,
      top_tags,
    } = payload || {};

    if (!session_id || !expected_current_entry_id || !action) {
      return err("400: 必須フィールド不足（session_id / expected_current_entry_id / action）", 400);
    }
    if (!["done", "skipped"].includes(action)) {
      return err("400: action は 'done' か 'skipped' を指定してください", 400);
    }

    // 1) 現在のstateを取得
    const { data: state, error: stErr } = await supabase
      .from("session_state")
      .select("session_id, current_entry_id")
      .eq("session_id", session_id)
      .maybeSingle();

    if (stErr) return err(`DB error(state): ${stErr.message}`, 500);
    if (!state) return err("409: session_state が未初期化です（ランダマイズ未実施）", 409);

    // CAS: 期待IDと一致？
    if (state.current_entry_id !== expected_current_entry_id) {
      return err("409: 他端末で進行がありました（expected_current_entry_id 不一致）", 409);
    }

    // 2) 現在のエントリを done/skip に
    const { error: updErr } = await supabase
      .from("session_entries")
      .update({
        status: action === "done" ? "done" : "skipped",
        ended_at: new Date().toISOString(),
      })
      .eq("id", expected_current_entry_id);
    if (updErr) return err(`DB error(update current): ${updErr.message}`, 500);

    // 3) 次の pending を取得
    const { data: next, error: nextErr } = await supabase
      .from("session_entries")
      .select("id, order_index")
      .eq("session_id", session_id)
      .eq("status", "pending")
      .order("order_index", { ascending: true })
      .limit(1)
      .maybeSingle();

    if (nextErr) return err(`DB error(select next): ${nextErr.message}`, 500);

    let nextEntry: null | { id: string; order_index: number } = null;

    if (next) {
      const { error: spErr } = await supabase
        .from("session_entries")
        .update({ status: "speaking", started_at: new Date().toISOString() })
        .eq("id", next.id);
      if (spErr) return err(`DB error(update next->speaking): ${spErr.message}`, 500);

      nextEntry = next;
    }

    // 4) state更新
    const { error: stUpdErr } = await supabase
      .from("session_state")
      .update({
        current_entry_id: nextEntry?.id ?? null,
        updated_at: new Date().toISOString(),
      })
      .eq("session_id", session_id);
    if (stUpdErr) return err(`DB error(update state): ${stUpdErr.message}`, 500);

    // 5) 代表カテゴリ/タグの加算（任意）
    if (top_label && typeof top_label === "string") {
      const r1 = await supabase.rpc("inc_session_topic_count", {
        p_session: session_id,
        p_label: top_label,
      });
      if (r1.error) return err(`RPC error(inc_session_topic_count): ${r1.error.message}`, 500);
    }
    if (Array.isArray(top_tags)) {
      for (const t of top_tags.slice(0, 3)) {
        if (!t) continue;
        const r2 = await supabase.rpc("inc_session_keyword_count", {
          p_session: session_id,
          p_tag: String(t),
        });
        if (r2.error) return err(`RPC error(inc_session_keyword_count): ${r2.error.message}`, 500);
      }
    }

    // 6) 押下ログ（個人ログイン廃止 → 匿名扱い）
    const { error: logErr } = await supabase
      .from("session_action_logs")
      .insert({
        session_id,
        actor_name: "guest",
        action: action,
        prev_entry_id: expected_current_entry_id,
      });
    if (logErr) return err(`DB error(log): ${logErr.message}`, 500);

    return ok({
      ok: true,
      session_state: { session_id, current_entry_id: nextEntry?.id ?? null },
      next_entry: nextEntry,
    });
  } catch (e: any) {
    return err(`500: ${e?.message ?? String(e)}`, 500);
  }
});
