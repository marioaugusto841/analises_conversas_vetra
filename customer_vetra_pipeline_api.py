# -*- coding: utf-8 -*-
# customer_vetra_pipeline_api.py
# Recebe empresas via POST /executar
# Aceita date_from / date_to no corpo do POST
# Se não vierem datas → calcula automaticamente (hoje - 2 → hoje)
# Busca customers na Aurora
# Troca contexto via GET/PUT /auth/me (Opção A)
# Verifica IDs no Supabase
# Gera CSV e JSON consolidado
# Envia automaticamente para webhook configurado

import os
import csv
import json
import time
import argparse
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

# carregar variáveis do .env (deve vir antes de usar os.getenv)
load_dotenv()

try:
    from fastapi import FastAPI, Request, HTTPException
    import uvicorn
except Exception:
    FastAPI = None
    HTTPException = None

# =================== CONFIG ===================
BASE_URL          = os.getenv("AURORA_BASE_URL", "https://api.auroraia.com.br")
LOGIN_URL         = f"{BASE_URL}/auth/login"
ME_URL            = f"{BASE_URL}/auth/me"
CUSTOMERS_URL     = f"{BASE_URL}/api/v1/customers"

AURORA_EMAIL      = os.getenv("AURORA_EMAIL", "seu.email@empresa.com")
AURORA_PASSWORD   = os.getenv("AURORA_PASSWORD", "troque-me")

PAGE_SIZE         = int(os.getenv("PAGE_SIZE", "500"))

# 🔐 Token da API usado para proteger o /executar
# (vem do arquivo .env, variável VETRA_PIPELINE_API_TOKEN)
API_STATIC_TOKEN = os.getenv("VETRA_PIPELINE_API_TOKEN")

# Supabase
SUPABASE_URL         = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY         = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY", "")
SUPABASE_TABLE       = os.getenv("SUPABASE_TABLE", "customers")
SUPABASE_ID_COLUMN   = os.getenv("SUPABASE_ID_COLUMN", "customer_id")
SUPABASE_CHECK_MODE  = os.getenv("SUPABASE_CHECK_MODE", "filter")
SUPABASE_BATCH_SIZE  = int(os.getenv("SUPABASE_BATCH_SIZE", "500"))

# 📡 Webhook fixo (sem token)
TARGET_POST_URL = "https://n8n.vetraia.com/webhook-test/7c70edea-2570-4abe-b351-62ca01f7cac4"

CSV_COLUMNS = [
    "id_vetra", "company_id", "company_name", "customer_id", "name", "phone",
    "status", "thread_id", "human_controller_id", "created_at", "updated_at",
    "unread_messages_count", "last_message_at", "last_message_body", "last_message_from",
    "company_tags", "pre_schedule_visit", "supabase_exists"
]

# =================== HTTP util ===================
def http_request(method: str, url: str, *, headers=None, params=None, json=None,
                 max_retries=3, backoff_base=0.8) -> requests.Response:
    headers = headers or {}
    params = params or {}
    attempt = 0
    while True:
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json, timeout=60)
            if 200 <= r.status_code < 300:
                return r
            # reintento para 5xx
            if r.status_code >= 500 and attempt < max_retries:
                attempt += 1
                time.sleep(backoff_base * (2 ** (attempt - 1)))
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            if attempt < max_retries:
                attempt += 1
                time.sleep(backoff_base * (2 ** (attempt - 1)))
                continue
            raise e

# =================== Aurora ===================
def login() -> str:
    headers = {"Content-Type": "application/json"}
    body = {"email": AURORA_EMAIL, "password": AURORA_PASSWORD}
    r = http_request("POST", LOGIN_URL, headers=headers, json=body)
    data = r.json()
    token = data.get("access_token") or data.get("token")
    if not token:
        raise RuntimeError(f"Falha no login: token ausente. Resposta: {data}")
    return token

def get_me(token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    r = http_request("GET", ME_URL, headers=headers)
    return r.json()

def put_auth_me_company(token: str, company_id: int) -> dict:
    """
    Opção A: alguns ambientes exigem o 'me' completo no PUT /auth/me.
    Fazemos GET /auth/me, atualizamos company_id e reenviamos o corpo completo.
    """
    me = get_me(token)
    # monta corpo com campos mais comuns/esperados pela API
    body = {
        "id": me.get("id"),
        "email": me.get("email"),
        "first_name": me.get("first_name"),
        "last_name": me.get("last_name"),
        "role": me.get("role"),
        "company_id": company_id,
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = http_request("PUT", ME_URL, headers=headers, json=body)
    return r.json()

def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse robusto para ISO-8601:
    - aceita 'Z' (UTC)
    - aceita milissegundos
    - se vier sem timezone, assume UTC
    """
    if not dt_str:
        return None
    s = str(dt_str).strip()
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        import re
        m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", s)
        if m:
            try:
                return datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
            except Exception:
                return None
        return None

def local_range_to_utc(date_from: date, date_to: date, tz_name: str = "America/Sao_Paulo") -> Tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(date_from, datetime.min.time()).replace(tzinfo=tz)
    end_local   = datetime.combine(date_to,   datetime.max.time()).replace(tzinfo=tz)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def fetch_customers(token: str, company_id_expected: int,
                    date_from_utc: datetime, date_to_utc: datetime,
                    page_size: int = PAGE_SIZE) -> List[Dict[str, Any]]:
    """
    Busca customers paginando e FILTRA localmente por:
      - company_id == company_id_expected
      - created_at dentro do intervalo inclusivo [date_from_utc, date_to_utc]
    Compatível com respostas que trazem 'customers' (preferido) ou 'items'/'data'/lista.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    all_items: List[Dict[str, Any]] = []
    seen_ids: Set[Any] = set()
    skip = 0

    while True:
        params = {"order_by": "created_at", "limit": page_size, "skip": skip}
        r = http_request("GET", CUSTOMERS_URL, headers=headers, params=params)
        data = r.json()

        # prioriza 'customers' como no script que funciona
        if isinstance(data, dict):
            items = data.get("customers")
            if items is None:
                items = data.get("items") or data.get("data")
        else:
            items = data  # pode vir lista crua

        if not items:
            print(f"  [fetch] página vazia (skip={skip})")
            break

        print(f"  [fetch] página len={len(items)} (skip={skip})")

        for it in items:
            # filtra por empresa
            if it.get("company_id") != company_id_expected:
                continue

            # parse de created_at
            created_at = parse_iso(it.get("created_at"))
            if not created_at:
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            created_at_utc = created_at.astimezone(timezone.utc)

            # filtra por período inclusivo
            if not (date_from_utc <= created_at_utc <= date_to_utc):
                continue

            _id = it.get("id")
            if _id in seen_ids:
                continue
            seen_ids.add(_id)

            all_items.append(it)

        # paginação por skip/limit
        if len(items) < page_size:
            break
        skip += page_size

    return all_items

# =================== Supabase ===================
def supabase_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "count=exact"
    }

def supabase_fetch_existing_ids(ids: List[Any]) -> Set[Any]:
    existing: Set[Any] = set()
    if not (SUPABASE_URL and SUPABASE_KEY and SUPABASE_TABLE and SUPABASE_ID_COLUMN):
        return existing

    rest_base = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = supabase_headers()

    for i in range(0, len(ids), SUPABASE_BATCH_SIZE):
        chunk = ids[i:i+SUPABASE_BATCH_SIZE]
        values = ",".join([f'\"{str(v)}\"' for v in chunk])
        params = {
            "select": SUPABASE_ID_COLUMN,
            SUPABASE_ID_COLUMN: f'in.({values})'
        }
        r = http_request("GET", rest_base, headers=headers, params=params)
        try:
            data = r.json()
        except Exception:
            data = []
        for row in data:
            existing.add(str(row.get(SUPABASE_ID_COLUMN)))
    return existing

# =================== Build/Write ===================
def build_rows(company_id: int, company_name: str,
               customers: List[Dict[str, Any]],
               supabase_existing: Set[Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for c in customers:
        cid = str(c.get("id"))
        exists = cid in supabase_existing
        rows.append({
            "id_vetra": c.get("id"),
            "company_id": company_id,
            "company_name": company_name,
            "customer_id": c.get("id"),
            "name": c.get("name"),
            "phone": c.get("phone"),
            "status": c.get("status"),
            "thread_id": c.get("thread_id"),
            "human_controller_id": c.get("human_controller_id"),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
            "unread_messages_count": c.get("unread_messages_count"),
            "last_message_at": c.get("last_message_at"),
            "last_message_body": c.get("last_message_body"),
            "last_message_from": c.get("last_message_from"),
            "company_tags": c.get("company", {}).get("tags") or "-",
            "pre_schedule_visit": False if c.get("pre_schedule_visit") in (None, "", "null") else c.get("pre_schedule_visit"),
            "supabase_exists": exists
        })
    return rows

def write_csv(rows: List[Dict[str, Any]], outname: str) -> str:
    outpath = os.path.abspath(outname)
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return outpath

def build_consolidated_json(companies_rows: Dict[int, List[Dict[str, Any]]]) -> Dict[str, Any]:
    companies_payload: List[Dict[str, Any]] = []
    for company_id, rows in companies_rows.items():
        if not rows:
            continue
        company_name = rows[0].get("company_name", "")
        companies_payload.append({
            "company_id": company_id,
            "company_name": company_name,
            "customers": rows
        })
    return {"companies": companies_payload}

def post_consolidated(payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    r = http_request("POST", TARGET_POST_URL, headers=headers, json=payload)
    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "text": r.text}

# =================== Core ===================
def compute_default_date_range() -> Tuple[str, str]:
    tz = ZoneInfo("America/Sao_Paulo")
    today_local = datetime.now(tz).date()
    return (today_local - timedelta(days=2)).isoformat(), today_local.isoformat()

def run_pipeline(companies_dict: Dict[int, str],
                 do_post: bool = True,
                 date_from_str: Optional[str] = None,
                 date_to_str: Optional[str] = None) -> Dict[str, Any]:
    if not date_from_str or not date_to_str:
        date_from_str, date_to_str = compute_default_date_range()

    date_from_utc, date_to_utc = local_range_to_utc(
        datetime.strptime(date_from_str, "%Y-%m-%d").date(),
        datetime.strptime(date_to_str, "%Y-%m-%d").date()
    )

    all_rows: List[Dict[str, Any]] = []
    per_company_rows: Dict[int, List[Dict[str, Any]]] = {}

    for company_id, company_name in companies_dict.items():
        print(f"\nEmpresa {company_id} - {company_name}")
        token1 = login()
        print("  1) login OK")

        # troca contexto de empresa (Opção A: GET + PUT /auth/me)
        try:
            _ = put_auth_me_company(token1, company_id)
            print("  2) PUT /auth/me OK (contexto trocado)")
        except requests.HTTPError as e:
            # log útil de diagnóstico
            status = getattr(e.response, "status_code", "?")
            text = getattr(e.response, "text", "")
            print("  2) PUT /auth/me FALHOU:", status, text)
            raise

        # em alguns ambientes é recomendável relogar após trocar contexto
        token2 = login()
        print("  3) novo login OK")

        customers = fetch_customers(token2, company_id, date_from_utc, date_to_utc)
        print(f"  4) {len(customers)} customers no período")

        ids = [c.get("id") for c in customers if c.get("id") is not None]
        existing = supabase_fetch_existing_ids(ids) if ids else set()
        print(f"  5) Supabase: {len(existing)} de {len(ids)} encontrados")

        rows = build_rows(company_id, company_name, customers, existing)
        all_rows.extend(rows)
        per_company_rows[company_id] = rows

    out_csv = f"relatorio_customers_CONSOLIDADO_{date_from_str}_{date_to_str}.csv"
    write_csv(all_rows, out_csv)
    print(f"\nCSV gerado: {out_csv}")

    # 🔥 FILTRA: só mantém customers que NÃO existem no Supabase
    filtered_per_company_rows: Dict[int, List[Dict[str, Any]]] = {}
    for company_id, rows in per_company_rows.items():
        filtered_per_company_rows[company_id] = [
            r for r in rows if not r.get("supabase_exists")
        ]

    # monta o JSON só com os "novos" para enviar ao webhook
    payload = build_consolidated_json(filtered_per_company_rows)

    with open("consolidado_companies.json", "w", encoding="utf-8") as jf:
        json.dump(payload, jf, ensure_ascii=False, indent=2)

    post_response = None
    if do_post:
        print(f"POST -> {TARGET_POST_URL}")
        post_response = post_consolidated(payload)
        print("Resposta do destino:", post_response)

    return {
        "date_from": date_from_str,
        "date_to": date_to_str,
        "csv_path": out_csv,
        "posted": do_post,
        "post_response": post_response
    }

# =================== FastAPI ===================
def build_app():
    if FastAPI is None:
        raise RuntimeError("FastAPI/uvicorn não instalados.")
    if not API_STATIC_TOKEN:
        raise RuntimeError("VETRA_PIPELINE_API_TOKEN não configurado no .env")

    app = FastAPI()

    @app.post("/api/executar")
    async def executar(request: Request):
        # 🔐 Autenticação simples via header
        auth_header = request.headers.get("x-api-key") or request.headers.get("authorization")

        expected_bearer = f"Bearer {API_STATIC_TOKEN}"

        if auth_header not in (API_STATIC_TOKEN, expected_bearer):
            # 401 se o token estiver errado ou não vier
            raise HTTPException(status_code=401, detail="Unauthorized")

        # ✅ Se passou da validação, segue o fluxo normal
        raw = await request.json()

        # --- 🔸 Normalização do payload: aceita dict OU list com 1 elemento ---
        if isinstance(raw, dict):
            payload = raw
        elif isinstance(raw, list):
            if not raw:
                raise ValueError("Payload list is empty")
            first = raw[0]
            # Se for no formato n8n [{ "json": { ... } }]
            if isinstance(first, dict) and isinstance(first.get("json"), dict):
                payload = first["json"]
            elif isinstance(first, dict):
                payload = first
            else:
                raise ValueError("Invalid list payload format")
        else:
            raise ValueError("Invalid payload type")

        # --- 🔸 Agora pode usar .get normalmente ---
        companies = payload.get("companies", [])
        do_post = payload.get("post", True)
        date_from = payload.get("date_from")
        date_to = payload.get("date_to")

        companies_dict: Dict[int, str] = {}
        for c in companies:
            cid = int(c["id"])
            companies_dict[cid] = c.get("name") or str(cid)

        result = run_pipeline(
            companies_dict,
            do_post=do_post,
            date_from_str=date_from,
            date_to_str=date_to
        )
        return {"status": "ok", **result}

    return app

# =================== CLI ===================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()

    if args.serve:
        if FastAPI is None:
            raise RuntimeError("FastAPI/uvicorn não instalados.")
        uvicorn.run(build_app(), host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
        return

if __name__ == "__main__":
    main()