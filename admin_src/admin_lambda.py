from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import boto3


ssm = boto3.client("ssm")
lambda_api = boto3.client("lambda")
scheduler = boto3.client("scheduler")
budgets = boto3.client("budgets", region_name="us-east-1")

POLAND_TZ = ZoneInfo("Europe/Warsaw")
STATUS_PARAM = os.getenv("SYNC_STATUS_PARAM", "/comarch-baselinker-sync/push-sync-status")
SYNC_FUNCTION_NAME = os.getenv("SYNC_FUNCTION_NAME", "comarch-baselinker-sync")
SYNC_CONFIG_PARAM = os.getenv("SYNC_CONFIG_PARAM", "/comarch-baselinker-sync/sync-config")
DEFAULT_COMARCH_XML_URL = os.getenv("DEFAULT_COMARCH_XML_URL", "")
DEFAULT_BL_INVENTORY_ID = os.getenv("DEFAULT_BL_INVENTORY_ID", "")
DEFAULT_BL_WAREHOUSE_ID = os.getenv("DEFAULT_BL_WAREHOUSE_ID", "")
DEFAULT_BL_API_MAX_RPM = os.getenv("DEFAULT_BL_API_MAX_RPM", "90")
BL_API_URL = os.getenv("BL_API_URL", "https://api.baselinker.com/connector.php")
BL_API_TOKEN_SSM_PARAM = os.getenv(
    "BL_API_TOKEN_SSM_PARAM",
    "/comarch-baselinker-sync/api-token",
)
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_SHA256 = os.getenv("ADMIN_PASSWORD_SHA256", "")
SCHEDULE_NAME = os.getenv("SCHEDULE_NAME", "comarch-baselinker-sync-midnight")
SCHEDULE_GROUP = os.getenv("SCHEDULE_GROUP", "default")
BUDGET_NAME = os.getenv("BUDGET_NAME", "comarch-baselinker-sync-monthly-budget")
BUDGET_LIMIT_USD = os.getenv("BUDGET_LIMIT_USD", "30")
BUDGET_USD_TO_PLN_RATE = os.getenv("BUDGET_USD_TO_PLN_RATE", "4.00")
BUDGET_FX_RATE_SSM_PARAM = os.getenv("BUDGET_FX_RATE_SSM_PARAM", "/comarch-baselinker-sync/usd-pln-rate")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
BRAND_NAME = os.getenv("BRAND_NAME", "Comarch → BaseLinker Sync")
BRAND_PANEL_TITLE = os.getenv("BRAND_PANEL_TITLE", "Product synchronization")
BRAND_PANEL_SUBTITLE = os.getenv(
    "BRAND_PANEL_SUBTITLE",
    "Status and manual product synchronization",
)
ADMIN_LOCALE = os.getenv("ADMIN_LOCALE", "en").strip().lower()
if ADMIN_LOCALE not in {"en", "pl"}:
    ADMIN_LOCALE = "en"
BRAND_PRIMARY_COLOR = os.getenv("BRAND_PRIMARY_COLOR", "#1673b8")
BRAND_PRIMARY_DARK_COLOR = os.getenv("BRAND_PRIMARY_DARK_COLOR", "#0f5d96")
BRAND_SECONDARY_COLOR = os.getenv("BRAND_SECONDARY_COLOR", "#183c5c")
BRAND_LOGO_ENABLED = os.getenv("BRAND_LOGO_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LOGO_PATH = Path(__file__).with_name("client-logo.png")

TRANSLATIONS = {
    "en": {
        "page_title_suffix": "synchronization",
        "inventory_fallback": "Inventory {id}",
        "warehouse_fallback": "Warehouse {id}",
        "validation_https": "The XML URL must start with https://",
        "validation_inventory": "Select a valid BaseLinker inventory.",
        "validation_warehouse": "Select a valid BaseLinker warehouse.",
        "validation_warehouse_inventory": "The selected warehouse is not assigned to the selected inventory.",
        "validation_rpm": "The request limit must be a number from 1 to 100.",
        "attention_post_error": "The post-sync audit failed. Notify the system administrator to review the details, or try running the synchronization again.",
        "attention_post_diff": "The post-sync audit found {count} inconsistencies. Notify the system administrator to review the details, or try running the synchronization again.",
        "pre_summary": "{diff} differences, {unchanged} unchanged",
        "pre_checking": "Checking what needs to change.",
        "pre_waiting": "Waiting for synchronization to start.",
        "sync_skipped": "Skipped because no differences were found.",
        "sync_deleted": "; deleted {deleted} of {target} obsolete records",
        "sync_running": "Saved {updated} of {target} records{deleted_text}",
        "sync_done": "Saved {updated} records{deleted_text}",
        "sync_ready": "Ready to start after differences are calculated.",
        "sync_waiting": "Waiting for the difference calculation.",
        "post_skipped": "Skipped because there were no changes to apply.",
        "post_clean": "Audit completed: no inconsistencies.",
        "post_checking": "Checking data consistency after synchronization.",
        "post_waiting": "Waiting for synchronization to finish.",
        "step_pre": "Calculating differences before synchronization",
        "step_sync": "Applying changes in BaseLinker",
        "step_post": "Post-sync audit",
        "summary_diff": "{count} = number of detected data issues or differences.",
        "summary_changed": "{count} = number of records that must be created or updated in BaseLinker.",
        "summary_missing": "{count} = records among those {changed} that are new or missing in BaseLinker.",
        "summary_extra": "{count} = records that exist in BaseLinker but should be deleted.",
        "summary_unchanged": "{count} = unchanged records.",
        "summary_sync_skipped": "Synchronization was skipped because the pre-sync audit found no differences.",
        "summary_sync_done": "Synchronization: saved {updated} records and deleted {deleted} obsolete records.",
        "summary_post_skipped": "The post-sync audit was skipped because there were no changes to apply.",
        "summary_post_clean": "Post-sync audit: no inconsistencies.",
        "refresh": "Refresh data",
        "start": "Start synchronization",
        "label_status": "Status",
        "label_progress": "Progress",
        "label_eta": "Estimated completion",
        "label_next_run": "Next run",
        "label_cost": "Cost this month",
        "label_completed": "What was done",
        "label_stages": "Synchronization stages",
        "label_settings": "Synchronization settings",
        "label_xml": "Comarch e-Sklep XML URL",
        "label_inventory": "BaseLinker inventory",
        "label_warehouse": "BaseLinker warehouse",
        "label_rpm": "Requests / min",
        "config_note": "Changes are not saved while editing. <strong>They will be used and remembered after clicking “Start synchronization”.</strong>",
        "footer": "The page refreshes every minute while synchronization is running.",
        "date_locale": "en-GB",
        "status_running": "Running",
        "status_success": "Completed",
        "status_error": "Error",
        "status_unknown": "Unknown",
        "message_started": "Synchronization started.",
        "message_progress": "Product synchronization is running.",
        "message_finished": "Synchronization completed.",
        "message_queued": "Synchronization was queued.",
        "message_already_running": "Synchronization is already running.",
        "message_no_changes": "There were no changes to apply.",
        "message_not_found": "The requested page or action was not found.",
        "message_unauthorized": "Access denied. Sign in again.",
        "error_access": "You do not have permission to perform this operation. Notify the system administrator.",
        "error_credentials": "AWS credentials are inactive or expired. Notify the system administrator.",
        "error_rate": "An external service limited the request rate. Try again shortly.",
        "error_timeout": "The operation took too long. Try again shortly.",
        "error_connection": "Could not connect to an external service. Try again shortly.",
        "error_token": "A valid BaseLinker token is not configured. Notify the system administrator.",
        "error_baselinker": "BaseLinker returned an error while loading data. Try again or notify the system administrator.",
        "error_save_config": "Could not save synchronization settings. Try again or notify the system administrator.",
        "error_load_config": "Could not load synchronization settings. Try again or notify the system administrator.",
        "error_format": "The service returned an unexpected response. Notify the system administrator.",
        "error_generic": "A technical problem occurred. Try again or notify the system administrator.",
        "error_missing_fx_param": "The USD/PLN exchange-rate SSM parameter is not configured.",
        "error_missing_account": "AWS_ACCOUNT_ID is not configured for the administration panel.",
        "step_waiting": "waiting",
        "step_ready": "ready",
        "step_running": "running",
        "step_done": "completed",
        "step_skipped": "skipped",
        "step_error": "requires attention",
        "fallback_pre_waiting": "Waiting to start.",
        "fallback_sync_waiting": "Waiting for the difference calculation.",
        "fallback_post_waiting": "Waiting for changes to finish.",
        "summary_placeholder": "A short summary will appear here after the differences are calculated.",
        "progress_pre": "Calculating differences before synchronization. This may take a few minutes.",
        "progress_post": "Checking data consistency after synchronization.",
        "progress_no_changes": "There were no changes to apply.",
        "progress_running": "Completed {done} of {total} changes",
        "progress_finished": "Completed {done} changes",
        "progress_details": "{done_text}: saved {written} records and deleted {deleted} obsolete records.",
        "progress_inactive": "No active synchronization.",
        "counters": "{diff} differences / {changed} to create or update / {missing} new or missing / {extra} to delete / {unchanged} unchanged",
        "queued_pre": "Starting the pre-sync difference calculation.",
        "queued_sync": "Waiting for the difference calculation.",
        "queued_post": "Waiting for synchronization to finish.",
        "queued_summary": "Synchronization started. Differences between Comarch e-Sklep and BaseLinker will be calculated first.",
        "awaiting_status": "Waiting for the first AWS status update.",
        "budget_error": "Could not load the budget: {error}",
        "budget_nbp_rate": " Exchange rate from the last synchronization: NBP{date}, 1 USD = {rate} PLN.",
        "budget_fallback_rate": " Fallback exchange rate{date}: 1 USD = {rate} PLN.",
        "budget_summary": "{percent}% of the monthly limit. {remaining} {currency} remaining.{rate_text}",
        "empty_warehouses": "No warehouses",
        "empty_inventories": "No inventories",
        "config_saved": "Settings were saved and will also be used for future synchronizations.",
        "button_starting": "Starting...",
        "button_loading_config": "Loading settings",
        "button_running": "Synchronization running",
        "button_waiting": "Waiting to start",
        "button_refreshing": "Refreshing...",
        "config_load_http": "Could not load settings ({status})",
        "status_load_http": "Could not load data ({status})",
        "sync_start_http": "Could not start synchronization ({status})",
        "updated_at": "Last update: {date} ({seconds}s ago)",
        "schedule_enabled": "Schedule is enabled",
        "schedule_disabled": "Schedule is disabled",
        "run_id": "Run ID: {id}",
        "currency_pln": "PLN",
        "budget_rate_date": " dated {date}",
        "budget_rate_saved": " saved {date}",
    },
    "pl": {
        "page_title_suffix": "synchronizacja",
        "inventory_fallback": "Katalog {id}",
        "warehouse_fallback": "Magazyn {id}",
        "validation_https": "Adres XML musi zaczynać się od https://",
        "validation_inventory": "Wybierz poprawny katalog Baselinker.",
        "validation_warehouse": "Wybierz poprawny magazyn Baselinker.",
        "validation_warehouse_inventory": "Wybrany magazyn nie jest przypisany do wybranego katalogu.",
        "validation_rpm": "Limit zapytań musi być liczbą od 1 do 100.",
        "attention_post_error": "Kontrola po aktualizacji zakończyła się błędem. Powiadom administratora systemu, żeby sprawdził szczegóły, albo spróbuj uruchomić aktualizację jeszcze raz.",
        "attention_post_diff": "Po aktualizacji wykryto {count} niespójności. Powiadom administratora systemu, żeby sprawdził szczegóły, albo spróbuj uruchomić aktualizację jeszcze raz.",
        "pre_summary": "{diff} różnic, {unchanged} bez zmian",
        "pre_checking": "Sprawdzamy, co trzeba zmienić.",
        "pre_waiting": "Czeka na uruchomienie aktualizacji.",
        "sync_skipped": "Pominięto, bo nie znaleziono różnic.",
        "sync_deleted": "; usunięto {deleted} z {target} zbędnych rekordów",
        "sync_running": "Zapisano {updated} z {target} rekordów{deleted_text}",
        "sync_done": "Zapisano {updated} rekordów{deleted_text}",
        "sync_ready": "Gotowe do rozpoczęcia po policzeniu różnic.",
        "sync_waiting": "Czeka na wynik liczenia różnic.",
        "post_skipped": "Pominięto, bo nie było żadnych zmian do wykonania.",
        "post_clean": "Kontrola zakończona: brak niespójności.",
        "post_checking": "Sprawdzamy, czy dane po aktualizacji są zgodne.",
        "post_waiting": "Czeka na zakończenie aktualizacji.",
        "step_pre": "Liczenie różnic przed aktualizacją",
        "step_sync": "Wprowadzanie zmian w Baselinkerze",
        "step_post": "Kontrola po aktualizacji",
        "summary_diff": "{count} = liczba wykrytych problemów/rozjazdów w danych.",
        "summary_changed": "{count} = liczba rekordów, które trzeba utworzyć albo zaktualizować w BL.",
        "summary_missing": "{count} = część z tych {changed}, które są nowe/brakujące w BL.",
        "summary_extra": "{count} = rekordy istniejące w BL, których nie powinno już być i trzeba je usunąć.",
        "summary_unchanged": "{count} = rekordy bez zmian.",
        "summary_sync_skipped": "Nie wykonano synchronizacji, bo audyt przed aktualizacją nie wykazał różnic.",
        "summary_sync_done": "Synchronizacja: zapisano {updated} rekordów, usunięto {deleted} zbędnych.",
        "summary_post_skipped": "Kontrola po aktualizacji została pominięta, bo nie było zmian do wykonania.",
        "summary_post_clean": "Kontrola po aktualizacji: brak niespójności.",
        "refresh": "Odśwież dane",
        "start": "Uruchom aktualizację",
        "label_status": "Stan",
        "label_progress": "Postęp",
        "label_eta": "Przewidywany koniec",
        "label_next_run": "Następne uruchomienie",
        "label_cost": "Koszt w tym miesiącu",
        "label_completed": "Co zostało zrobione",
        "label_stages": "Etapy aktualizacji",
        "label_settings": "Ustawienia aktualizacji",
        "label_xml": "Link do XML z Comarch e-Sklep",
        "label_inventory": "Katalog Baselinker",
        "label_warehouse": "Magazyn Baselinker",
        "label_rpm": "Zapytań / min",
        "config_note": "Zmiany nie zapisują się podczas edycji. <strong>Zostaną użyte i zapamiętane dopiero po kliknięciu „Uruchom aktualizację”.</strong>",
        "footer": "Strona odświeża dane co minutę, gdy aktualizacja jest w toku.",
        "date_locale": "pl-PL",
        "status_running": "W toku",
        "status_success": "Zakończona",
        "status_error": "Błąd",
        "status_unknown": "Nieznany",
        "message_started": "Aktualizacja została uruchomiona.",
        "message_progress": "Aktualizacja produktów jest w toku.",
        "message_finished": "Aktualizacja zakończona.",
        "message_queued": "Aktualizacja została dodana do kolejki.",
        "message_already_running": "Aktualizacja już trwa.",
        "message_no_changes": "Nie było żadnych zmian do wykonania.",
        "message_not_found": "Nie znaleziono takiej strony lub akcji.",
        "message_unauthorized": "Brak dostępu. Zaloguj się ponownie.",
        "error_access": "Brak uprawnień do wykonania tej operacji. Powiadom administratora systemu.",
        "error_credentials": "Poświadczenia AWS są nieaktywne albo wygasły. Powiadom administratora systemu.",
        "error_rate": "Usługa zewnętrzna ograniczyła liczbę zapytań. Spróbuj ponownie za chwilę.",
        "error_timeout": "Operacja trwała zbyt długo. Spróbuj ponownie za chwilę.",
        "error_connection": "Nie udało się połączyć z usługą zewnętrzną. Spróbuj ponownie za chwilę.",
        "error_token": "Brak poprawnie skonfigurowanego tokenu Baselinker. Powiadom administratora systemu.",
        "error_baselinker": "BaseLinker zwrócił błąd podczas pobierania danych. Spróbuj ponownie albo powiadom administratora systemu.",
        "error_save_config": "Nie udało się zapisać ustawień aktualizacji. Spróbuj ponownie albo powiadom administratora systemu.",
        "error_load_config": "Nie udało się pobrać ustawień aktualizacji. Spróbuj ponownie albo powiadom administratora systemu.",
        "error_format": "Usługa zwróciła odpowiedź w nieoczekiwanym formacie. Powiadom administratora systemu.",
        "error_generic": "Wystąpił problem techniczny. Spróbuj ponownie albo powiadom administratora systemu.",
        "error_missing_fx_param": "Brak nazwy parametru SSM z kursem USD/PLN.",
        "error_missing_account": "Brak AWS_ACCOUNT_ID w konfiguracji panelu.",
        "step_waiting": "czeka",
        "step_ready": "gotowe do startu",
        "step_running": "w toku",
        "step_done": "zakończone",
        "step_skipped": "pominięte",
        "step_error": "wymaga sprawdzenia",
        "fallback_pre_waiting": "Czeka na uruchomienie.",
        "fallback_sync_waiting": "Czeka na wynik liczenia różnic.",
        "fallback_post_waiting": "Czeka na zakończenie zmian.",
        "summary_placeholder": "Tu pojawi się krótkie podsumowanie po policzeniu różnic.",
        "progress_pre": "Liczymy różnice przed aktualizacją. To może potrwać kilka minut.",
        "progress_post": "Sprawdzamy zgodność danych po aktualizacji.",
        "progress_no_changes": "Nie było żadnych zmian do wykonania.",
        "progress_running": "Wykonano {done} z {total} zmian",
        "progress_finished": "Wykonano {done} zmian",
        "progress_details": "{done_text}: zapisano {written} rekordów, usunięto {deleted} zbędnych.",
        "progress_inactive": "Brak aktywnej aktualizacji.",
        "counters": "{diff} rozjazdów / {changed} do utworzenia albo aktualizacji / {missing} nowych lub brakujących / {extra} do usunięcia / {unchanged} bez zmian",
        "queued_pre": "Uruchamiamy liczenie różnic przed aktualizacją.",
        "queued_sync": "Czeka na wynik liczenia różnic.",
        "queued_post": "Czeka na zakończenie aktualizacji.",
        "queued_summary": "Aktualizacja została uruchomiona. Najpierw policzymy różnice między Comarch e-Sklep a Baselinkerem.",
        "awaiting_status": "Oczekujemy na pierwszy zapis statusu z AWS.",
        "budget_error": "Nie udało się pobrać budżetu: {error}",
        "budget_nbp_rate": " Kurs z ostatniego uruchomienia synchronizacji: NBP{date}, 1 USD = {rate} PLN.",
        "budget_fallback_rate": " Kurs awaryjny{date}: 1 USD = {rate} PLN.",
        "budget_summary": "{percent}% miesięcznego limitu. Pozostało {remaining} {currency}.{rate_text}",
        "empty_warehouses": "Brak magazynów",
        "empty_inventories": "Brak katalogów",
        "config_saved": "Ustawienia zostały zapisane i będą używane także przy kolejnych aktualizacjach.",
        "button_starting": "Uruchamianie...",
        "button_loading_config": "Ładowanie ustawień",
        "button_running": "Aktualizacja trwa",
        "button_waiting": "Czekam na start",
        "button_refreshing": "Odświeżanie...",
        "config_load_http": "Nie udało się pobrać ustawień ({status})",
        "status_load_http": "Nie udało się pobrać danych ({status})",
        "sync_start_http": "Nie udało się uruchomić aktualizacji ({status})",
        "updated_at": "Ostatni zapis: {date} ({seconds}s temu)",
        "schedule_enabled": "Harmonogram jest włączony",
        "schedule_disabled": "Harmonogram nie jest włączony",
        "run_id": "Numer uruchomienia: {id}",
        "currency_pln": "zł",
        "budget_rate_date": " z {date}",
        "budget_rate_saved": " zapisany {date}",
    },
}


def _translations() -> dict:
    return TRANSLATIONS.get(ADMIN_LOCALE, TRANSLATIONS["en"])


def _t(key: str, **values) -> str:
    template = _translations().get(key, TRANSLATIONS["en"].get(key, key))
    return template.format(**values)


def _safe_css_color(value: str, fallback: str) -> str:
    cleaned = str(value or "").strip()
    if re.fullmatch(r"#[0-9a-fA-F]{6}", cleaned):
        return cleaned
    return fallback


def _brand_initials() -> str:
    words = re.findall(r"[A-Za-z0-9]+", BRAND_NAME)
    if not words:
        return "CB"
    if len(words) == 1:
        return words[0][:2].upper()
    return (words[0][:1] + words[1][:1]).upper()


def _json_response(status_code: int, payload: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(payload, ensure_ascii=False),
    }


def _html_response(status_code: int, body: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": body,
    }


def _image_response(status_code: int, content_type: str, body: bytes) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": content_type,
            "Cache-Control": "private, max-age=86400",
        },
        "isBase64Encoded": True,
        "body": base64.b64encode(body).decode("ascii"),
    }


def _logo_response() -> dict:
    if not BRAND_LOGO_ENABLED:
        return _json_response(404, {"ok": False, "message": "Brand logo is disabled."})
    try:
        return _image_response(200, "image/png", LOGO_PATH.read_bytes())
    except OSError as exc:
        return _json_response(404, {"ok": False, "message": f"Logo not found: {exc}"})


def _favicon_response() -> dict:
    primary = _safe_css_color(BRAND_PRIMARY_COLOR, "#1673b8")
    initials = html.escape(_brand_initials())
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="12" fill="#eaf3ff"/>
  <text x="32" y="42" text-anchor="middle"
        font-family="Trebuchet MS, Arial, sans-serif"
        font-size="31" font-weight="800" fill="{primary}">{initials}</text>
</svg>"""
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "image/svg+xml; charset=utf-8",
            "Cache-Control": "private, max-age=86400",
        },
        "body": svg,
    }


def _unauthorized() -> dict:
    return {
        "statusCode": 401,
        "headers": {
            "WWW-Authenticate": 'Basic realm="Comarch BaseLinker Sync Admin"',
            "Content-Type": "text/plain; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": "Unauthorized",
    }


def _headers(event: dict) -> dict:
    return {str(k).lower(): str(v) for k, v in (event.get("headers") or {}).items()}


def _is_authorized(event: dict) -> bool:
    expected_hash = ADMIN_PASSWORD_SHA256.strip().lower()
    if expected_hash == "":
        return False

    auth = _headers(event).get("authorization", "")
    if not auth.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception:
        return False

    if not hmac.compare_digest(username, ADMIN_USERNAME):
        return False
    actual_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return hmac.compare_digest(actual_hash, expected_hash)


def _request_info(event: dict) -> tuple[str, str]:
    http = (event.get("requestContext") or {}).get("http") or {}
    method = str(http.get("method") or event.get("httpMethod") or "GET").upper()
    path = str(event.get("rawPath") or event.get("path") or "/")
    return method, path.rstrip("/") or "/"


def _load_status() -> dict:
    try:
        raw = ssm.get_parameter(Name=STATUS_PARAM, WithDecryption=False)["Parameter"]["Value"]
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        return {"status": "unknown", "error": f"{type(exc).__name__}: {exc}"}
    return {"status": "unknown"}


def _next_midnight_pl() -> str:
    now = datetime.now(POLAND_TZ)
    candidate = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate.isoformat()


def _load_schedule() -> dict:
    out = {
        "name": SCHEDULE_NAME,
        "group": SCHEDULE_GROUP,
        "next_run_iso": _next_midnight_pl(),
    }
    try:
        data = scheduler.get_schedule(Name=SCHEDULE_NAME, GroupName=SCHEDULE_GROUP)
        out.update(
            {
                "state": data.get("State"),
                "expression": data.get("ScheduleExpression"),
                "timezone": data.get("ScheduleExpressionTimezone"),
            }
        )
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _parse_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _parse_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip().replace(",", "."))
    except Exception:
        return default


def _first_dict(*items: object) -> dict:
    for item in items:
        if isinstance(item, dict):
            return item
    return {}


def _load_usd_to_pln_rate() -> dict:
    fallback_rate = max(0.0, _parse_float(BUDGET_USD_TO_PLN_RATE, 4.0))
    fallback = {
        "rate": fallback_rate,
        "source": "fallback_env",
        "effective_date": "",
        "fetched_at_iso": "",
        "error": "",
    }
    if BUDGET_FX_RATE_SSM_PARAM.strip() == "":
        fallback["error"] = _t("error_missing_fx_param")
        return fallback
    try:
        raw = ssm.get_parameter(Name=BUDGET_FX_RATE_SSM_PARAM, WithDecryption=False)[
            "Parameter"
        ]["Value"]
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("SSM FX parameter is not a JSON object.")
        rate = _parse_float(payload.get("rate"), 0.0)
        if rate <= 0:
            raise ValueError("SSM FX parameter does not contain a positive rate.")
        return {
            "rate": rate,
            "source": _clean(payload.get("source")) or "ssm",
            "effective_date": _clean(payload.get("effective_date")),
            "fetched_at_iso": _clean(payload.get("fetched_at_iso")),
            "error": _clean(payload.get("error")),
        }
    except Exception as exc:
        fallback["error"] = f"{type(exc).__name__}: {exc}"
        return fallback


def _load_budget_status() -> dict:
    limit_default = max(0.0, _parse_float(BUDGET_LIMIT_USD, 30.0))
    fx = _load_usd_to_pln_rate()
    usd_to_pln_rate = max(0.0, _parse_float(fx.get("rate"), 4.0))
    out = {
        "name": BUDGET_NAME,
        "limit_usd": round(limit_default, 2),
        "spent_usd": 0.0,
        "remaining_usd": round(limit_default, 2),
        "limit_pln": round(limit_default * usd_to_pln_rate, 2),
        "spent_pln": 0.0,
        "remaining_pln": round(limit_default * usd_to_pln_rate, 2),
        "percent_used": 0.0,
        "currency": "PLN",
        "source_currency": "USD",
        "display_currency": "PLN",
        "usd_to_pln_rate": round(usd_to_pln_rate, 4),
        "usd_to_pln_source": fx.get("source", ""),
        "usd_to_pln_effective_date": fx.get("effective_date", ""),
        "usd_to_pln_fetched_at_iso": fx.get("fetched_at_iso", ""),
        "usd_to_pln_error": fx.get("error", ""),
        "source": "aws_budgets",
        "error": "",
    }
    if AWS_ACCOUNT_ID.strip() == "":
        out["error"] = _t("error_missing_account")
        return out
    try:
        data = budgets.describe_budget(AccountId=AWS_ACCOUNT_ID, BudgetName=BUDGET_NAME)
        budget = data.get("Budget", {}) if isinstance(data, dict) else {}
        limit = budget.get("BudgetLimit", {}) if isinstance(budget.get("BudgetLimit"), dict) else {}
        spend = (
            budget.get("CalculatedSpend", {}).get("ActualSpend", {})
            if isinstance(budget.get("CalculatedSpend"), dict)
            else {}
        )
        limit_usd = _parse_float(limit.get("Amount"), limit_default)
        spent_usd = _parse_float(spend.get("Amount"), 0.0)
        remaining_usd = max(0.0, limit_usd - spent_usd)
        source_currency = _clean(spend.get("Unit") or limit.get("Unit") or "USD") or "USD"
        out.update(
            {
                "limit_usd": round(limit_usd, 2),
                "spent_usd": round(spent_usd, 4),
                "remaining_usd": round(remaining_usd, 4),
                "limit_pln": round(limit_usd * usd_to_pln_rate, 2),
                "spent_pln": round(spent_usd * usd_to_pln_rate, 2),
                "remaining_pln": round(remaining_usd * usd_to_pln_rate, 2),
                "percent_used": round((spent_usd * 100.0 / limit_usd), 2)
                if limit_usd > 0
                else 0.0,
                "currency": "PLN",
                "source_currency": source_currency,
                "display_currency": "PLN",
                "usd_to_pln_rate": round(usd_to_pln_rate, 4),
                "usd_to_pln_source": fx.get("source", ""),
                "usd_to_pln_effective_date": fx.get("effective_date", ""),
                "usd_to_pln_fetched_at_iso": fx.get("fetched_at_iso", ""),
                "usd_to_pln_error": fx.get("error", ""),
                "updated_at_iso": datetime.now(POLAND_TZ).isoformat(),
            }
        )
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _default_sync_config() -> dict:
    return {
        "comarch_xml_url": DEFAULT_COMARCH_XML_URL,
        "bl_inventory_id": _parse_int(DEFAULT_BL_INVENTORY_ID, 0),
        "bl_inventory_name": "",
        "bl_warehouse_id": DEFAULT_BL_WAREHOUSE_ID,
        "bl_warehouse_name": "",
        "bl_api_max_rpm": max(1, min(100, _parse_int(DEFAULT_BL_API_MAX_RPM, 90))),
        "source": "lambda_env_defaults",
    }


def _load_sync_config() -> dict:
    config = _default_sync_config()
    if SYNC_CONFIG_PARAM == "":
        return config
    try:
        raw = ssm.get_parameter(Name=SYNC_CONFIG_PARAM, WithDecryption=True)["Parameter"]["Value"]
        parsed = json.loads(raw)
    except Exception:
        return config
    if not isinstance(parsed, dict):
        return config
    if _clean(parsed.get("comarch_xml_url")):
        config["comarch_xml_url"] = _clean(parsed.get("comarch_xml_url"))
    inv_id = _parse_int(parsed.get("bl_inventory_id"), 0)
    if inv_id > 0:
        config["bl_inventory_id"] = inv_id
    if _clean(parsed.get("bl_inventory_name")):
        config["bl_inventory_name"] = _clean(parsed.get("bl_inventory_name"))
    if _clean(parsed.get("bl_warehouse_id")):
        config["bl_warehouse_id"] = _clean(parsed.get("bl_warehouse_id"))
    if _clean(parsed.get("bl_warehouse_name")):
        config["bl_warehouse_name"] = _clean(parsed.get("bl_warehouse_name"))
    rpm = _parse_int(parsed.get("bl_api_max_rpm"), 0)
    if rpm > 0:
        config["bl_api_max_rpm"] = max(1, min(100, rpm))
    config["source"] = "ssm_parameter"
    return config


def _save_sync_config(config: dict) -> None:
    payload = {
        "comarch_xml_url": _clean(config.get("comarch_xml_url")),
        "bl_inventory_id": int(config.get("bl_inventory_id") or 0),
        "bl_inventory_name": _clean(config.get("bl_inventory_name")),
        "bl_warehouse_id": _clean(config.get("bl_warehouse_id")),
        "bl_warehouse_name": _clean(config.get("bl_warehouse_name")),
        "bl_api_max_rpm": int(config.get("bl_api_max_rpm") or 90),
        "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        "updated_by": "admin_panel",
    }
    ssm.put_parameter(
        Name=SYNC_CONFIG_PARAM,
        Value=json.dumps(payload, ensure_ascii=False),
        Type="String",
        Overwrite=True,
        Tier="Standard",
    )


def _get_bl_api_token() -> str:
    raw = ssm.get_parameter(Name=BL_API_TOKEN_SSM_PARAM, WithDecryption=True)["Parameter"]["Value"]
    token = _clean(raw)
    if token == "":
        raise RuntimeError("BL API token is empty.")
    return token


def _bl_api_call(method: str, parameters: Optional[dict] = None, timeout_sec: int = 30) -> dict:
    payload = urllib.parse.urlencode(
        {
            "method": method,
            "parameters": json.dumps(parameters or {}, ensure_ascii=False),
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        BL_API_URL,
        data=payload,
        headers={
            "X-BLToken": _get_bl_api_token(),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": "comarch-baselinker-sync-admin/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"{method}: malformed response")
    if data.get("status") != "SUCCESS":
        raise RuntimeError(f"{method}: {data.get('error_message', 'unknown error')}")
    return data


def _name_from_row(row: dict, fallback: str) -> str:
    for key in ("name", "inventory_name", "warehouse_name", "description"):
        value = _clean(row.get(key))
        if value:
            return value
    return fallback


def _load_bl_options() -> dict:
    inventories_data = _bl_api_call("getInventories")
    warehouses_data = _bl_api_call("getInventoryWarehouses")

    inventories = []
    for row in inventories_data.get("inventories", []):
        if not isinstance(row, dict):
            continue
        inv_id = _parse_int(row.get("inventory_id"), 0)
        if inv_id <= 0:
            continue
        raw_warehouses = row.get("warehouses", [])
        warehouse_ids = []
        if isinstance(raw_warehouses, list):
            warehouse_ids = [_clean(item) for item in raw_warehouses if _clean(item)]
        inventories.append(
            {
                "id": inv_id,
                "name": _name_from_row(row, _t("inventory_fallback", id=inv_id)),
                "warehouse_ids": warehouse_ids,
            }
        )

    referenced_warehouse_ids = []
    for inventory in inventories:
        for warehouse_id in inventory.get("warehouse_ids", []):
            if warehouse_id and warehouse_id not in referenced_warehouse_ids:
                referenced_warehouse_ids.append(warehouse_id)

    warehouse_name_by_raw_id = {}
    raw_warehouses = warehouses_data.get("warehouses", [])
    rows = []
    if isinstance(raw_warehouses, dict):
        rows = [
            {**value, "warehouse_id": key} if isinstance(value, dict) else {"warehouse_id": key, "name": value}
            for key, value in raw_warehouses.items()
        ]
    elif isinstance(raw_warehouses, list):
        rows = raw_warehouses
    for row in rows:
        if not isinstance(row, dict):
            continue
        warehouse_id = _clean(row.get("warehouse_id") or row.get("id"))
        if warehouse_id == "":
            continue
        warehouse_name_by_raw_id[warehouse_id] = _name_from_row(
            row,
            _t("warehouse_fallback", id=warehouse_id),
        )

    warehouses = []
    used_warehouse_ids = set()
    for warehouse_id in referenced_warehouse_ids:
        raw_id = warehouse_id.split("_", 1)[1] if "_" in warehouse_id else warehouse_id
        name = (
            warehouse_name_by_raw_id.get(warehouse_id)
            or warehouse_name_by_raw_id.get(raw_id)
            or _t("warehouse_fallback", id=warehouse_id)
        )
        warehouses.append({"id": warehouse_id, "name": name})
        used_warehouse_ids.add(warehouse_id)

    for raw_id, name in warehouse_name_by_raw_id.items():
        prefixed_candidates = {raw_id, f"bl_{raw_id}", f"shop_{raw_id}"}
        if prefixed_candidates & used_warehouse_ids:
            continue
        warehouses.append({"id": raw_id, "name": name})

    inventories.sort(key=lambda item: item["name"].lower())
    warehouses.sort(key=lambda item: item["name"].lower())
    return {"inventories": inventories, "warehouses": warehouses}


def _config_payload() -> dict:
    config = _load_sync_config()
    options = _load_bl_options()
    return {
        "config": config,
        "defaults": _default_sync_config(),
        "options": options,
    }


def _request_json(event: dict) -> dict:
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    if not isinstance(raw, str) or raw.strip() == "":
        return {}
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, dict) else {}


def _validate_sync_config(config_raw: dict, options: dict) -> dict:
    comarch_url = _clean(config_raw.get("comarch_xml_url"))
    if not comarch_url.startswith("https://"):
        raise ValueError(_t("validation_https"))

    inventory_id = _parse_int(config_raw.get("bl_inventory_id"), 0)
    inventory_by_id = {int(item["id"]): item for item in options.get("inventories", [])}
    if inventory_id <= 0 or inventory_id not in inventory_by_id:
        raise ValueError(_t("validation_inventory"))
    inventory = inventory_by_id[inventory_id]

    warehouse_id = _clean(config_raw.get("bl_warehouse_id"))
    warehouse_by_id = {str(item["id"]): item for item in options.get("warehouses", [])}
    if warehouse_id == "" or warehouse_id not in warehouse_by_id:
        raise ValueError(_t("validation_warehouse"))
    allowed_warehouses = set(inventory.get("warehouse_ids") or [])
    if allowed_warehouses and warehouse_id not in allowed_warehouses:
        raise ValueError(_t("validation_warehouse_inventory"))

    rpm = _parse_int(config_raw.get("bl_api_max_rpm"), 90)
    if rpm < 1 or rpm > 100:
        raise ValueError(_t("validation_rpm"))

    return {
        "comarch_xml_url": comarch_url,
        "bl_inventory_id": inventory_id,
        "bl_inventory_name": inventory["name"],
        "bl_warehouse_id": warehouse_id,
        "bl_warehouse_name": warehouse_by_id[warehouse_id]["name"],
        "bl_api_max_rpm": rpm,
    }


def _summarize_status(status: dict) -> dict:
    current_status = _clean(status.get("status", ""))
    progress = status.get("progress") if isinstance(status.get("progress"), dict) else {}
    result = status.get("result") if isinstance(status.get("result"), dict) else {}
    running_without_progress = current_status == "running" and not progress
    usable_result = {} if running_without_progress else result
    usable_status = {} if running_without_progress else status
    source = progress if progress else usable_result
    pre_audit_summary = _first_dict(
        source.get("pre_audit_summary"),
        usable_result.get("pre_audit_summary"),
        progress.get("pre_audit_summary"),
        usable_status.get("pre_audit_summary"),
    )
    post_audit_summary = _first_dict(
        source.get("post_audit_summary"),
        usable_result.get("post_audit_summary"),
        progress.get("post_audit_summary"),
        usable_status.get("post_audit_summary"),
    )

    total = int(source.get("global_total_records") or 0)
    processed = int(source.get("global_processed") or 0)
    requested = int(source.get("global_requested") or 0)
    updated = int(source.get("global_updated") or 0)
    delete_target_from_audit = int(pre_audit_summary.get("extra_in_bl") or 0)
    delete_target = int(source.get("global_delete_target") or 0) or delete_target_from_audit
    delete_requested = int(source.get("global_delete_requested") or 0)
    delete_deleted = int(source.get("global_delete_deleted") or 0)
    delete_failed = int(source.get("global_delete_failed") or 0)
    if (
        delete_deleted <= 0
        and delete_target > 0
        and _clean(status.get("status")) == "success"
        and post_audit_summary
        and int(post_audit_summary.get("diff_total") or 0) == 0
    ):
        delete_deleted = delete_target
    changed_target = int(source.get("global_changed_target") or 0)
    write_target = changed_target if changed_target > 0 else total
    mutation_total = max(0, write_target + delete_target)
    mutation_done = max(0, requested + delete_deleted)
    if mutation_total > 0:
        percent = round((min(mutation_done, mutation_total) * 100.0 / mutation_total), 2)
    else:
        percent = round((processed * 100.0 / total), 2) if total > 0 else 0.0
    audit_unchanged = int(
        pre_audit_summary.get("unchanged_records")
        or post_audit_summary.get("unchanged_records")
        or 0
    )
    skipped_unchanged = int(source.get("global_skipped_unchanged") or 0)
    sync_skipped_no_changes = bool(
        source.get("sync_skipped_no_changes")
        or usable_result.get("sync_skipped_no_changes")
        or usable_status.get("sync_skipped_no_changes")
    )
    post_audit_skipped_no_changes = bool(
        source.get("post_audit_skipped_no_changes")
        or usable_result.get("post_audit_skipped_no_changes")
        or usable_status.get("post_audit_skipped_no_changes")
    )
    pre_audit_executed = bool(
        source.get("pre_audit_executed")
        or usable_result.get("pre_audit_executed")
        or pre_audit_summary
    )
    post_audit_executed = bool(
        source.get("post_audit_executed")
        or usable_result.get("post_audit_executed")
        or post_audit_summary
    )
    phase = source.get("phase") or usable_result.get("phase") or usable_status.get("phase") or ""
    sync_stage = source.get("sync_stage") or usable_result.get("sync_stage") or usable_status.get("sync_stage") or ""
    if sync_stage == "":
        if str(phase).startswith("pre_audit"):
            sync_stage = "pre_audit"
        elif str(phase).startswith("post_audit"):
            sync_stage = "post_audit"
        elif current_status == "running":
            has_sync_activity = bool(
                pre_audit_executed
                or pre_audit_summary
                or requested > 0
                or updated > 0
                or delete_requested > 0
                or delete_deleted > 0
                or sync_skipped_no_changes
                or post_audit_executed
                or post_audit_summary
            )
            sync_stage = "sync" if has_sync_activity else "pre_audit"
        elif current_status == "success":
            sync_stage = "finished"
        else:
            sync_stage = "unknown"

    pre_diff = int(pre_audit_summary.get("diff_total") or 0)
    pre_missing = int(pre_audit_summary.get("missing_in_bl") or 0)
    pre_changed = int(pre_audit_summary.get("changed_records") or 0)
    pre_extra = int(pre_audit_summary.get("extra_in_bl") or 0)
    post_diff = int(post_audit_summary.get("diff_total") or 0)
    post_error = _clean(
        source.get("post_audit_error")
        or usable_result.get("post_audit_error")
        or usable_status.get("post_audit_error")
    )
    post_audit_has_inconsistencies = bool(post_audit_summary and post_diff > 0)
    needs_admin_attention = bool(post_error or post_audit_has_inconsistencies)
    admin_attention_message = ""
    if post_error:
        admin_attention_message = _t("attention_post_error")
    elif post_audit_has_inconsistencies:
        admin_attention_message = _t("attention_post_diff", count=post_diff)

    def _step_status(step: str) -> str:
        if step == "pre_audit":
            if _clean(source.get("pre_audit_error") or usable_result.get("pre_audit_error")):
                return "error"
            if sync_stage == "pre_audit" and current_status == "running":
                return "running"
            return "done" if pre_audit_executed else "waiting"
        if step == "sync":
            if sync_skipped_no_changes:
                return "skipped"
            if sync_stage == "sync" and current_status == "running":
                return "running"
            if sync_stage == "post_audit" and current_status == "running":
                return "done"
            if current_status in {"success", "error"} or post_audit_executed or post_audit_skipped_no_changes:
                return "done"
            return "waiting" if not pre_audit_executed else "ready"
        if step == "post_audit":
            if needs_admin_attention:
                return "error"
            if post_audit_skipped_no_changes:
                return "skipped"
            if sync_stage == "post_audit" and current_status == "running":
                return "running"
            return "done" if post_audit_executed else "waiting"
        return "waiting"

    pre_audit_step_status = _step_status("pre_audit")
    sync_step_status = _step_status("sync")
    post_audit_step_status = _step_status("post_audit")

    def _pre_audit_summary_text() -> str:
        if pre_audit_summary:
            return _t("pre_summary", diff=pre_diff, unchanged=audit_unchanged)
        if pre_audit_step_status == "running":
            return _t("pre_checking")
        return _t("pre_waiting")

    def _sync_summary_text() -> str:
        if sync_skipped_no_changes:
            return _t("sync_skipped")
        delete_text = ""
        if delete_target > 0 or delete_deleted > 0:
            delete_text = _t(
                "sync_deleted",
                deleted=delete_deleted,
                target=delete_target,
            )
        if sync_step_status == "running":
            return _t(
                "sync_running",
                updated=updated,
                target=write_target,
                deleted_text=delete_text,
            )
        if sync_step_status == "done":
            return _t("sync_done", updated=updated, deleted_text=delete_text)
        if sync_step_status == "ready":
            return _t("sync_ready")
        return _t("sync_waiting")

    def _post_audit_summary_text() -> str:
        if needs_admin_attention:
            return admin_attention_message
        if post_audit_skipped_no_changes:
            return _t("post_skipped")
        if post_audit_summary:
            return _t("post_clean")
        if post_audit_step_status == "running":
            return _t("post_checking")
        return _t("post_waiting")

    steps = [
        {
            "key": "pre_audit",
            "label": _t("step_pre"),
            "status": pre_audit_step_status,
            "summary": _pre_audit_summary_text(),
        },
        {
            "key": "sync",
            "label": _t("step_sync"),
            "status": sync_step_status,
            "summary": _sync_summary_text(),
        },
        {
            "key": "post_audit",
            "label": _t("step_post"),
            "status": post_audit_step_status,
            "summary": _post_audit_summary_text(),
        },
    ]

    summary_lines = []
    if pre_audit_summary:
        summary_lines.append(_t("summary_diff", count=pre_diff))
        summary_lines.append(_t("summary_changed", count=pre_changed))
        summary_lines.append(
            _t("summary_missing", count=pre_missing, changed=pre_changed)
        )
        summary_lines.append(_t("summary_extra", count=pre_extra))
        summary_lines.append(_t("summary_unchanged", count=audit_unchanged))
    if sync_skipped_no_changes:
        summary_lines.append(_t("summary_sync_skipped"))
    elif requested > 0 or updated > 0 or delete_deleted > 0:
        summary_lines.append(
            _t(
                "summary_sync_done",
                updated=updated,
                deleted=delete_deleted,
            )
        )
    if post_audit_skipped_no_changes:
        summary_lines.append(_t("summary_post_skipped"))
    elif needs_admin_attention:
        summary_lines.append(admin_attention_message)
    elif post_audit_summary:
        summary_lines.append(_t("summary_post_clean"))

    updated_unix = int(status.get("updated_at_unix") or 0)
    updated_age_sec = 0
    if updated_unix > 0:
        updated_age_sec = max(0, int(datetime.now(timezone.utc).timestamp()) - updated_unix)

    return {
        "status": status.get("status", "unknown"),
        "run_id": status.get("run_id", ""),
        "updated_at_iso": status.get("updated_at_iso", ""),
        "updated_age_sec": updated_age_sec,
        "phase": phase,
        "sync_stage": sync_stage,
        "global_total_records": total,
        "global_processed": processed,
        "progress_percent": percent,
        "global_requested": requested,
        "global_updated": updated,
        "global_skipped_unchanged": skipped_unchanged,
        "global_changed_target": changed_target,
        "global_write_target": write_target,
        "global_delete_target": delete_target,
        "global_delete_requested": delete_requested,
        "global_delete_deleted": delete_deleted,
        "global_delete_failed": delete_failed,
        "mutation_total": mutation_total,
        "mutation_done": mutation_done,
        "unchanged_records": audit_unchanged or skipped_unchanged,
        "unchanged_records_source": "pre_audit" if audit_unchanged else "runtime",
        "pre_audit_summary": pre_audit_summary,
        "post_audit_summary": post_audit_summary,
        "post_audit_diff_total": post_diff,
        "post_audit_error": post_error,
        "pre_audit_executed": pre_audit_executed,
        "sync_skipped_no_changes": sync_skipped_no_changes,
        "post_audit_executed": post_audit_executed,
        "post_audit_skipped_no_changes": post_audit_skipped_no_changes,
        "post_audit_has_inconsistencies": post_audit_has_inconsistencies,
        "needs_admin_attention": needs_admin_attention,
        "admin_attention_message": admin_attention_message,
        "steps": steps,
        "summary_lines": summary_lines,
        "global_errors_count": int(source.get("global_errors_count") or 0),
        "eta_finish_iso": source.get("eta_finish_iso") or usable_result.get("eta_finish_iso"),
        "message": status.get("message") or "",
    }


def _status_payload() -> dict:
    raw_status = _load_status()
    return {
        "sync": _summarize_status(raw_status),
        "raw_status": raw_status,
        "schedule": _load_schedule(),
        "budget": _load_budget_status(),
        "server_time_iso": datetime.now(POLAND_TZ).isoformat(),
    }


def _trigger_sync(event: dict) -> dict:
    current = _summarize_status(_load_status())
    if current.get("status") == "running" and int(current.get("updated_age_sec") or 0) < 3600:
        return _json_response(
            409,
            {
                "ok": False,
                "message": "Sync is already running.",
                "sync": current,
            },
        )

    try:
        body = _request_json(event)
        options = _load_bl_options()
        next_config = _validate_sync_config(
            body.get("config") if isinstance(body.get("config"), dict) else _load_sync_config(),
            options,
        )
        _save_sync_config(next_config)
    except ValueError as exc:
        return _json_response(400, {"ok": False, "message": str(exc)})
    except Exception as exc:
        return _json_response(
            500,
            {"ok": False, "message": f"Failed to save sync config: {type(exc).__name__}: {exc}"},
        )

    response = lambda_api.invoke(
        FunctionName=SYNC_FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(
            {
                "source": "admin_panel",
                "reason": "manual_on_demand_sync",
                "sync_config_saved": True,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
    )
    return _json_response(
        202,
        {
            "ok": True,
            "status_code": response.get("StatusCode"),
            "request_id": (response.get("ResponseMetadata") or {}).get("RequestId"),
            "message": "Sync queued.",
            "config": next_config,
        },
    )


def _page() -> str:
    primary = _safe_css_color(BRAND_PRIMARY_COLOR, "#1673b8")
    primary_dark = _safe_css_color(BRAND_PRIMARY_DARK_COLOR, "#0f5d96")
    secondary = _safe_css_color(BRAND_SECONDARY_COLOR, "#183c5c")
    locale = html.escape(ADMIN_LOCALE)
    i18n_json = json.dumps(_translations(), ensure_ascii=False).replace("<", "\\u003c")
    brand_name = html.escape(BRAND_NAME)
    panel_title = html.escape(BRAND_PANEL_TITLE)
    panel_subtitle = html.escape(BRAND_PANEL_SUBTITLE)
    initials = html.escape(_brand_initials())
    if BRAND_LOGO_ENABLED:
        brand_visual = (
            '<div class="brand-logo">'
            '<img src="/assets/client-logo.png" alt="' + brand_name + '">'
            "</div>"
        )
    else:
        brand_visual = f'<div class="brand-mark" aria-hidden="true">{initials}</div>'

    template = """<!doctype html>
<html lang="__LOCALE__">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__BRAND_NAME__ - __PAGE_TITLE_SUFFIX__</title>
  <link rel="icon" href="/assets/favicon.svg" type="image/svg+xml">
  <style>
    :root {
      --orange: __PRIMARY_COLOR__;
      --orange-dark: __PRIMARY_DARK_COLOR__;
      --navy: __SECONDARY_COLOR__;
      --danger: #b3261e;
      --ink: #263652;
      --muted: #6a6a6a;
      --line: #e8e2df;
      --panel: #ffffff;
      --soft: #f7f4f2;
      --good: #0d7a4f;
      --warn: #b66a00;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background: linear-gradient(180deg, #fff 0%, var(--soft) 100%);
      font-family: "Trebuchet MS", "Segoe UI", sans-serif;
    }
    .topbar { height: 5px; background: var(--orange); }
    main { max-width: 1120px; margin: 0 auto; padding: 28px 18px 42px; }
    header {
      display: flex;
      gap: 18px;
      align-items: center;
      justify-content: space-between;
      padding: 18px 0 24px;
      border-bottom: 1px solid var(--line);
    }
    .brand { display: flex; align-items: center; gap: 18px; }
    .brand-mark {
      display: grid;
      width: 52px;
      height: 52px;
      flex: 0 0 52px;
      place-items: center;
      border-radius: 14px;
      color: #fff;
      background: linear-gradient(145deg, #2698d8, var(--orange));
      box-shadow: 0 12px 28px rgba(22, 115, 184, .24);
      font-weight: 900;
      letter-spacing: -.04em;
    }
    .brand-logo {
      display: flex;
      align-items: center;
      justify-content: center;
      width: 178px;
      max-width: 44vw;
      min-height: 44px;
      padding: 4px 0;
    }
    .brand-logo img {
      display: block;
      width: 100%;
      height: auto;
    }
    h1 { margin: 0; font-size: 25px; line-height: 1.1; color: var(--navy); }
    .subtitle { margin-top: 5px; color: var(--muted); font-size: 14px; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
    button {
      appearance: none;
      border: 1px solid var(--ink);
      background: #fff;
      color: var(--ink);
      min-height: 42px;
      padding: 0 16px;
      font-weight: 700;
      cursor: pointer;
      border-radius: 2px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 9px;
    }
    button.primary { border-color: var(--orange); background: var(--orange); color: #fff; }
    button.primary:hover { background: var(--orange-dark); }
    button:disabled { opacity: .45; cursor: not-allowed; }
    .spinner {
      width: 16px;
      height: 16px;
      border: 2px solid currentColor;
      border-right-color: transparent;
      border-radius: 50%;
      display: none;
      animation: spin .8s linear infinite;
    }
    button.loading .spinner { display: inline-block; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 22px 0;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 4px;
      padding: 16px;
      min-height: 118px;
    }
    .panel.wide { grid-column: span 2; }
    .panel.config { grid-column: 1 / -1; }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .08em;
      margin-bottom: 10px;
    }
    .value { font-size: 24px; font-weight: 800; overflow-wrap: anywhere; }
    .small { color: var(--muted); font-size: 13px; margin-top: 8px; overflow-wrap: anywhere; }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 24px;
      font-weight: 800;
    }
    .dot { width: 11px; height: 11px; border-radius: 999px; background: var(--muted); }
    .dot.running { background: var(--warn); }
    .dot.success { background: var(--good); }
    .dot.error { background: var(--danger); }
    .bar {
      width: 100%;
      height: 10px;
      background: #ece8e5;
      border-radius: 999px;
      overflow: hidden;
      margin-top: 14px;
    }
    .bar span { display: block; height: 100%; width: 0%; background: var(--orange); }
    .steps {
      display: grid;
      gap: 10px;
      margin-top: 8px;
    }
    .step {
      display: grid;
      grid-template-columns: 20px 1fr;
      gap: 10px;
      align-items: start;
      padding: 10px;
      border: 1px solid var(--line);
      background: #fffaf6;
      border-radius: 4px;
    }
    .step-mark {
      width: 20px;
      height: 20px;
      border-radius: 50%;
      border: 2px solid var(--muted);
      margin-top: 1px;
    }
    .step.running .step-mark {
      border-color: var(--orange);
      border-right-color: transparent;
      animation: spin .8s linear infinite;
    }
    .step.done .step-mark { border-color: var(--good); background: var(--good); }
    .step.skipped .step-mark { border-color: var(--muted); background: #f0ece8; }
    .step.error .step-mark { border-color: var(--danger); background: var(--danger); }
    .step.error {
      border-color: rgba(179, 38, 30, .45);
      background: #fff1ef;
      box-shadow: inset 3px 0 0 var(--danger);
    }
    .step.error .step-title { color: var(--danger); }
    .step.error .step-summary {
      color: #7d1b16;
      font-weight: 700;
    }
    .step-title { font-weight: 800; color: var(--navy); }
    .step-summary { margin-top: 3px; color: var(--muted); font-size: 13px; }
    .attention-alert {
      margin-top: 12px;
      padding: 11px 12px;
      border: 1px solid rgba(179, 38, 30, .45);
      background: #fff1ef;
      color: #7d1b16;
      border-radius: 4px;
      font-size: 13px;
      font-weight: 700;
      line-height: 1.4;
    }
    .attention-alert.hidden { display: none; }
    .summary-list {
      margin-top: 12px;
      padding-top: 10px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .budget-ok { color: var(--good); }
    .budget-warn { color: var(--warn); }
    .budget-danger { color: var(--danger); }
    .config-grid {
      display: grid;
      grid-template-columns: 2fr 1fr 1fr 140px;
      gap: 12px;
      align-items: end;
    }
    .field label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
      font-weight: 700;
    }
    input, select {
      width: 100%;
      min-height: 42px;
      border: 1px solid var(--line);
      border-radius: 2px;
      background: #fff;
      color: var(--ink);
      padding: 0 11px;
      font: inherit;
    }
    input:focus, select:focus {
      outline: 2px solid rgba(216, 111, 32, .22);
      border-color: var(--orange);
    }
    .config-note { margin-top: 12px; color: var(--muted); font-size: 13px; }
    .config-note strong { color: var(--ink); }
    .foot { margin-top: 18px; color: var(--muted); font-size: 12px; }
    @media (max-width: 760px) {
      header { align-items: flex-start; flex-direction: column; }
      .actions { justify-content: flex-start; width: 100%; }
      button { width: 100%; }
      .grid { grid-template-columns: 1fr; }
      .panel.wide { grid-column: span 1; }
      .panel.config { grid-column: span 1; }
      .config-grid { grid-template-columns: 1fr; }
      h1 { font-size: 22px; }
      .brand-mark { width: 46px; height: 46px; flex-basis: 46px; }
      .brand-logo { width: 152px; max-width: 70vw; }
    }
  </style>
</head>
<body>
  <div class="topbar"></div>
  <main>
    <header>
      <div class="brand">
        __BRAND_VISUAL__
        <div>
          <h1>__PANEL_TITLE__</h1>
          <div class="subtitle">__PANEL_SUBTITLE__</div>
        </div>
      </div>
      <div class="actions">
        <button id="refreshBtn"><span class="spinner"></span><span class="btnText">__REFRESH__</span></button>
        <button id="syncBtn" class="primary"><span class="spinner"></span><span class="btnText">__START__</span></button>
      </div>
    </header>

    <section class="grid">
      <div class="panel">
        <div class="label">__LABEL_STATUS__</div>
        <div class="status"><span id="statusDot" class="dot"></span><span id="statusText">...</span></div>
        <div id="message" class="small"></div>
        <div id="attentionAlert" class="attention-alert hidden"></div>
      </div>
      <div class="panel">
        <div class="label">__LABEL_PROGRESS__</div>
        <div id="progressText" class="value">0%</div>
        <div class="bar"><span id="progressBar"></span></div>
        <div id="recordsText" class="small"></div>
      </div>
      <div class="panel">
        <div class="label">__LABEL_ETA__</div>
        <div id="etaText" class="value">-</div>
        <div id="updatedText" class="small"></div>
      </div>
      <div class="panel">
        <div class="label">__LABEL_NEXT_RUN__</div>
        <div id="nextRunText" class="value">-</div>
        <div id="scheduleText" class="small"></div>
      </div>
      <div class="panel">
        <div class="label">__LABEL_COST__</div>
        <div id="budgetText" class="value">-</div>
        <div id="budgetSmall" class="small"></div>
      </div>
      <div class="panel wide">
        <div class="label">__LABEL_COMPLETED__</div>
        <div id="countersText" class="value">-</div>
        <div id="runText" class="small"></div>
      </div>
      <div class="panel wide">
        <div class="label">__LABEL_STAGES__</div>
        <div id="stepsList" class="steps"></div>
        <div id="summaryText" class="summary-list"></div>
      </div>
      <div class="panel config">
        <div class="label">__LABEL_SETTINGS__</div>
        <div class="config-grid">
          <div class="field">
            <label for="comarchUrlInput">__LABEL_XML__</label>
            <input id="comarchUrlInput" type="url" placeholder="https://...">
          </div>
          <div class="field">
            <label for="inventorySelect">__LABEL_INVENTORY__</label>
            <select id="inventorySelect"></select>
          </div>
          <div class="field">
            <label for="warehouseSelect">__LABEL_WAREHOUSE__</label>
            <select id="warehouseSelect"></select>
          </div>
          <div class="field">
            <label for="rpmInput">__LABEL_RPM__</label>
            <input id="rpmInput" type="number" min="1" max="100" step="1">
          </div>
        </div>
        <div id="configNote" class="config-note">__CONFIG_NOTE__</div>
      </div>
    </section>
    <div class="foot">__FOOTER__</div>
  </main>
  <script>
    const I18N = __I18N_JSON__;
    const statusText = document.getElementById('statusText');
    const statusDot = document.getElementById('statusDot');
    const progressText = document.getElementById('progressText');
    const progressBar = document.getElementById('progressBar');
    const recordsText = document.getElementById('recordsText');
    const etaText = document.getElementById('etaText');
    const updatedText = document.getElementById('updatedText');
    const nextRunText = document.getElementById('nextRunText');
    const scheduleText = document.getElementById('scheduleText');
    const budgetText = document.getElementById('budgetText');
    const budgetSmall = document.getElementById('budgetSmall');
    const countersText = document.getElementById('countersText');
    const runText = document.getElementById('runText');
    const stepsList = document.getElementById('stepsList');
    const summaryText = document.getElementById('summaryText');
    const comarchUrlInput = document.getElementById('comarchUrlInput');
    const inventorySelect = document.getElementById('inventorySelect');
    const warehouseSelect = document.getElementById('warehouseSelect');
    const rpmInput = document.getElementById('rpmInput');
    const configNote = document.getElementById('configNote');
    const message = document.getElementById('message');
    const attentionAlert = document.getElementById('attentionAlert');
    const syncBtn = document.getElementById('syncBtn');
    const refreshBtn = document.getElementById('refreshBtn');
    let timer = null;
    let isTriggering = false;
    let isRefreshing = false;
    let isConfigLoading = true;
    let triggerLockUntil = 0;
    let forcePollingUntil = 0;
    let configOptions = { inventories: [], warehouses: [] };
    let lastSync = {};

    function tr(key, values = {}) {
      let text = String(I18N[key] || key);
      Object.entries(values).forEach(([name, value]) => {
        text = text.replaceAll(`{${name}}`, String(value));
      });
      return text;
    }

    function fmt(value) {
      if (!value) return '-';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString(I18N.date_locale || 'en-GB');
    }

    function statusLabel(status) {
      const map = {
        running: tr('status_running'),
        success: tr('status_success'),
        error: tr('status_error'),
        unknown: tr('status_unknown')
      };
      return map[status] || tr('status_unknown');
    }

    function messageLabel(text) {
      const raw = String(text || '').trim();
      if (!raw) return '';
      const map = {
        'Sync started.': tr('message_started'),
        'Sync in progress.': tr('message_progress'),
        'Sync finished.': tr('message_finished'),
        'Sync queued.': tr('message_queued'),
        'Sync is already running.': tr('message_already_running'),
        'Sync skipped; pre-audit diff_total=0.': tr('message_no_changes'),
        'Not found': tr('message_not_found'),
        'Unauthorized': tr('message_unauthorized')
      };
      if (map[raw]) return map[raw];

      const lower = raw.toLowerCase();
      if (lower.includes('accessdenied') || lower.includes('not authorized') || lower.includes('unauthorized')) {
        return tr('error_access');
      }
      if (lower.includes('expiredtoken') || lower.includes('security token') || lower.includes('invalidclienttokenid')) {
        return tr('error_credentials');
      }
      if (lower.includes('throttl') || lower.includes('too many requests') || lower.includes('rate limit') || lower.includes('429')) {
        return tr('error_rate');
      }
      if (lower.includes('timeout') || lower.includes('timed out')) {
        return tr('error_timeout');
      }
      if (lower.includes('failed to fetch') || lower.includes('networkerror') || lower.includes('could not connect') || lower.includes('endpointconnection') || lower.includes('connection')) {
        return tr('error_connection');
      }
      if (lower.includes('bl api token') || lower.includes('bltoken') || lower.includes('api token')) {
        return tr('error_token');
      }
      if (lower.includes('baselinker') || lower.includes('getinventories') || lower.includes('getinventorywarehouses')) {
        return tr('error_baselinker');
      }
      if (lower.includes('failed to save sync config')) {
        return tr('error_save_config');
      }
      if (lower.includes('failed to load sync config')) {
        return tr('error_load_config');
      }
      if (lower.includes('malformed') || lower.includes('json') || lower.includes('decode')) {
        return tr('error_format');
      }
      return tr('error_generic');
    }

    function statusTextForStep(status) {
      const map = {
        waiting: tr('step_waiting'),
        ready: tr('step_ready'),
        running: tr('step_running'),
        done: tr('step_done'),
        skipped: tr('step_skipped'),
        error: tr('step_error')
      };
      return map[status] || status || tr('step_waiting');
    }

    function renderAttention(sync) {
      const text = sync && sync.needs_admin_attention ? sync.admin_attention_message : '';
      if (text) {
        attentionAlert.textContent = text;
        attentionAlert.classList.remove('hidden');
      } else {
        attentionAlert.textContent = '';
        attentionAlert.classList.add('hidden');
      }
    }

    function renderSteps(steps, summaryLines) {
      const safeSteps = Array.isArray(steps) && steps.length ? steps : [
        { label: tr('step_pre'), status: 'waiting', summary: tr('fallback_pre_waiting') },
        { label: tr('step_sync'), status: 'waiting', summary: tr('fallback_sync_waiting') },
        { label: tr('step_post'), status: 'waiting', summary: tr('fallback_post_waiting') }
      ];
      stepsList.innerHTML = safeSteps.map((step) => `
        <div class="step ${step.status || 'waiting'}">
          <div class="step-mark" aria-hidden="true"></div>
          <div>
            <div class="step-title">${escapeHtml(step.label || '-')} · ${escapeHtml(statusTextForStep(step.status))}</div>
            <div class="step-summary">${escapeHtml(step.summary || '')}</div>
          </div>
        </div>
      `).join('');
      const lines = Array.isArray(summaryLines) ? summaryLines.filter(Boolean) : [];
      summaryText.innerHTML = lines.length
        ? lines.map((line) => `<div>${escapeHtml(line)}</div>`).join('')
        : tr('summary_placeholder');
    }

    function renderProgressDetails(sync) {
      const stage = sync.sync_stage || '';
      if (stage === 'pre_audit') {
        return tr('progress_pre');
      }
      if (stage === 'post_audit') {
        return tr('progress_post');
      }
      if (sync.sync_skipped_no_changes) {
        return tr('progress_no_changes');
      }
      const writeTarget = Number(sync.global_write_target || sync.global_total_records || 0);
      const written = Number(sync.global_updated || sync.global_requested || sync.global_processed || 0);
      const deleteTarget = Number(sync.global_delete_target || 0);
      const deleted = Number(sync.global_delete_deleted || 0);
      const mutationTotal = Number(sync.mutation_total || (writeTarget + deleteTarget));
      const mutationDone = Number(sync.mutation_done || (written + deleted));
      if (mutationTotal > 0 || mutationDone > 0) {
        const doneText = sync.status === 'running'
          ? tr('progress_running', { done: mutationDone, total: mutationTotal })
          : tr('progress_finished', { done: mutationDone });
        return tr('progress_details', {
          done_text: doneText,
          written,
          deleted
        });
      }
      return tr('progress_inactive');
    }

    function renderCounters(sync) {
      const pre = sync.pre_audit_summary || {};
      const diffTotal = Number(pre.diff_total || 0);
      const changed = Number(pre.changed_records || sync.global_write_target || 0);
      const missing = Number(pre.missing_in_bl || 0);
      const extra = Number(pre.extra_in_bl || sync.global_delete_target || 0);
      const unchanged = Number(sync.unchanged_records || 0);
      if (diffTotal > 0 || changed > 0 || missing > 0 || extra > 0 || unchanged > 0) {
        countersText.textContent = tr('counters', {
          diff: diffTotal,
          changed,
          missing,
          extra,
          unchanged
        });
      } else if (sync.status === 'running' && sync.sync_stage === 'pre_audit') {
        countersText.textContent = tr('pre_checking');
      } else {
        countersText.textContent = '-';
      }
    }

    function renderQueuedStart() {
      const optimistic = {
        status: 'running',
        sync_stage: 'pre_audit',
        updated_age_sec: 0,
        progress_percent: 0,
        message: 'Sync queued.',
        steps: [
          { label: tr('step_pre'), status: 'running', summary: tr('queued_pre') },
          { label: tr('step_sync'), status: 'waiting', summary: tr('queued_sync') },
          { label: tr('step_post'), status: 'waiting', summary: tr('queued_post') }
        ],
        summary_lines: [tr('queued_summary')]
      };
      lastSync = optimistic;
      statusText.textContent = statusLabel(optimistic.status);
      statusDot.className = 'dot running';
      message.textContent = messageLabel(optimistic.message);
      renderAttention(optimistic);
      progressText.textContent = '0.00%';
      progressBar.style.width = '0%';
      recordsText.textContent = renderProgressDetails(optimistic);
      etaText.textContent = '-';
      updatedText.textContent = tr('awaiting_status');
      renderSteps(optimistic.steps, optimistic.summary_lines);
      renderCounters(optimistic);
      runText.textContent = '';
      updateSyncButton(optimistic);
      scheduleNextRefresh(optimistic);
    }

    function escapeHtml(value) {
      return String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }

    function renderBudget(budget) {
      const data = budget || {};
      const spent = Number(data.spent_pln || 0);
      const limit = Number(data.limit_pln || 0);
      const spentWhole = Math.floor(Math.max(0, spent));
      const limitWhole = Math.floor(Math.max(0, limit));
      const remainingWhole = Math.max(0, limitWhole - spentWhole);
      const percent = Number(data.percent_used || 0);
      const displayPercent = limitWhole > 0 ? (spentWhole * 100 / limitWhole) : percent;
      const currency = data.display_currency || data.currency || 'PLN';
      const currencyLabel = currency === 'PLN' ? tr('currency_pln') : currency;
      const rate = Number(data.usd_to_pln_rate || 0);
      budgetText.textContent = `${spentWhole} / ${limitWhole} ${currencyLabel}`;
      budgetText.className = `value ${displayPercent >= 100 ? 'budget-danger' : displayPercent >= 80 ? 'budget-warn' : 'budget-ok'}`;
      if (data.error) {
        budgetSmall.textContent = tr('budget_error', { error: messageLabel(data.error) });
      } else {
        const fxSource = data.usd_to_pln_source || '';
        const fxDate = data.usd_to_pln_effective_date || '';
        const fxFetchedAt = data.usd_to_pln_fetched_at_iso || '';
        let rateText = '';
        if (rate > 0 && fxSource === 'nbp') {
          rateText = tr('budget_nbp_rate', {
            date: fxDate ? tr('budget_rate_date', { date: fxDate }) : '',
            rate: rate.toFixed(2)
          });
        } else if (rate > 0) {
          rateText = tr('budget_fallback_rate', {
            date: fxFetchedAt ? tr('budget_rate_saved', { date: fmt(fxFetchedAt) }) : '',
            rate: rate.toFixed(2)
          });
        }
        budgetSmall.textContent = tr('budget_summary', {
          percent: displayPercent.toFixed(1),
          remaining: remainingWhole,
          currency: currencyLabel,
          rate_text: rateText
        });
      }
    }

    function setButtonLoading(button, loading, label) {
      const text = button.querySelector('.btnText');
      button.classList.toggle('loading', loading);
      if (text) text.textContent = label;
    }

    function optionLabel(item) {
      return item.name ? `${item.name}` : `${item.id}`;
    }

    function fillSelect(select, items, selectedValue, emptyLabel) {
      select.innerHTML = '';
      if (!items.length) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = emptyLabel;
        select.appendChild(option);
        return;
      }
      items.forEach((item) => {
        const option = document.createElement('option');
        option.value = String(item.id);
        option.textContent = optionLabel(item);
        select.appendChild(option);
      });
      select.value = String(selectedValue || items[0].id);
      if (select.value === '' && selectedValue) {
        const option = document.createElement('option');
        option.value = String(selectedValue);
        option.textContent = String(selectedValue);
        select.appendChild(option);
        select.value = String(selectedValue);
      }
    }

    function selectedInventory() {
      const id = Number(inventorySelect.value || 0);
      return configOptions.inventories.find((item) => Number(item.id) === id) || null;
    }

    function refreshWarehouseOptions(selectedValue) {
      const inventory = selectedInventory();
      let allowed = configOptions.warehouses;
      if (inventory && Array.isArray(inventory.warehouse_ids) && inventory.warehouse_ids.length) {
        const allowedIds = new Set(inventory.warehouse_ids.map(String));
        allowed = configOptions.warehouses.filter((item) => allowedIds.has(String(item.id)));
      }
      fillSelect(warehouseSelect, allowed, selectedValue || warehouseSelect.value, tr('empty_warehouses'));
    }

    async function loadConfig() {
      isConfigLoading = true;
      updateSyncButton(lastSync);
      try {
        const res = await fetch('/api/config', { credentials: 'include' });
        const data = await res.json();
        if (!res.ok) throw new Error(data.message || tr('config_load_http', { status: res.status }));
        const config = data.config || {};
        configOptions = data.options || { inventories: [], warehouses: [] };
        comarchUrlInput.value = config.comarch_xml_url || '';
        rpmInput.value = config.bl_api_max_rpm || 90;
        fillSelect(inventorySelect, configOptions.inventories || [], config.bl_inventory_id, tr('empty_inventories'));
        refreshWarehouseOptions(config.bl_warehouse_id);
        configNote.innerHTML = tr('config_note');
      } catch (err) {
        configNote.textContent = messageLabel(err.message);
      } finally {
        isConfigLoading = false;
        updateSyncButton(lastSync);
      }
    }

    function collectConfig() {
      return {
        comarch_xml_url: comarchUrlInput.value.trim(),
        bl_inventory_id: Number(inventorySelect.value || 0),
        bl_warehouse_id: warehouseSelect.value,
        bl_api_max_rpm: Number(rpmInput.value || 90)
      };
    }

    function updateSyncButton(sync) {
      const running = sync.status === 'running' && (sync.updated_age_sec || 0) < 3600;
      const recentlyTriggered = Date.now() < triggerLockUntil;
      syncBtn.disabled = isTriggering || running || recentlyTriggered || isConfigLoading;
      if (isTriggering) {
        setButtonLoading(syncBtn, true, tr('button_starting'));
      } else if (isConfigLoading) {
        setButtonLoading(syncBtn, true, tr('button_loading_config'));
      } else if (running) {
        setButtonLoading(syncBtn, true, tr('button_running'));
      } else if (recentlyTriggered) {
        setButtonLoading(syncBtn, true, tr('button_waiting'));
      } else {
        setButtonLoading(syncBtn, false, tr('start'));
      }
    }

    function scheduleNextRefresh(sync) {
      if (timer) clearTimeout(timer);
      const running = sync.status === 'running' && (sync.updated_age_sec || 0) < 3600;
      const forcePolling = Date.now() < forcePollingUntil;
        if (running || forcePolling) {
        timer = setTimeout(() => {
          loadStatus().catch((err) => {
            message.textContent = messageLabel(err.message);
          });
        }, 60000);
      }
    }

    async function loadStatus() {
      if (isRefreshing) return;
      isRefreshing = true;
      setButtonLoading(refreshBtn, true, tr('button_refreshing'));
      try {
        const res = await fetch('/api/status', { credentials: 'include' });
        if (!res.ok) throw new Error(tr('status_load_http', { status: res.status }));
        const data = await res.json();
        const sync = data.sync || {};
        lastSync = sync;
        const schedule = data.schedule || {};
        const budget = data.budget || {};
        const pct = Number(sync.progress_percent || 0);

        statusText.textContent = statusLabel(sync.status);
        statusDot.className = `dot ${sync.status || ''}`;
        message.textContent = messageLabel(sync.message);
        renderAttention(sync);
        progressText.textContent = `${pct.toFixed(2)}%`;
        progressBar.style.width = `${Math.max(0, Math.min(100, pct))}%`;
        recordsText.textContent = renderProgressDetails(sync);
        etaText.textContent = fmt(sync.eta_finish_iso);
        updatedText.textContent = tr('updated_at', {
          date: fmt(sync.updated_at_iso),
          seconds: sync.updated_age_sec || 0
        });
        nextRunText.textContent = fmt(schedule.next_run_iso);
        scheduleText.textContent = schedule.state === 'ENABLED'
          ? tr('schedule_enabled')
          : tr('schedule_disabled');
        renderBudget(budget);
        renderSteps(sync.steps, sync.summary_lines);
        renderCounters(sync);
        runText.textContent = sync.run_id ? tr('run_id', { id: sync.run_id }) : '';
        updateSyncButton(sync);

        scheduleNextRefresh(sync);
      } finally {
        isRefreshing = false;
        setButtonLoading(refreshBtn, false, tr('refresh'));
      }
    }

    async function triggerSync() {
      if (isTriggering || syncBtn.disabled) return;
      isTriggering = true;
      syncBtn.disabled = true;
      setButtonLoading(syncBtn, true, tr('button_starting'));
      try {
        const res = await fetch('/api/sync', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ config: collectConfig() })
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(messageLabel(data.message) || tr('sync_start_http', { status: res.status }));
        }
        configNote.innerHTML = tr('config_saved');
        triggerLockUntil = Date.now() + 30000;
        forcePollingUntil = Date.now() + 15 * 60 * 1000;
        renderQueuedStart();
        setTimeout(() => {
          loadStatus().catch((err) => {
            message.textContent = messageLabel(err.message);
          });
        }, 3000);
      } catch (err) {
        alert(messageLabel(err.message));
      } finally {
        isTriggering = false;
        updateSyncButton(lastSync);
      }
    }

    refreshBtn.addEventListener('click', () => loadStatus().catch((err) => {
      message.textContent = messageLabel(err.message);
      isRefreshing = false;
      setButtonLoading(refreshBtn, false, tr('refresh'));
    }));
    inventorySelect.addEventListener('change', () => refreshWarehouseOptions());
    syncBtn.addEventListener('click', triggerSync);
    loadConfig().catch((err) => {
      configNote.textContent = messageLabel(err.message);
    });
    loadStatus().catch((err) => {
      statusText.textContent = tr('status_error');
      message.textContent = messageLabel(err.message);
      isRefreshing = false;
      setButtonLoading(refreshBtn, false, tr('refresh'));
    });
  </script>
</body>
</html>"""
    replacements = {
        "__LOCALE__": locale,
        "__I18N_JSON__": i18n_json,
        "__BRAND_NAME__": brand_name,
        "__PAGE_TITLE_SUFFIX__": html.escape(_t("page_title_suffix")),
        "__PANEL_TITLE__": panel_title,
        "__PANEL_SUBTITLE__": panel_subtitle,
        "__BRAND_VISUAL__": brand_visual,
        "__PRIMARY_COLOR__": primary,
        "__PRIMARY_DARK_COLOR__": primary_dark,
        "__SECONDARY_COLOR__": secondary,
        "__REFRESH__": html.escape(_t("refresh")),
        "__START__": html.escape(_t("start")),
        "__LABEL_STATUS__": html.escape(_t("label_status")),
        "__LABEL_PROGRESS__": html.escape(_t("label_progress")),
        "__LABEL_ETA__": html.escape(_t("label_eta")),
        "__LABEL_NEXT_RUN__": html.escape(_t("label_next_run")),
        "__LABEL_COST__": html.escape(_t("label_cost")),
        "__LABEL_COMPLETED__": html.escape(_t("label_completed")),
        "__LABEL_STAGES__": html.escape(_t("label_stages")),
        "__LABEL_SETTINGS__": html.escape(_t("label_settings")),
        "__LABEL_XML__": html.escape(_t("label_xml")),
        "__LABEL_INVENTORY__": html.escape(_t("label_inventory")),
        "__LABEL_WAREHOUSE__": html.escape(_t("label_warehouse")),
        "__LABEL_RPM__": html.escape(_t("label_rpm")),
        "__CONFIG_NOTE__": _t("config_note"),
        "__FOOTER__": html.escape(_t("footer")),
    }
    for marker, value in replacements.items():
        template = template.replace(marker, value)
    return template


def lambda_handler(event, context):
    if not _is_authorized(event):
        return _unauthorized()

    method, path = _request_info(event)
    if method == "GET" and path in {"/", "/admin"}:
        return _html_response(200, _page())
    if method == "GET" and path == "/assets/client-logo.png":
        return _logo_response()
    if method == "GET" and path == "/assets/favicon.svg":
        return _favicon_response()
    if method == "GET" and path == "/api/status":
        return _json_response(200, _status_payload())
    if method == "GET" and path == "/api/config":
        try:
            return _json_response(200, _config_payload())
        except Exception as exc:
            return _json_response(
                500,
                {"ok": False, "message": f"Failed to load sync config: {type(exc).__name__}: {exc}"},
            )
    if method == "POST" and path == "/api/sync":
        return _trigger_sync(event)
    return _json_response(404, {"ok": False, "message": "Not found"})
