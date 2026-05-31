import os
import hashlib
import httpx
import logging
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# --- Конфигурация из переменных окружения ---
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

# Вывод конфигурации при старте
logger.info("=" * 60)
logger.info(f"GRADUAL_MIGRATION: {GRADUAL_MIGRATION}")
logger.info(f"MOVIES_MIGRATION_PERCENT: {MOVIES_MIGRATION_PERCENT}%")
logger.info(f"MONOLITH_URL: {MONOLITH_URL}")
logger.info(f"MOVIES_SERVICE_URL: {MOVIES_SERVICE_URL}")
logger.info(f"EVENTS_SERVICE_URL: {EVENTS_SERVICE_URL}")
logger.info("=" * 60)

# --- Клиент HTTPX с поддержкой keep-alive ---
client = httpx.AsyncClient(timeout=30.0)

# --- Вспомогательная функция: выбор цели для movies ---
def get_movies_target(request: Request) -> str:
    """
    Если фиче-флаг включён, вычисляем хеш от комбинации client-ip + URL,
    чтобы один и тот же клиент стабильно попадал на один и тот же бэкенд
    (sticky routing), но в целом распределение соответствовало проценту.
    """
    if not GRADUAL_MIGRATION:
        logger.info(f"[MOVIES] Gradual migration OFF → MONOLITH")
        return MONOLITH_URL

    # Получаем IP клиента (учитываем заголовки прокси)
    client_ip = request.client.host if request.client else "127.0.0.1"
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()

    # Формируем строку для хеширования
    hash_input = f"{client_ip}:{request.url.path}"
    hash_hex = hashlib.md5(hash_input.encode()).hexdigest()
    hash_int = int(hash_hex, 16)
    bucket = hash_int % 100

    # Определяем, попадает ли запрос в процент миграции
    if bucket < MOVIES_MIGRATION_PERCENT:
        target = MOVIES_SERVICE_URL
        logger.info(f"[MOVIES] IP={client_ip} Path={request.url.path} Bucket={bucket} < {MOVIES_MIGRATION_PERCENT}% → MOVIES SERVICE")
    else:
        target = MONOLITH_URL
        logger.info(f"[MOVIES] IP={client_ip} Path={request.url.path} Bucket={bucket} >= {MOVIES_MIGRATION_PERCENT}% → MONOLITH")
    
    return target

# --- Универсальный прокси-метод ---
async def proxy_request(target_url: str, request: Request) -> Response:
    """Пробрасывает запрос на target_url и возвращает ответ клиенту."""
    method = request.method
    path = request.url.path
    query = request.url.query
    full_url = f"{target_url}{path}"
    if query:
        full_url += f"?{query}"

    logger.info(f"[PROXY] {method} {path} → {full_url}")

    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    body = await request.body()

    try:
        upstream_response = await client.request(
            method=method,
            url=full_url,
            headers=headers,
            content=body,
        )
        logger.info(f"[PROXY] Response: {upstream_response.status_code}")
    except httpx.RequestError as exc:
        logger.error(f"[PROXY] Error: {exc}")
        return Response(
            content=f"Proxy error: {exc}",
            status_code=502,
        )

    return StreamingResponse(
        content=upstream_response.aiter_bytes(),
        status_code=upstream_response.status_code,
        headers=dict(upstream_response.headers),
    )

# --- Маршруты для movies (разные варианты путей) ---

@app.api_route(
    "/api/movies/{rest_of_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_movies_no_version(request: Request, rest_of_path: str = ""):
    """Обрабатывает /api/movies/* (без версии)"""
    logger.info(f"[ROUTE] Caught /api/movies/{rest_of_path}")
    target = get_movies_target(request)
    return await proxy_request(target, request)

@app.api_route(
    "/api/v1/movies/{rest_of_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_movies_v1(request: Request, rest_of_path: str = ""):
    """Обрабатывает /api/v1/movies/* (с версией)"""
    logger.info(f"[ROUTE] Caught /api/v1/movies/{rest_of_path}")
    target = get_movies_target(request)
    return await proxy_request(target, request)

@app.api_route(
    "/api/movies",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_movies_no_version_root(request: Request):
    """Обрабатывает /api/movies (корневой без версии)"""
    logger.info(f"[ROUTE] Caught /api/movies")
    target = get_movies_target(request)
    return await proxy_request(target, request)

@app.api_route(
    "/api/v1/movies",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_movies_v1_root(request: Request):
    """Обрабатывает /api/v1/movies (корневой с версией)"""
    logger.info(f"[ROUTE] Caught /api/v1/movies")
    target = get_movies_target(request)
    return await proxy_request(target, request)

# --- Маршруты для events ---

@app.api_route(
    "/api/events/{rest_of_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_events_no_version(request: Request, rest_of_path: str = ""):
    """Обрабатывает /api/events/* (без версии)"""
    logger.info(f"[ROUTE] Caught /api/events/{rest_of_path}")
    return await proxy_request(EVENTS_SERVICE_URL, request)

@app.api_route(
    "/api/v1/events/{rest_of_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_events_v1(request: Request, rest_of_path: str = ""):
    """Обрабатывает /api/v1/events/* (с версией)"""
    logger.info(f"[ROUTE] Caught /api/v1/events/{rest_of_path}")
    return await proxy_request(EVENTS_SERVICE_URL, request)

@app.api_route(
    "/api/events",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_events_no_version_root(request: Request):
    """Обрабатывает /api/events (корневой без версии)"""
    logger.info(f"[ROUTE] Caught /api/events")
    return await proxy_request(EVENTS_SERVICE_URL, request)

@app.api_route(
    "/api/v1/events",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_events_v1_root(request: Request):
    """Обрабатывает /api/v1/events (корневой с версией)"""
    logger.info(f"[ROUTE] Caught /api/v1/events")
    return await proxy_request(EVENTS_SERVICE_URL, request)

# --- Общий маршрут для всего остального (должен быть ПОСЛЕДНИМ) ---
@app.api_route(
    "/{rest_of_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def route_default(request: Request, rest_of_path: str = ""):
    """Все остальные запросы идут на монолит"""
    logger.info(f"[ROUTE] Default route: /{rest_of_path} → MONOLITH")
    return await proxy_request(MONOLITH_URL, request)

# --- Health-check с подробной информацией ---
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "gradual_migration": GRADUAL_MIGRATION,
        "movies_migration_percent": MOVIES_MIGRATION_PERCENT,
        "routes": {
            "movies": ["/api/movies", "/api/movies/*", "/api/v1/movies", "/api/v1/movies/*"],
            "events": ["/api/events", "/api/events/*", "/api/v1/events", "/api/v1/events/*"],
            "default": "/* → monolith"
        }
    }