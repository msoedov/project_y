import asyncio
import json
import logging
import sys
import uuid
from collections import defaultdict, namedtuple
from functools import lru_cache

import aiohttp
import daiquiri
from aiohttp import web

import contextvars

daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stdout, formatter=daiquiri.formatter.JSON_FORMATTER),
    ),
)

# Unique transaction id
TX = contextvars.ContextVar("var", default=42)
logger = daiquiri.getLogger(__name__)


class Options:
    conn_timeout = 30
    read_timeout = 30
    default_schema = "http"


class Upstream(namedtuple("Upstream", ["name", "host", "port", "endpoint", "session"])):
    @property
    def url(self) -> str:
        return f"{Options.default_schema}://{self.host}:{self.port}{self.endpoint}"

    @property
    def host_port(self) -> str:
        return f"{self.host}:{self.port}"


class ApiResponse(namedtuple("ApiResponse", ["body", "status", "headers", "name"])):
    def transform(self):
        try:
            return json.loads(self.body)
        except ValueError as e:
            return {"error": self.body.decode("utf-8")}


def default_connection() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        conn_timeout=Options.conn_timeout, read_timeout=Options.read_timeout
    )


# Declarative configuration of API gateway routing
service_a = Upstream("service1", "0.0.0.0", 9091, "/api/test", default_connection())
service_b = Upstream("service2", "0.0.0.0", 9092, "/api/test", default_connection())
service_c = Upstream("service3", "0.0.0.0", 9093, "/api/test", default_connection())

routing_table = {
    "service1": [service_a],
    "service2": [service_b],
    "service3": [service_c],
    "grouped": [service_a, service_b, service_c],
}


async def send_request(
    srv: Upstream, method: str, headers: dict, data: bytes
) -> ApiResponse:
    """
    Handle single request to upstream service

    Arguments:
        srv {Upstream} -- [description]
        method {str} -- [description]
        headers {dict} -- [description]
        data {bytes} -- [description]

    Returns:
        ApiResponse -- [description]
    """

    headers["Host"] = srv.host_port

    logger.info("requesting", url=srv.url, method=method, tx=TX.get())
    try:
        resp = await srv.session.request(method, srv.url, headers=headers, data=data)
    except aiohttp.client_exceptions.ClientConnectorError as e:
        logger.error("connection error", url=srv.url, method=method, tx=TX.get())
        return ApiResponse(
            b"Bad gateway #trace=0cf36ac4-b79e-4b74-85f1-fc823822c743",
            502,
            {},
            srv.name,
        )
    try:
        data = await resp.read()
    except IOError as e:
        return ApiResponse(
            b"Bad gateway #trace=0cf36ac4-b79e-4b74-85f1-fc823822c744",
            502,
            {},
            srv.name,
        )
    logger.info(
        "done",
        url=srv.url,
        method=method,
        bytes=len(data),
        srv_name=srv.name,
        code=resp.status,
        tx=TX.get(),
    )
    return ApiResponse(data, resp.status, resp.headers, srv.name)


async def gateway(request) -> web.Response:
    """
    Main api gateway handler
    """
    upstream: str = request.match_info["service"].strip()

    srvs: [Upstream] = routing_table.get(upstream)
    TX.set(str(uuid.uuid4().hex))
    logger.info("request", upstream=upstream, tx=TX.get())

    if not srvs:
        logger.error("no routes to upstream", upstream=upstream, tx=TX.get())
        return web.Response(
            text="Bad gateway #trace=0cf36ac4-b79e-4b74-85f1-fc823822c742", status=502
        )

    data = None
    if request.body_exists:
        data = await request.read()

    results = await asyncio.gather(
        *(
            send_request(
                srv, method=request.method, headers=request.headers.copy(), data=data
            )
            for srv in srvs
        )
    )

    if len(results) == 1:
        #  Optimization for most common case of one upstream service
        reply = results[0]
        return web.Response(body=reply.body, status=reply.status, headers=reply.headers)

    # Reconcile multiple response codes, return most optimistic status code
    status = min(results, key=lambda r: r.status).status
    headers = dict(results[0].headers)
    headers.pop("Content-Length", None)
    #  Transform payload
    data = json.dumps({r.name: r.transform() for r in results})
    return web.Response(body=data, status=status, headers=headers)


def init_func(*argv):
    app = web.Application()
    app.router.add_routes([web.route("*", "/api/{service}", gateway)])
    return app


if __name__ == "__main__":
    web.run_app(init_func(), access_log=None)
