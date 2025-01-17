import pathlib
import ssl
from typing import Dict

import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.web

import definitions
import mhs_common.configuration.configuration_manager as configuration_manager
import outbound.request.synchronous.handler as client_request_handler
import utilities.integration_adaptors_logger as log
from handlers import healthcheck_handler
from mhs_common import workflow
from mhs_common.routing import route_lookup_client, spine_route_lookup_client, sds_api_client
from persistence import persistence_adaptor
from persistence.persistence_adaptor_factory import get_persistence_adaptor
from mhs_common.workflow import sync_async_resynchroniser as resync
from outbound.transmission import outbound_transmission
from utilities import config, certs
from utilities import secrets
from utilities.string_utilities import str2bool

logger = log.IntegrationAdaptorsLogger(__name__)


def configure_http_client():
    """
    Configure Tornado to use the curl HTTP client.
    """
    tornado.httpclient.AsyncHTTPClient.configure('tornado.curl_httpclient.CurlAsyncHTTPClient')


def initialise_workflows(transmission: outbound_transmission.OutboundTransmission, party_key: str,
                         work_description_store: persistence_adaptor.PersistenceAdaptor,
                         sync_async_store: persistence_adaptor.PersistenceAdaptor,
                         max_request_size: int,
                         routing: route_lookup_client.RouteLookupClient) \
        -> Dict[str, workflow.CommonWorkflow]:
    """Initialise the workflows
    :param transmission: The transmission object to be used to make requests to the spine endpoints
    :param party_key: The party key to use to identify this MHS.
    :param work_description_store: The persistence adaptor for the state database.
    :param sync_async_store: The persistence adaptor for the sync-async database.
    :param max_request_size: The maximum size of the request body that gets sent to Spine.
    :param persistence_store_retries The number of times to retry storing values in the work description or sync-async
    databases.
    :param routing: The routing and reliability component to use to request routing/reliability details
    from.
    :return: The workflows that can be used to handle messages.
    """

    resynchroniser = resync.SyncAsyncResynchroniser(sync_async_store,
                                                    int(config.get_config('RESYNC_RETRIES', '20')),
                                                    float(config.get_config('RESYNC_INTERVAL', '1.0')),
                                                    float(config.get_config('RESYNC_INITIAL_DELAY', '0')))

    return workflow.get_workflow_map(party_key,
                                     work_description_store=work_description_store,
                                     transmission=transmission,
                                     resynchroniser=resynchroniser,
                                     max_request_size=max_request_size,
                                     routing=routing
                                     )


def initialise_spine_route_lookup():
    logger.info("Initializing LDAP lookup using SpineRoutLookup")

    spine_route_lookup_url = config.get_config('SPINE_ROUTE_LOOKUP_URL')
    spine_org_code = config.get_config('SPINE_ORG_CODE')
    validate_cert = str2bool(config.get_config('SPINE_ROUTE_LOOKUP_VALIDATE_CERT', default=str(True)))

    route_data_dir = pathlib.Path(definitions.ROOT_DIR) / "route"
    certificates = certs.Certs.create_certs_files(route_data_dir,
                                                  private_key=secrets.get_secret_config('SPINE_ROUTE_LOOKUP_CLIENT_KEY',
                                                                                        default=None),
                                                  local_cert=secrets.get_secret_config('SPINE_ROUTE_LOOKUP_CLIENT_CERT',
                                                                                       default=None),
                                                  ca_certs=secrets.get_secret_config('SPINE_ROUTE_LOOKUP_CA_CERTS',
                                                                                     default=None))

    route_proxy_host = config.get_config('SPINE_ROUTE_LOOKUP_HTTP_PROXY', default=None)
    route_proxy_port = None
    if route_proxy_host is not None:
        route_proxy_port = int(config.get_config('SPINE_ROUTE_LOOKUP_HTTP_PROXY_PORT', default="3128"))

    return spine_route_lookup_client.SpineRouteLookupClient(spine_route_lookup_url, spine_org_code,
                                                      client_cert=certificates.local_cert_path,
                                                      client_key=certificates.private_key_path,
                                                      ca_certs=certificates.ca_certs_path,
                                                      http_proxy_host=route_proxy_host,
                                                      http_proxy_port=route_proxy_port,
                                                      validate_cert=validate_cert)


def initialise_sds_api_client():
    logger.info("Initializing LDAP lookup using SDS API")

    sds_url = config.get_config('SDS_API_URL')
    sds_api_key = config.get_config('SDS_API_KEY')
    spine_org_code = config.get_config('SPINE_ORG_CODE')

    if ssl.get_default_verify_paths().cafile is None:
        raise Exception("Unable to find path to root certificates using the OpenSSL library. This is required to communicate with SDS. Quitting.")

    return sds_api_client.SdsApiClient(sds_url, sds_api_key, spine_org_code)


def start_tornado_server(data_dir: pathlib.Path, workflows: Dict[str, workflow.CommonWorkflow]) -> None:
    """
    Start Tornado server
    :param data_dir: The directory to load interactions configuration from.
    :param workflows: The workflows to be used to handle messages.
    """
    interactions_config_file = str(data_dir / "interactions" / "interactions.json")
    config_manager = configuration_manager.ConfigurationManager(interactions_config_file)

    # Note that the paths in generate_openapi.py should be updated if these
    # paths are changed
    supplier_application = tornado.web.Application(
        [
            (r"/", client_request_handler.SynchronousHandler, dict(config_manager=config_manager, workflows=workflows)),
            (r"/healthcheck", healthcheck_handler.HealthcheckHandler)
        ])
    supplier_server = tornado.httpserver.HTTPServer(supplier_application)
    server_port = int(config.get_config('OUTBOUND_SERVER_PORT', default='80'))
    supplier_server.listen(server_port)

    logger.info('Starting outbound server at port {server_port}', fparams={'server_port': server_port})
    tornado_io_loop = tornado.ioloop.IOLoop.current()
    try:
        tornado_io_loop.start()
    except KeyboardInterrupt:
        logger.warning('Keyboard interrupt')
        pass
    finally:
        tornado_io_loop.stop()
        tornado_io_loop.close(True)
    logger.info('Server shut down, exiting...')


def main():
    config.setup_config("MHS")
    secrets.setup_secret_config("MHS")
    log.configure_logging("outbound")

    data_dir = pathlib.Path(definitions.ROOT_DIR) / "data"

    configure_http_client()

    routing_lookup_method = config.get_config('OUTBOUND_ROUTING_LOOKUP_METHOD', default='SPINE_ROUTE_LOOKUP')
    if routing_lookup_method == 'SPINE_ROUTE_LOOKUP':
        routing = initialise_spine_route_lookup()
    elif routing_lookup_method == 'SDS_API':
        routing = initialise_sds_api_client()
    else:
        raise KeyError

    certificates = certs.Certs.create_certs_files(data_dir / '..',
                                                  private_key=secrets.get_secret_config('''-----BEGIN PRIVATE KEY-----
    MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/eldvdphgPbqf
    FcMEmoom173g7ptz7iHMxvAXwEBz1+oY/WCgOt/9DAUYLWkpjX7NbMuk6biZ2PQL
    fyWXjxj5OKzW76bqoZnb2kYoBuQYx+Nj3hhNC3Qnrxbnx7IzzK/qCNa+SzOVbVjf
    eOPTy6VIicEMLjp5GYcyJ/c1gsZPwASxsLo71ieAbiy18m/wgiDvY+dQ8YjN6k+Z
    xsrRrjjyF7iCOFdMg8C4Rdtz7ik2TRltrE1AngkZVV4EOsdJffDblfxLD4qvVdRW
    wLdzoMh1pc9TjdmXyrAutsfe5NOR+RbffHSARXFW5fb7xI1Y/B1GmSBFMCAC+uiU
    QvXhaZcrAgMBAAECggEAILeIxRTgiGFDx/bx8ZFuVaC7YvmiOjbQM6syTvychq63
    4zk4D2i/6P7qx+zKcT0qE3OpgC4nSrJVULmS9MiQndTfQ4KDZMfkADrVjfVPeCR/
             aAsE3/U7DaTPJx9qBYvN6xFDkBRNZhTfAKApXYEIZtKS17/2YcbI4PsPNGd59xou
    df7zgbd6SA47pAHoregeaRcsMIztqtjf3UhYe4WoqHZFKUvTkxgPbs7E1p5TZ92g
    RslTdDbQqLaHdiTOqKB/g363qhJ5dVVm+rb6MjtgJGdnpLVH5XxuboECXF4FRJR3
    StrM2HGSB1z7ZeasNiKtT63kMO7QKiA5TyltzwHM0QKBgQDns/3bNqYfuhWCdvwq
    W0oON9OKAByWRIuwWzIQWIQUm2szwH2fgUJB2V3/AXD/y2SEdQZM3A0L0xBhVTQj
    cc2nvL7p6J+oiYp2GUHm73tHpkAzq7hbA6ANRGRmRD2pIUjOL5qPbtwM+gfGTpcN
    mjUYGfw1ggk0LOZHn+4sSVANnwKBgQDTjoPhPtI1O8up91NBbSXeSp3qMWUa9dtK
    eLB8wEKCBWdlFL0fD1hMjmJEqaY4UZm1Gs/sbDIkGogahmsS6lEiTWFbEK6gFUTY
    dCJ65WhuqdLKyvkwmr4694Ctp6Ejhbod/2/2GBPhWLrUTAQSZ4TH8Zp/KJZSXsgl
    4jVO6Syy9QKBgQDXhI1I/RESi8T8IG63e0hr5zOFtkrg3wtL09fCaoMYo+PYNGDl
    H5cgpu4OhymzVF2/8xYUIc6kxAMFdfpUScOwFRlDe1QesSiwZxfsla8G2zX2mfCV
                     /8486PO2SB1Olx4gYxkR910JWPwoUeuhBGIEdA8rOjQTavwbfUBNwzeKIwKBgFka
    lntBbWIUfFRrIjrVUPTOcrKX+WCgmqtEJ/lzNM/0nLbbREiXuvYLpmILHkJsRBQe
    ZeLLM1c3gYnCgcimvmN3OgEUBqjQLH4KdBdVFmY9ytW1Jb2N/39wjVcW2mzOvzQx
    SSPawkzQhWgzWCe0SB26qfrSynWJDD3Ah/ljhnsdAoGAaCDlZ/N/BPvMWfafyGXg
    Zr1iEso3Vzulr3/UwasqdbURVKbglhWzdTuZMwiPjhbrVcPDZzpg02uHHM7UXtBI
    g83mz0maeg1cRE6ftVMRQULcrdF1hJ44db0oHbA7FRODBJhIazPtLPVAXY+j57KA
    1vsiWMuSqmPRbL5yiaY2QVc=
    -----END PRIVATE KEY-----'''),
                                                  local_cert=secrets.get_secret_config('''-----BEGIN PRIVATE KEY-----
    MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/eldvdphgPbqf
    FcMEmoom173g7ptz7iHMxvAXwEBz1+oY/WCgOt/9DAUYLWkpjX7NbMuk6biZ2PQL
    fyWXjxj5OKzW76bqoZnb2kYoBuQYx+Nj3hhNC3Qnrxbnx7IzzK/qCNa+SzOVbVjf
    eOPTy6VIicEMLjp5GYcyJ/c1gsZPwASxsLo71ieAbiy18m/wgiDvY+dQ8YjN6k+Z
    xsrRrjjyF7iCOFdMg8C4Rdtz7ik2TRltrE1AngkZVV4EOsdJffDblfxLD4qvVdRW
    wLdzoMh1pc9TjdmXyrAutsfe5NOR+RbffHSARXFW5fb7xI1Y/B1GmSBFMCAC+uiU
    QvXhaZcrAgMBAAECggEAILeIxRTgiGFDx/bx8ZFuVaC7YvmiOjbQM6syTvychq63
    4zk4D2i/6P7qx+zKcT0qE3OpgC4nSrJVULmS9MiQndTfQ4KDZMfkADrVjfVPeCR/
             aAsE3/U7DaTPJx9qBYvN6xFDkBRNZhTfAKApXYEIZtKS17/2YcbI4PsPNGd59xou
    df7zgbd6SA47pAHoregeaRcsMIztqtjf3UhYe4WoqHZFKUvTkxgPbs7E1p5TZ92g
    RslTdDbQqLaHdiTOqKB/g363qhJ5dVVm+rb6MjtgJGdnpLVH5XxuboECXF4FRJR3
    StrM2HGSB1z7ZeasNiKtT63kMO7QKiA5TyltzwHM0QKBgQDns/3bNqYfuhWCdvwq
    W0oON9OKAByWRIuwWzIQWIQUm2szwH2fgUJB2V3/AXD/y2SEdQZM3A0L0xBhVTQj
    cc2nvL7p6J+oiYp2GUHm73tHpkAzq7hbA6ANRGRmRD2pIUjOL5qPbtwM+gfGTpcN
    mjUYGfw1ggk0LOZHn+4sSVANnwKBgQDTjoPhPtI1O8up91NBbSXeSp3qMWUa9dtK
    eLB8wEKCBWdlFL0fD1hMjmJEqaY4UZm1Gs/sbDIkGogahmsS6lEiTWFbEK6gFUTY
    dCJ65WhuqdLKyvkwmr4694Ctp6Ejhbod/2/2GBPhWLrUTAQSZ4TH8Zp/KJZSXsgl
    4jVO6Syy9QKBgQDXhI1I/RESi8T8IG63e0hr5zOFtkrg3wtL09fCaoMYo+PYNGDl
    H5cgpu4OhymzVF2/8xYUIc6kxAMFdfpUScOwFRlDe1QesSiwZxfsla8G2zX2mfCV
                     /8486PO2SB1Olx4gYxkR910JWPwoUeuhBGIEdA8rOjQTavwbfUBNwzeKIwKBgFka
    lntBbWIUfFRrIjrVUPTOcrKX+WCgmqtEJ/lzNM/0nLbbREiXuvYLpmILHkJsRBQe
    ZeLLM1c3gYnCgcimvmN3OgEUBqjQLH4KdBdVFmY9ytW1Jb2N/39wjVcW2mzOvzQx
    SSPawkzQhWgzWCe0SB26qfrSynWJDD3Ah/ljhnsdAoGAaCDlZ/N/BPvMWfafyGXg
    Zr1iEso3Vzulr3/UwasqdbURVKbglhWzdTuZMwiPjhbrVcPDZzpg02uHHM7UXtBI
    g83mz0maeg1cRE6ftVMRQULcrdF1hJ44db0oHbA7FRODBJhIazPtLPVAXY+j57KA
    1vsiWMuSqmPRbL5yiaY2QVc=
    -----END PRIVATE KEY-----'''),
                                                  ca_certs=secrets.get_secret_config('''-----BEGIN CERTIFICATE-----
MIIFhzCCA2+gAwIBAgIQGjdQ3OTSYx62oSmGTy9tazANBgkqhkiG9w0BAQwFADBM
MQswCQYDVQQGEwJHQjEMMAoGA1UEChMDbmhzMQswCQYDVQQLEwJDQTEiMCAGA1UE
AxMZTkhTIFBUTCBSb290IEF1dGhvcml0eSBHMjAeFw0yMjA4MDQxNDA1NDNaFw0z
MjA4MDQxNDM1NDNaMEwxCzAJBgNVBAYTAkdCMQwwCgYDVQQKEwNuaHMxCzAJBgNV
BAsTAkNBMSIwIAYDVQQDExlOSFMgSU5UIEF1dGhlbnRpY2F0aW9uIEcyMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxT9GjD+It1PZUGSCxqgZl0GFu1Bs
T2+IrzTQP0PnI0GQSEVmln4629ezXxhPigPqokzw4lDS/x5a/1qcSVMzgPBkaYH8
04+MEBQNatuZhEu6zJPr6ARR3kGEf6MfxyllL5FwxU7AYNuACb6eVKvST/OC40Vx
CGEhoIwvhvA016K50wYwtv8oiaywpHx+NbD2VhdOOsHNHZIauOmqHzY3IwmvS5NA
NiZx8s8ctETbRsrwgO3p/667ix3PZME9yCPmzhm9TsyJABEjIDrRm1qW15V+GNfz
jjBkN+j5dmtJRHbO+KLwwqY63sHg3JNeA74FFxxfVlSwUykeuQTT8UbcbwIDAQAB
o4IBYzCCAV8wDgYDVR0PAQH/BAQDAgEGMBIGA1UdEwEB/wQIMAYBAf8CAQEwPgYD
VR0gBDcwNTAzBgsqhjoAiXtlAAMCADAkMCIGCCsGAQUFBwIBFhZodHRwczovL3Br
aS5uaHMudWsvQ1BTMHgGCCsGAQUFBwEBBGwwajAjBggrBgEFBQcwAYYXaHR0cDov
L29jc3AubmhzLnVrL29jc3AwQwYIKwYBBQUHMAKGN2h0dHA6Ly9wa2kubmhzLnVr
L1BUTC9HMi9yb290L05IU1BUTFJvb3RBdXRob3JpdHlHMi5jcnQwPwYDVR0fBDgw
NjA0oDKgMIYuaHR0cDovL2NybC5uaHMudWsvUFRML0cyL3Jvb3QvTkhTUFRMcm9v
dEcyLmNybDAfBgNVHSMEGDAWgBT1ttVSqL14/nJhaztdjCPAsBYM8zAdBgNVHQ4E
FgQUFsdHBKhgyeRdq5nylPfD3jOY1CIwDQYJKoZIhvcNAQEMBQADggIBAEWWjN0d
6uxtKi5aLEv0CtUXqg7MJClCHKwCpYuewzI/OfRux5LuL7xHQx8Baj5Jh2IiZLkc
vPQ626RVnKaYOvJAoM7UWMFgr3vta0uMEAnxRIOCpNiQFDh4HBpzbQNcMg8zkKUi
JMb4OmT09zCzGTG2WdPT6KwAJOXw9IVeT+Z8ggScCmbiLKHT+s9y612oekIH0SxM
/BNjmyYWt02cON6e92XXs6refjoJS29Kne2nBwersk1bLAumcueVBEtnMUBILXlH
XoFcCIZSOca/qg9K7jyxl+uyXWK74AblMi0RfKsziM34Ux+hKv03SknLT9kbIBt0
lbjntoeweu4oXMDQ+wdjSRe0OM0Ed2ttFMsI8jSkJAQlvN1uks5/M+cdAsg3D7Gp
Df3WPHCT17ulr9VJ5I16XOb6JnNoMGEgUQm/AyNGO2zLm+XLo4Ujk/dKES08Cwwm
zjXqOCti2Kp9mWYF8x1gOwIu4ye+rBhJdlNnxvbdV4oOyo1CYyw261jWI19yCaDJ
Hmgq1F8M7nY0C9NRqlRaB1G+p1+mZVlVMOOD6EmprV80rBDfVN/N2swbmSGhijNe
zAlqBMIkp6jTG9lEJbvtpY6aGWGlEheb2pPcBCXcBknI1Lhqv/sgdM6zbkzD+rAi
oukkF6E6wLgCHgPz3FJwVBnM0NdaISHTBbOQ
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFtDCCA5ygAwIBAgIQHJP1UsE9cdiw+4IqFnxM3TANBgkqhkiG9w0BAQwFADBM
MQswCQYDVQQGEwJHQjEMMAoGA1UEChMDbmhzMQswCQYDVQQLEwJDQTEiMCAGA1UE
AxMZTkhTIFBUTCBSb290IEF1dGhvcml0eSBHMjAeFw0yMjA4MDIxNTE3MjRaFw00
MjA4MDIxNTQ3MjRaMEwxCzAJBgNVBAYTAkdCMQwwCgYDVQQKEwNuaHMxCzAJBgNV
BAsTAkNBMSIwIAYDVQQDExlOSFMgUFRMIFJvb3QgQXV0aG9yaXR5IEcyMIICIjAN
BgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAwIy5AVQWAT35rfI0AYCql2F04Yp9
M6gTgv6vmxtdHsIJgLCrbxy5p6hQIxCu0Jq4Fc8rflv5jDZOPO6tsFRuJck1Xv17
53jBAuo00Q0XoORdfTymktLp78P2zNvj3Y3oaXUW6dzwztQf/BLjSwD0A3uuiej8
BuemHDQDCWHhfu8fkVC85n8XPO1QKzmpTHXBK5tgx5SkDzfxk3zsc31tgs+/xRmZ
Jc45qWQazFCtGr5rpeKb/9thi9+LbctCMO2Be4La4u6rePOhR8zx73zpLnrtOD8s
Nfxqk59vyIZ7fqfGCfIs8hdcQFOvfABalSaW2Bg4njwOY879WjyxtE1jpPB/fuDp
/cQtrdNVLEY+fEa+WBbX/TG2GAQFFle/ThU+c2mtkfToRL42Hrbzfqg9wr69e2oI
79cQ3DKP1Eaq0bzw8TkOfswoKgKm4QGlBzJFAWaAEvisX+JgRtPzvSTMCttUbBYp
NnfXwayQ8s9IwYwPeFDQrs0/MR+uUqtObUv9B3T4bNBXoIc98rn7+/x5yQhGYre8
0YzcqeL33A/K6Tzu+P3u9DftfkOgcnZyg69ePqAAjrY9OA2RqnVfxm7vraaEgxMm
rpslCni/FM+/P6yV4LdnDJuKfaMq0k+RfkbsT2WXsJHLIec8uRgpoa74QvEO/p85
UDIFBUds0QqN1GsCAwEAAaOBkTCBjjAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/
BAUwAwEB/zArBgNVHRAEJDAigA8yMDIyMDgwMjE1MTcyNFqBDzIwNDIwODAyMTU0
NzI0WjAfBgNVHSMEGDAWgBT1ttVSqL14/nJhaztdjCPAsBYM8zAdBgNVHQ4EFgQU
9bbVUqi9eP5yYWs7XYwjwLAWDPMwDQYJKoZIhvcNAQEMBQADggIBAKekcO9zq3ER
YFOJTDqkY4NoDSTmlab4Al07hLJ8WYckSePQ9HmxVEqnTVYBCtPHfcyUlUqbBQVh
DBQ2ZzERONqq2ENevGh6Li/0ZxuPiQhb5hxL7uv20vTEmbkSrPYs5TKYNbkJ8gx+
JT4uoxETLfNHG6WvGV2VBbMR+dZQisRoR0jm8P7n7wkTHjiDNH7FLfgozy3hIfUR
0bhLEt3HlniPBoC4egvZMP/R8rnOwqdEmStP6YR2BZxUSmrQozOamVJIljMTiTlG
xwqmvVMnRtaYIQsdOyiXpS4UjocbebV+a7u9Bbst/Y2rV8PVprpATlqj4YOjqrNx
v5MrLgyknNKUIqhbJXAy9j74x8OO1tilH6vf32zapZa/GtHpTvo7nRr1pNnfUgbp
vBCvXoxIrg7rrWx3kjRF9Cri1d5khUDXYJzewflSsgYLLTygQ+lwthw+XghdHNos
TARNSRPrO5JsWmSc6R9bjcuyhAsxvhAS1LpE1EwckkMUgSvdfmKbNc3RkjyusGYP
Nlo+MiECpArwqymOxnULpKCwgJApVrwht0eDYzIw5XCe68FCQ/Ewaj25l81gVyWQ
gM4KvdmCt0vk++15mcuUayTdcUg4cAGegvP8g0a9qncHT83J9E4D47QvftZkqZtF
E3e+hb4BEdtJoedF9IHxjaHpRVhwJT98
-----END CERTIFICATE-----'''))
    max_retries = int(config.get_config('OUTBOUND_TRANSMISSION_MAX_RETRIES', default="3"))
    retry_delay = int(config.get_config('OUTBOUND_TRANSMISSION_RETRY_DELAY', default="100"))
    validate_cert = str2bool(config.get_config('OUTBOUND_VALIDATE_CERTIFICATE', default=str(True)))
    http_proxy_host = config.get_config('OUTBOUND_HTTP_PROXY', default=None)
    http_proxy_port = None
    if http_proxy_host is not None:
        http_proxy_port = int(config.get_config('OUTBOUND_HTTP_PROXY_PORT', default="3128"))
    transmission = outbound_transmission.OutboundTransmission(certificates.local_cert_path,
                                                              certificates.private_key_path, certificates.ca_certs_path,
                                                              max_retries, retry_delay, validate_cert, http_proxy_host,
                                                              http_proxy_port)

    party_key = secrets.get_secret_config('PARTY_KEY')

    work_description_store = get_persistence_adaptor(
        table_name=config.get_config('STATE_TABLE_NAME'),
        max_retries=int(config.get_config('STATE_STORE_MAX_RETRIES', default='3')),
        retry_delay=int(config.get_config('STATE_STORE_RETRY_DELAY', default='100')) / 1000)

    sync_async_store = get_persistence_adaptor(
        table_name=config.get_config('SYNC_ASYNC_STATE_TABLE_NAME'),
        max_retries=int(config.get_config('SYNC_ASYNC_STORE_MAX_RETRIES', default='3')),
        retry_delay=int(config.get_config('SYNC_ASYNC_STORE_RETRY_DELAY', default='100')) / 1000)

    max_request_size = int(config.get_config('SPINE_REQUEST_MAX_SIZE'))
    workflows = initialise_workflows(transmission, party_key, work_description_store, sync_async_store,
                                     max_request_size, routing)
    start_tornado_server(data_dir, workflows)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.critical('Fatal exception in main application', exc_info=True)
    finally:
        logger.info('Exiting application')
