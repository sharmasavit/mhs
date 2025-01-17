"""This module defines the outbound transmission component."""

from ssl import SSLError
from typing import Dict

from comms.common_https import CommonHttps
from retry import retriable_action
from mhs_common.transmission import transmission_adaptor
from tornado import httpclient
from utilities import integration_adaptors_logger as log

logger = log.IntegrationAdaptorsLogger(__name__)


class OutboundTransmissionError(Exception):
    pass


class OutboundTransmission(transmission_adaptor.TransmissionAdaptor):
    errors_not_to_retry = (
        httpclient.HTTPClientError,
        SSLError
    )

    """A component that sends HTTP requests to a remote MHS."""

    def __init__(self, client_cert: str, client_key: str, ca_certs: str, max_retries: int,
                 retry_delay: int, validate_cert: bool, http_proxy_host: str = None, http_proxy_port: int = None):
        """Create a new OutboundTransmission that loads certificates from the specified directory.

        :param client_cert: A string containing the filepath of the client certificate file.
        :param client_key: A string containing the filepath of the client private key file.
        :param ca_certs: A string containing the filepath of the certificate authority certificate file.
        :param max_retries: An integer with the value of the max number times to retry sending the request if it fails.
        :param retry_delay: An integer representing the delay (in milliseconds) to use between retry attempts.
        :param http_proxy_host The hostname of the HTTP proxy to be used.
        :param http_proxy_port The port of the HTTP proxy to be used.
        """
        self._client_cert = '''-----BEGIN CERTIFICATE-----
MIIE5DCCA8ygAwIBAgIRAIMzY0v3bf5i/ydIEFfrzT0wDQYJKoZIhvcNAQELBQAw
TDELMAkGA1UEBhMCR0IxDDAKBgNVBAoTA25oczELMAkGA1UECxMCQ0ExIjAgBgNV
BAMTGU5IUyBJTlQgQXV0aGVudGljYXRpb24gRzIwHhcNMjQwODI5MTIzNzU3WhcN
MjcwODI5MTMwNzU3WjBRMQwwCgYDVQQKEwNuaHMxEDAOBgNVBAsTB0RldmljZXMx
LzAtBgNVBAMTJmludHRpZi5jaGVja3VwaGVhbHRoLnRoaXJkcGFydHkubmhzLnVr
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAv3pXb3aYYD26nxXDBJqK
Jte94O6bc+4hzMbwF8BAc9fqGP1goDrf/QwFGC1pKY1+zWzLpOm4mdj0C38ll48Y
+Tis1u+m6qGZ29pGKAbkGMfjY94YTQt0J68W58eyM8yv6gjWvkszlW1Y33jj08ul
SInBDC46eRmHMif3NYLGT8AEsbC6O9YngG4stfJv8IIg72PnUPGIzepPmcbK0a44
8he4gjhXTIPAuEXbc+4pNk0ZbaxNQJ4JGVVeBDrHSX3w25X8Sw+Kr1XUVsC3c6DI
daXPU43Zl8qwLrbH3uTTkfkW33x0gEVxVuX2+8SNWPwdRpkgRTAgAvrolEL14WmX
KwIDAQABo4IBujCCAbYwCwYDVR0PBAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMB
BggrBgEFBQcDAjBvBggrBgEFBQcBAQRjMGEwIwYIKwYBBQUHMAGGF2h0dHA6Ly9v
Y3NwLm5ocy51ay9vY3NwMDoGCCsGAQUFBzAChi5odHRwOi8vcGtpLm5ocy51ay9p
bnQvRzIvYXV0aC9OSFNJTlRBdXRoRzIuY3J0MD4GA1UdIAQ3MDUwMwYLKoY6AIl7
ZQADAgAwJDAiBggrBgEFBQcCARYWaHR0cHM6Ly9wa2kubmhzLnVrL0NQUzBDBgNV
HR8EPDA6MDigNqA0hjJodHRwOi8vY3JsLm5ocy51ay9pbnQvZzIvYXV0aC9OSFNJ
TlRhdXRoRzJfYzIwLmNybDArBgNVHRAEJDAigA8yMDI0MDgyOTEyMzc1N1qBDzIw
MjYxMDA1MDEwNzU3WjAfBgNVHSMEGDAWgBQWx0cEqGDJ5F2rmfKU98PeM5jUIjAd
BgNVHQ4EFgQUbsMArmsnKZWDE4Daqn7xF/PgGfMwCQYDVR0TBAIwADAaBgkqhkiG
9n0HQQAEDTALGwVWMTAuMAMCBLAwDQYJKoZIhvcNAQELBQADggEBAH0FQTh/IZLV
kOK01Maw6aCeg3/1jks1RhIBAVK2OVoB0Fe3q4Dx/rFnqjWmIypyOrCWe6Av1RJD
j4f2f8tIRCUnINXQLLa5hA6L8QcCqSf92vPQegGE6RPGoyY3Z67xXDW0JjKhNfZk
8Jee3Py+XEYu1nxMkZAc0sewiPm4bbgiL/YtBVycGLR3OG++5Y+fWMGLd9Kjr6cQ
MaMVqxifurPZYqrja0XwFGnU3fFeT73cRGlym3TrCDrpWatsDQquIXtJAU7y+1K0
ENHrdjPazyJryo2EH7yGZCSD9GukcETIPjzAsxiN+fjfXoC2FaOHynWp7ikoaCv1
jN8AbCnViNM=
-----END CERTIFICATE-----'''
        self._client_key ='''-----BEGIN PRIVATE KEY-----
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
-----END PRIVATE KEY-----'''
        self._ca_certs = '''-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----'''
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._validate_cert = validate_cert

        self._proxy_host = http_proxy_host
        self._proxy_port = http_proxy_port

    async def make_request(self, url: str, headers: Dict[str, str], message: str,
                           raise_error_response: bool = True) -> httpclient.HTTPResponse:

        async def make_http_request():
            logger.info("About to send message with {headers} to {url} using {proxy_host} & {proxy_port}",
                        fparams={
                            "headers": headers,
                            "url": url,
                            "proxy_host": self._proxy_host,
                            "proxy_port": self._proxy_port
                        })
            response = await CommonHttps.make_request(url=url, method="POST", headers=headers, body=message,
                                                      client_cert=self._client_cert, client_key=self._client_key,
                                                      ca_certs=self._ca_certs, validate_cert=self._validate_cert,
                                                      http_proxy_host=self._proxy_host,
                                                      http_proxy_port=self._proxy_port,
                                                      raise_error_response=raise_error_response)
            logger.info("Sent message with {headers} to {url} using {proxy_host} & {proxy_port} and "
                        "received status code {code}",
                        fparams={
                            "headers": headers,
                            "url": url,
                            "proxy_host": self._proxy_host,
                            "proxy_port": self._proxy_port,
                            "code": response.code
                        })
            return response

        retry_result = await retriable_action.RetriableAction(make_http_request, self._max_retries,
                                                              self._retry_delay / 1000) \
            .with_success_check(lambda r: r.code != 599) \
            .with_retriable_exception_check(self._is_exception_retriable) \
            .execute()

        if not retry_result.is_successful:
            logger.error("Failed to make outbound HTTP request to {url}", fparams={"url": url})

            exception_raised = retry_result.exception
            if exception_raised:
                raise exception_raised
            else:
                raise OutboundTransmissionError("The max number of retries to make a request has been exceeded")

        return retry_result.result

    def _is_tornado_network_error(self, e: Exception) -> bool:
        return isinstance(e, httpclient.HTTPClientError) and e.code == 599

    def _is_exception_retriable(self, e: Exception) -> bool:
        retriable = True

        if isinstance(e, self.errors_not_to_retry):
            retriable = False

        # While we normally don't want to retry on an HTTP error, there is a special case where the Tornado
        # HTTPClientError's code is set to 599, which actually represents a connection error, rather than an HTTP error
        # response.
        if self._is_tornado_network_error(e):
            retriable = True

        return retriable
