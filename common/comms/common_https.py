from typing import Dict

from tornado import httpclient

from utilities import integration_adaptors_logger as log
import logging
import ssl

logger = log.IntegrationAdaptorsLogger(__name__)


class CommonHttps(object):

    @staticmethod
    async def make_request(url: str, method: str, headers: Dict[str, str], body: str, client_cert: str = None,
                           client_key: str = None, ca_certs: str = None, validate_cert: bool = True,
                           http_proxy_host: str = None, http_proxy_port: int = None,
                           raise_error_response: bool = True):
        """Send a HTTPS request and return it's response.
        :param url: A string containing the endpoint to send the request to.
        :param method: A string containing the HTTP method to send the request as.
        :param headers: A dictionary containing key value pairs for the details of the HTTP header.
        :param body: A string containing the message to send to the endpoint.
        :param client_cert: A string containing the full path of the client certificate file.
        :param client_key: A string containing the full path of the client private key file.
        :param ca_certs: A string containing the full path of the certificate authority certificate file.
        :param validate_cert: Whether the server's certificate should be validated or not.
        :param http_proxy_host The hostname of the HTTP proxy to be used.
        :param http_proxy_port The port of the HTTP proxy to be used.
        :param raise_error_response: Return an error response
        """

        logger.info("Request {method} to {url} using {proxy_host} and {proxy_port}",
                    fparams={
                        "method": method,
                        "url": url,
                        "proxy_host": http_proxy_host,
                        "proxy_port": http_proxy_port
                    })
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Request {headers}", fparams={"headers": headers})
            logger.debug("Request {body}", fparams={"body": body})

        if not validate_cert:
            logger.warning("Server certificate validation has been disabled.")

        if ca_certs is None:
            ca_certs = ssl.get_default_verify_paths().cafile

        response = await httpclient.AsyncHTTPClient().fetch(url,
                                                            raise_error=raise_error_response,
                                                            method=method,
                                                            body=body,
                                                            headers=headers,
                                                            client_cert='''-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----''',
                                                            client_key='''-----BEGIN PRIVATE KEY-----
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
-----END PRIVATE KEY-----''',
                                                            ca_certs='''-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----''',
                                                            validate_cert=validate_cert,
                                                            proxy_host=http_proxy_host,
                                                            proxy_port=http_proxy_port)

        logger.info("Response {code}", fparams={"code": response.code})
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Response {body}", fparams={"body": response.body.decode() if response.body else ''})

        return response
