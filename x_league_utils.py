def raise_html_status_code(status_code:int):
    match status_code:
        case 200:
            return None
        case 400:
            raise ConnectionError('HTTP 400 Bad Request:\n\tA client error has caused this connection to be closed unexpectedly.')
        case 401:
            raise ConnectionRefusedError('HTTP 401 Unauthorized:\t\nFor some reason, the website has deemed this request as a unauthorized request.')
        case 403:
            raise ConnectionRefusedError('HTTP 403 Forbidden:\t\nFor some reason, the  website has forbidden you and/or this device\'s IP address from attempting to communicate to this device.')
        case 404:
            raise ConnectionError('HTTP 404 Not Found:\t\nFor some reason, the webpage could not be found.')
        case 418:
            raise AssertionError('HTTP 418 \"I\'m a Teapot\" Error:\t\nThe webpage for this is not a web page, but a teapot-like aparatus.')
        case 500:
            raise ConnectionError('HTTP 500 Internal Server Error:\t\nAn unspecified issue within the website has prevented this function from connecting to the website.')
        case 501:
            raise ConnectionError('HTTP 501 Not Implemented:\n\tThis webpage, and or connection method has not been implemented.')
        case 502:
            raise ConnectionAbortedError('HTTP 502 Bad Gateway:\n\tDue to a bad gateway on the server side, a connection could not be made to the website.')
        case 503:
            raise ConnectionAbortedError('HTTP 503 Service Unavalible:\n\tThe website is tempararily unadvalible.')
        case 504:
            raise ConnectionAbortedError('HTTP 504 Gateway Timeout:\n\tThe function did not recive a timely response from the  website.')
        case 511:
            raise ConnectionRefusedError('HTTP 511 Network Authentication Required:\n\tTo use this functtion, and by extension the internet, you need to authenticate your access to this internet connection.')
        case default:
            raise Exception(f'Unhandled HTTP Status code. Code: {status_code}')
