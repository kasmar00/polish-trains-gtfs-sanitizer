import impuls

class NoSSLVerifyHttpResource(impuls.HTTPResource):
    """
    Wrapper for :py:class:`~impuls.HTTPResource` that doesn't verify SSL certs
    """
    def _do_request(self):
        return self.session.send(self.request.prepare(), stream=True, verify=False)