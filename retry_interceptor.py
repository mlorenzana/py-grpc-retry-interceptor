from grpc import StatusCode, RpcError, UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor
import time
import random


class RetryInterceptor(UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor):

    def __init__(self, max_retries=3, retry_codes=None, retry_timeout_ms=100, retry_jitter_ms=20):
        if retry_codes is None:
            retry_codes = [StatusCode.UNAVAILABLE, StatusCode.RESOURCE_EXHAUSTED]
        self.max_retries = max_retries
        self.retry_codes = retry_codes
        self.retry_timeout_ms = retry_timeout_ms
        self.retry_jitter_ms = retry_jitter_ms

        if self.retry_jitter_ms > self.retry_timeout_ms:
            raise ValueError('retry_jitter_ms cannot be greater than retry_timeout_ms')

    def _next_retry_timeout_seconds(self):
        ms_timeout = self.retry_timeout_ms + (random.randint(-1, 1) * self.retry_jitter_ms)
        s_timeout = ms_timeout / 1000
        return s_timeout

    def intercept_unary_unary(self, continuation, client_call_details, request):
        retry_count = 0
        while True:
            try:
                response = continuation(client_call_details, request)
                return response
            except RpcError as e:
                if e.code() not in self.retry_codes:
                    raise e
                if retry_count >= self.max_retries:
                    raise e
                retry_count += 1
                time.sleep(self._next_retry_timeout_seconds())

    def intercept_unary_stream(self, continuation, client_call_details, request):

        def intercept(continuation, client_call_details, request):

            def iterator_wrapper(gen):

                retry_count = 0
                has_started = False
                while True:
                    try:
                        val = next(gen)
                        has_started = True
                        yield val
                    except RpcError as e:
                        if has_started:
                            raise e
                        if e.code() not in self.retry_codes:
                            raise e
                        if retry_count >= self.max_retries:
                            raise e

                        retry_count += 1
                        timeout = self._next_retry_timeout_seconds()
                        time.sleep(timeout)

                        gen = continuation(client_call_details, request)
                    except StopIteration:
                        return

            return iterator_wrapper(continuation(client_call_details, request))

        return intercept(continuation, client_call_details, request)
