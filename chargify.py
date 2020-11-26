"""
Asynchronous Chargify client.
"""
from json.decoder import JSONDecodeError

import aiohttp
from aiohttp.client_exceptions import ContentTypeError


class APIError(Exception):
    """
    An error returned by the Chargify API.
    """

    def __init__(self, data):
        super().__init__(str(data))
        self.data = data


class Chargify:
    """
    Asynchronous Chargify client.
    """

    def __init__(self, domain, api_key):
        self.domain = domain
        self._session = None
        self._auth = aiohttp.BasicAuth(api_key, "x")

    async def _request(self, method, endpoint, params=None, json=None):
        url = "https://%s.chargify.com/%s" % (self.domain, endpoint)
        async with self._session.request(
                method, url, auth=self._auth, params=params, json=json) as req:
            try:
                response = await req.json()
            except ContentTypeError:
                response = None
            except JSONDecodeError:
                response = await req.text()

            if req.status >= 200 and req.status < 300:
                return response

            try:
                raise APIError(response['errors'])
            except TypeError:
                raise APIError(str(response))

    async def _paginated_request(self, *args, key=None, **kwargs):
        page = 1

        if "params" not in kwargs:
            kwargs['params'] = {}
        while True:
            kwargs['params']['page'] = page
            results = await self._request(*args, **kwargs)
            if key:
                results = results[key]
            if not results:
                return
            for result in results:
                yield result
            page += 1

    async def get_customer(self, customer_id):
        """
        GET /customers/{id}.json

        This method allows to retrieve the Customer properties by Chargify-generated Customer ID.
        """
        if not customer_id:
            raise ValueError("invalid customer_id")
        return await self._request("GET", f"customers/{customer_id}.json")

    async def get_customer_by_reference(self, reference):
        """
        GET /customers/lookup.json

        Use this method to return the customer object if you have the Reference ID (Your App) value.
        """
        return await self._request("GET", "customers/lookup.json", params=dict(reference=reference))

    async def get_customer_subscriptions(self, customer_id):
        """
        GET /customers/{customer_id}/subscriptions.json

        This method lists all subscriptions that belong to a customer.
        """
        return await self._request("GET", f"customers/{customer_id}/subscriptions.json")

    async def get_customers(self):
        """
        GET /customers.json
        """
        return self._paginated_request("GET", "customers.json")

    async def get_invoices(self):
        """
        GET /invoices.json
        """
        return self._paginated_request("GET", "invoices.json", key="invoices")

    async def create_customer(self, customer):
        """
        POST /customers.json
        """
        return await self._request("POST", "customers.json", json=dict(customer=customer))

    async def delete_customer(self, customer_id):
        """
        DELETE /customers/{id}.json

        This method allows to delete the Customer. When a delete response is received, the response
        status will be 204. There will be no content sent with the status.
        """
        await self._request("DELETE", f"customers/{customer_id}.json")
        return True

    async def get_subscription(self, subscription_id):
        """
        GET /subscriptions/{subscription_id}.json

        Read a Subscription.
        """
        return await self._request("GET", f"subscriptions/{subscription_id}.json")

    async def get_subscriptions(self, per_page=100):
        """
        Get /subscriptions.json
        """
        # Maximum per_page is 200
        return self._paginated_request(
            "GET", "subscriptions.json", params=dict(per_page=per_page))

    async def get_subscription_events(self, subscription_id, per_page=100):
        """
        GET /subscriptions/{subscription_id}/events.json

        The following request will return a list of events for a subscription.

        Each event type has its own `event_specific_data` specified.
        """
        return self._paginated_request(
            "GET", f"subscriptions/{subscription_id}/events.json", params=dict(per_page=per_page))

    async def get_product(self, product_id):
        """
        GET /products/{product_id}.json

        This endpoint allows you to read the current details of a product that you've created in
        Chargify.
        """
        return await self._request("GET", f"products/{product_id}.json")

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_):
        await self._session.close()
