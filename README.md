# chargify-to-chargebee
Serve spreadsheets for transferring Chargify to Chargebee

## Usage
This is deployed like most Python programs.

Set the following environment variables:

```
CHARGIFY_DOMAIN=
CHARGIFY_API_KEY=
STRIPE_API_KEY=
```

You can also add these to a file called `.env` and they will be loaded automatically.
1. ```
   pip install -r requirements.txt
   ```
1. ```
   python server.py
   ```

The server will start on port 8080, and the following paths will be available:

* `/customers/csv`
* `/subscriptions/csv`
* `/invoices/csv`

## Development
The following command will start a hot-reload development server:

```
adev runserver server.py
```
