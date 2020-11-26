"""
Serve ingstion statistics for consumption in to a Sheets spreadsheet.

Development:

$ adev runserver server.py
"""
import asyncio
import csv
import logging
import os
import sys
from datetime import datetime
from io import StringIO

import stripe
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from dateutil.parser import parse as dateparse
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from chargify import Chargify

load_dotenv()

POLL_INTERVAL = 300   # seconds
CHARGIFY_DOMAIN = os.environ['CHARGIFY_DOMAIN']
CHARGIFY_API_KEY = os.environ['CHARGIFY_API_KEY']
STRIPE_API_KEY = os.environ['STRIPE_API_KEY']

stripe.api_key = STRIPE_API_KEY


async def start_background_tasks(app: web.Application):
    """ Start background tasks. """
    app['data_task'] = asyncio.create_task(data_task(app))


async def stop_background_tasks(app: web.Application):
    """ Stop background tasks. """
    app['data_task'].cancel()
    await app['data_task']


async def data_task(app: web.Application):
    """ Fetch data periodically. """
    log = app['log']
    while True:
        (app['customers_data'], app['subscriptions_data'], app['invoices_data'],
        app['chargify_subscriptions_data'], app['chargify_invoices_data'],
        app['stripe_customers_data']) = await export_data()
        log.info('CSV data loaded')
        await asyncio.sleep(POLL_INTERVAL)


def create_app():
    """
    Create web application.
    """
    logging.getLogger("stripe").setLevel(logging.WARNING)

    log = logging.Logger(__name__)
    start_logger()

    app = web.Application()
    app['log'] = log
    app['customers_data'], app['subscriptions_data'], app['invoices_data'] = dict(), dict(), dict()
    app['chargify_subscriptions_data'], app['chargify_invoices_data'], app['stripe_customers_data'] = dict(), dict(), dict()

    app['chargify_domain'] = CHARGIFY_DOMAIN
    app['chargify_api_key'] = CHARGIFY_API_KEY

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(stop_background_tasks)

    app.router.add_get('/healthcheck', healthcheck, name='healthcheck')
    app.router.add_get('/customers/csv', customers_csv, name='customers_csv')
    app.router.add_get('/subscriptions/csv', subscriptions_csv, name='subscriptions_csv')
    app.router.add_get('/invoices/csv', invoices_csv, name='invoices_csv')
    app.router.add_get(
        '/chargify_subscriptions/csv',
        chargify_subscriptions_csv,
        name='chargify_subscriptions_csv')
    app.router.add_get(
        '/chargify_invoices/csv',
        chargify_invoices_csv,
        name='chargify_invoices_csv')
    app.router.add_get('/stripe_customers/csv', stripe_customers_csv, name='stripe_customers_csv')
    return app


async def healthcheck(_: Request):
    """
    A simple healthcheck.
    """
    return Response(text="OK", content_type="text/html")


async def customers_csv(request: Request):
    """
    Output customers CSV format for consumption by Google Sheets.
    """
    output = StringIO()
    writer = csv.writer(output)

    if not request.app['customers_data']:
        raise Exception("data not loaded yet")

    writer.writerows(request.app['customers_data'])

    return Response(text=output.getvalue(), content_type="text/csv")


async def subscriptions_csv(request: Request):
    """
    Output subscriptions CSV format for consumption by Google Sheets.
    """
    output = StringIO()
    writer = csv.writer(output)

    if not request.app['subscriptions_data']:
        raise Exception("data not loaded yet")

    writer.writerows(request.app['subscriptions_data'])

    return Response(text=output.getvalue(), content_type="text/csv")


async def invoices_csv(request: Request):
    """
    Output invoices CSV format for consumption by Google Sheets.
    """
    output = StringIO()
    writer = csv.writer(output)

    if not request.app['invoices_data']:
        raise Exception("data not loaded yet")

    writer.writerows(request.app['invoices_data'])

    return Response(text=output.getvalue(), content_type="text/csv")


async def chargify_subscriptions_csv(request: Request):
    """
    Output Chargify subscriptions data.
    """
    output = StringIO()
    writer = csv.writer(output)

    if not request.app['chargify_subscriptions_data']:
        raise Exception("data not loaded yet")

    writer.writerows(request.app['chargify_subscriptions_data'])

    return Response(text=output.getvalue(), content_type="text/csv")


async def chargify_invoices_csv(request: Request):
    """
    Output Chargify invoices data.
    """
    output = StringIO()
    writer = csv.writer(output)

    if not request.app['chargify_invoices_data']:
        raise Exception("data not loaded yet")

    writer.writerows(request.app['chargify_invoices_data'])

    return Response(text=output.getvalue(), content_type="text/csv")


async def stripe_customers_csv(request: Request):
    """
    Output Stripe customers data.
    """
    output = StringIO()
    writer = csv.writer(output)

    if not request.app['stripe_customers_data']:
        raise Exception("data not loaded yet")

    writer.writerows(request.app['stripe_customers_data'])

    return Response(text=output.getvalue(), content_type="text/csv")


async def export_data():
    """
    Get customer data from Stripe and Chargify.
    """
    # Get data we need from Chargify
    chargify = Chargify(CHARGIFY_DOMAIN, CHARGIFY_API_KEY)
    async with chargify:
        subscriptions = [record['subscription'] async for record in await chargify.get_subscriptions()]
        invoices = [record async for record in await chargify.get_invoices()]

    subscriptions_lookup = {subscription['id']: subscription for subscription in subscriptions}

    # Get the customers from Stripe so we have access to their cards
    customers = []
    response = stripe.Customer.list(limit=100)
    customers.extend(response['data'])
    # Get more
    while response['data']:
        response = stripe.Customer.list(limit=100, starting_after=customers[-1]['id'])
        customers.extend(response['data'])

    customers_lookup = {customer['id']: customer for customer in customers}

    columns = (
        "customer[id]",
        "customer[first_name]",
        "customer[last_name]",
        "customer[phone]",
        "customer[company]",
        "customer[email]",
        "payment_method[type]",
        "payment_method[gateway_account_id]",
        "payment_method[reference_id]",
        "customer[auto_collection]",
        "customer[taxability]",
        "customer[vat_number]",
        "customer[preferred_currency_code]",
        "customer[net_term_days]",
        "customer[allow_direct_debit]",
        "customer[locale]",
        "customer[meta_data]",
        "customer[consolidated_invoicing]",
        "customer[invoice_notes]",
        "billing_address[first_name]",
        "billing_address[last_name]",
        "billing_address[email]", ''
        "billing_address[company]",
        "billing_address[phone]",
        "billing_address[line1]",
        "billing_address[line2]",
        "billing_address[line3]",
        "billing_address[city]",
        "billing_address[state_code]",
        "billing_address[state]",
        "billing_address[zip]",
        "billing_address[country]",
        "billing_address[validation_status]",
        "customer[registered_for_gst]",
        "customer[entity_code]",
        "customer[exempt_number]",
    )
    rows = [columns]

    for subscription in subscriptions:
        customer = subscription['customer']
        try:
            credit_card = subscription['credit_card']
        except KeyError:
            credit_card = dict(
                first_name=None,
                last_name=None,
                billing_address=None,
                billing_address_2=None,
                billing_city=None,
                billing_state=None,
                billing_zip=None,
                billing_country=None,
                vault_token=None,
            )

        stripe_customer_id = credit_card['vault_token']
        if stripe_customer_id:
            stripe_customer = customers_lookup[stripe_customer_id]
            default_source = stripe_customer['default_source']
            card_token = f"{stripe_customer_id}/{default_source}"
        else:
            card_token = None

        taxability = "taxable" if credit_card['billing_country'] == "GB" else "exempt"

        row = (
            customer['reference'],  # customer[id]",
            customer['first_name'],  # customer[first_name]",
            customer['last_name'],  # customer[last_name]",
            customer['phone'],  # customer[phone]",
            customer['organization'],  # customer[company]",
            customer['email'],  # customer[email]",
            "card" if card_token else None,  # payment_method[type]",
            "stripe" if card_token else None,  # payment_method[gateway_account_id]",
            card_token,  # payment_method[reference_id]",
            "on" if card_token else "off",  # customer[auto_collection]",
            taxability,  # customer[taxability]",
            customer['vat_number'] if taxability == "taxable" else None,  # customer[vat_number]",
            subscription['currency'],  # customer[preferred_currency_code]",
            None,  # customer[net_term_days]",
            None,  # customer[allow_direct_debit]",
            None,  # customer[locale]",
            None,  # customer[meta_data]",
            None,  # customer[consolidated_invoicing]",
            None,  # customer[invoice_notes]",
            credit_card['first_name'],  # billing_address[first_name]",
            credit_card['last_name'],  # billing_address[last_name]",
            customer['email'],  # billing_address[email]",
            None,  # billing_address[company]",
            None,  # billing_address[phone]",
            credit_card['billing_address'],  # billing_address[line1]",
            credit_card['billing_address_2'],  # billing_address[line2]",
            None,  # billing_address[line3]",
            credit_card['billing_city'],  # billing_address[city]",
            None,  # billing_address[state_code]",
            credit_card['billing_state'],  # billing_address[state]",
            credit_card['billing_zip'],  # billing_address[zip]",
            credit_card['billing_country'],  # billing_address[country]",
            "yes" if customer['verified'] else "no",  # billing_address[validation_status]",
            None,  # customer[registered_for_gst]",
            None,  # customer[entity_code]",
            None,  # customer[exempt_number]",
        )
        rows.append(row)

    customers_rows = rows

    # Build the subscriptions data
    columns = (
        "customer[id]",
        "subscription[id]",
        "subscription[plan_id]",
        "subscription[plan_quantity]",
        "subscription[plan_unit_price]",
        "currency",
        "subscription[setup_fee]",
        "subscription[status]",
        "subscription[start_date]",
        "subscription[trial_start]",
        "subscription[trial_end]",
        "subscription[started_at]",
        "subscription[current_term_start]",
        "subscription[current_term_end]",
        "subscription[cancelled_at]",
        "subscription[pause_date]",
        "subscription[resume_date]",
        "billing_cycles",
        "subscription[auto_collection]",
        "subscription[po_number]",
        "coupon_ids[0]",
        "coupon_ids[1]",
        "subscription[payment_source_id]",
        "subscription[invoice_notes]",
        "subscription[meta_data]",
        "shipping_address[first_name]",
        "shipping_address[last_name]",
        "shipping_address[email]",
        "shipping_address[company]",
        "shipping_address[phone]",
        "shipping_address[line1]",
        "shipping_address[line2]",
        "shipping_address[line3]",
        "shipping_address[city]",
        "shipping_address[state_code]",
        "shipping_address[state]",
        "shipping_address[zip]",
        "shipping_address[country]",
        "shipping_address[validation_status]",
        "addons[id][0]",
        "addons[quantity][0]",
        "addons[unit_price][0]",
        "addons[id][1]",
        "addons[quantity][1]",
        "addons[unit_price][1]")

    rows = [columns]

    subscriptions_to_plans = dict()

    for subscription in subscriptions:
        customer = subscription['customer']

        plan_id = subscription['product']['handle']
        if not plan_id:
            continue

        plan_id = {
            "unlimited": "unlimited-gbp",
            "pro-plus": "professional-gbp",
            "pro": "scale-gbp",
            "basic": "starter-gbp",
        }[plan_id]

        subscriptions_to_plans[subscription['id']] = plan_id

        status = {
            "active": "active",
            "canceled": "cancelled",
            "expired": "cancelled",
            "trial_ended": "cancelled",
            "trialing": "trial",
            "past_due": "active",
            "on_hold": "paused"
        }[subscription['state']]

        if subscription['coupon_codes']:
            coupon_ids = subscription['coupon_codes']
            if len(coupon_ids) == 1:
                coupon_ids.append(None)
        else:
            coupon_ids = [None, None]

        row = (
            customer['reference'],  # customer[id]",
            subscription['id'],  # subscription[id]",
            plan_id,  # subscription[plan_id]",
            1,  # subscription[plan_quantity]",
            None,
            # subscription['product']['price_in_cents'] / 100# subscription[plan_unit_price]",
            "GBP",  # curreny",
            0,  # subscription[setup_fee]",
            status,  # subscription[status]",
            None,  # subscription[start_date]" // this is the date for future subscriptions,
            # subscription[trial_start]",
            format_date(subscription['trial_started_at'] if subscription['state'] == "trialing"
                        else None),
            # subscription[trial_end]",
            format_date(subscription['trial_ended_at']if subscription['state'] == "trialing"
                        else None),
            # subscription[started_at]",
            format_date(subscription['created_at'] if status in ("active", "cancelled") else None),
            # subscription[current_term_start]",
            format_date(subscription['current_period_started_at']
                        if status in ("active", "paused") else None),
            # subscription[current_term_end]",
            format_date(subscription['current_period_ends_at']
                        if status in ("active", "paused") else None),
            format_date(subscription['trial_ended_at'] if subscription['state'] == "trial_ended"
                        else subscription['canceled_at']),
            # subscription[cancelled_at]",
            format_date(subscription['on_hold_at']),  # subscription[pause_date]",
            None,  # subscription[resume_date]",
            None,  # billing_cycles",
            "on",  # subscription[auto_collection]",
            None,  # subscription[po_number]",
            coupon_ids[0],  # coupon_ids[0]",
            coupon_ids[1],  # coupon_ids[1]",
            None,  # subscription[payment_source_id]",
            None,  # subscription[invoice_notes]",
            None,  # subscription[meta_data]",
            None,  # shipping_address[first_name]",
            None,  # shipping_address[last_name]",
            None,  # shipping_address[email]",
            None,  # shipping_address[company]",
            None,  # shipping_address[phone]",
            None,  # shipping_address[line1]",
            None,  # shipping_address[line2]",
            None,  # shipping_address[line3]",
            None,  # shipping_address[city]",
            None,  # shipping_address[state_code]",
            None,  # shipping_address[state]",
            None,  # shipping_address[zip]",
            None,  # shipping_address[country]",
            None,  # shipping_address[validation_status]",
            None,  # addons[id][0]",
            None,  # addons[quantity][0]",
            None,  # addons[unit_price][0]",
            None,  # addons[id][1]",
            None,  # addons[quantity][1]",
            None,  # addons[unit_price][1]"
        )
        rows.append(row)

    subscriptions_rows = rows

    # Build the invoices data
    columns = (
        "invoice[id]",
        "invoice[currency_code]",
        "invoice[customer_id]",
        "invoice[subscription_id]",
        "invoice[status]",
        "invoice[date]",
        "invoice[po_number]",
        "invoice[price_type]",
        "tax_override_reason",
        "invoice[vat_number]",
        "invoice[total]",
        "round_off",
        "invoice[due_date]",
        "invoice[net_term_days]",
        "use_for_proration",
        "billing_address[first_name]",
        "billing_address[last_name]",
        "billing_address[email]",
        "billing_address[company]",
        "billing_address[phone]",
        "billing_address[line1]",
        "billing_address[line2]",
        "billing_address[line3]",
        "billing_address[city]",
        "billing_address[state_code]",
        "billing_address[state]",
        "billing_address[zip]",
        "billing_address[country]",
        "billing_address[validation_status]",
        "shipping_address[first_name]",
        "shipping_address[last_name]",
        "shipping_address[email]",
        "shipping_address[company]",
        "shipping_address[phone]",
        "shipping_address[line1]",
        "shipping_address[line2]",
        "shipping_address[line3]",
        "shipping_address[city]",
        "shipping_address[state_code]",
        "shipping_address[state]",
        "shipping_address[zip]",
        "shipping_address[country]",
        "shipping_address[validation_status]",
        "line_items[id][0]",
        "line_items[entity_type][0]",
        "line_items[entity_id][0]",
        "line_items[date_from][0]",
        "line_items[date_to][0]",
        "line_items[description][0]",
        "line_items[unit_amount][0]",
        "line_items[quantity][0]",
        "line_items[amount][0]",
        "line_items[item_level_discount1_entity_id][0]",
        "line_items[item_level_discount1_amount][0]",
        "line_items[item_level_discount2_entity_id][0]",
        "line_items[item_level_discount2_amount][0]",
        "line_items[tax1_name][0]",
        "line_items[tax1_amount][0]",
        "line_items[tax2_name][0]",
        "line_items[tax2_amount][0]",
        "line_items[tax3_name][0]",
        "line_items[tax3_amount][0]",
        "line_items[tax4_name][0]",
        "line_items[tax4_amount][0]",
        "line_item_tiers[line_item_id][0]",
        "line_item_tiers[starting_unit][0]",
        "line_item_tiers[ending_unit][0]",
        "line_item_tiers[quantity_used][0]",
        "line_item_tiers[unit_amount][0]",
        "discounts[entity_type][0]",
        "discounts[entity_id][0]",
        "discounts[description][0]",
        "discounts[amount][0]",
        "taxes[name][0]",
        "taxes[rate][0]",
        "taxes[amount][0]",
        "taxes[description][0]",
        "taxes[juris_type][0]",
        "taxes[juris_name][0]",
        "taxes[juris_code][0]",
        "payments[amount][0]",
        "payments[payment_method][0]",
        "payments[date][0]",
        "payments[reference_number][0]",
        "notes[entity_type][0]",
        "notes[entity_id][0]",
        "notes[note][0]",
        "line_items[date_from][1]",
        "line_items[date_to][1]",
        "line_items[description][1]",
        "line_items[unit_amount][1]",
        "line_items[quantity][1]",
        "line_items[amount][1]",
        "line_items[entity_type][1]",
        "line_items[entity_id][1]",
    )

    rows = [columns]

    for invoice in invoices:
        customer = invoice['customer']
        subscription = subscriptions_lookup[invoice['subscription_id']]
        try:
            plan_id = subscriptions_to_plans[subscription['id']]
        except KeyError:
            continue
        billing_address = invoice['billing_address']
        period_from = dateparse(invoice['issue_date'])
        period_to = period_from + relativedelta(months=1)

        taxed = bool(float(invoice['tax_amount']))

        if invoice['status'] == "canceled":
            continue
        if not float(invoice['subtotal_amount']):
            continue

        amount = float(invoice['subtotal_amount']) - float(invoice['credit_amount'])
        discount_amount = float(invoice['discount_amount'])
        total_amount = amount - discount_amount + float(invoice['tax_amount'])

        # if invoice['uid'] == "inv_97h2jvz5jv2x7":
        #     import pdb
        #     pdb.set_trace()
        #     pass

        row = (
            invoice['uid'],  # invoice[id],
            "GBP",  # invoice[currency_code],
            None,  # Not applicable if subscription is specified # invoice[customer_id],
            invoice['subscription_id'],  # invoice[subscription_id],
            invoice['status'],  # invoice[status],
            format_date(invoice['issue_date']),  # invoice[date],
            invoice['sequence_number'],  # invoice[po_number],
            "tax_inclusive",  # invoice[price_type],
            None,  # tax_override_reason,
            None,  # invoice[vat_number],
            total_amount,  # invoice[total],
            None,  # round_off,
            None,  # invoice[due_date],
            None,  # invoice[net_term_days],
            "TRUE",  # use_for_proration,
            customer['first_name'],  # billing_address[first_name],
            customer['last_name'],  # billing_address[last_name],
            customer['email'],  # billing_address[email],
            customer['organization'],  # billing_address[company],
            None,  # billing_address[phone],
            billing_address['street'],  # billing_address[line1],
            billing_address['line2'],  # billing_address[line2],
            None,  # billing_address[line3],
            billing_address['city'],  # billing_address[city],
            None,  # billing_address[state_code],
            billing_address['state'],  # billing_address[state],
            billing_address['zip'],  # billing_address[zip],
            billing_address['country'],  # billing_address[country],
            None,  # billing_address[validation_status],
            None,  # shipping_address[first_name],
            None,  # shipping_address[last_name],
            None,  # shipping_address[email],
            None,  # shipping_address[company],
            None,  # shipping_address[phone],
            None,  # shipping_address[line1],
            None,  # shipping_address[line2],
            None,  # shipping_address[line3],
            None,  # shipping_address[city],
            None,  # shipping_address[state_code],
            None,  # shipping_address[state],
            None,  # shipping_address[zip],
            None,  # shipping_address[country],
            None,  # shipping_address[validation_status],
            None,  # line_items[id][0],
            "plan",  # line_items[entity_type][0],
            plan_id,  # line_items[entity_id][0],
            format_date(period_from),  # line_items[date_from][0],
            format_date(period_to),  # line_items[date_to][0],
            # line_items[description][0],
            f"{invoice['product_family_name']} - {invoice['product_name']}",
            amount,  # line_items[unit_amount][0],
            1,  # line_items[quantity][0],
            amount,  # line_items[amount][0],
            None,  # line_items[item_level_discount1_entity_id][0],
            None,  # line_items[item_level_discount1_amount][0],
            None,  # line_items[item_level_discount2_entity_id][0],
            None,  # line_items[item_level_discount2_amount][0],
            "VAT" if taxed else None,  # line_items[tax1_name][0],
            invoice['tax_amount'] if taxed else None,  # line_items[tax1_amount][0],
            None,  # line_items[tax2_name][0],
            None,  # line_items[tax2_amount][0],
            None,  # line_items[tax3_name][0],
            None,  # line_items[tax3_amount][0],
            None,  # line_items[tax4_name][0],
            None,  # line_items[tax4_amount][0],
            None,  # line_item_tiers[line_item_id][0],
            None,  # line_item_tiers[starting_unit][0],
            None,  # line_item_tiers[ending_unit][0],
            None,  # line_item_tiers[quantity_used][0],
            None,  # line_item_tiers[unit_amount][0],
            # discounts[entity_type][0],
            "document_level_coupon" if subscription['coupon_code'] and discount_amount else None,
            subscription['coupon_code'] if discount_amount else None,  # discounts[entity_id][0],
            None,  # discounts[description][0],
            invoice['discount_amount'] if discount_amount else None,  # discounts[amount][0],
            "VAT" if taxed else None,  # taxes[name][0],
            "20" if taxed else None,  # taxes[rate][0],
            invoice['tax_amount'] if taxed else None,  # taxes[amount][0],
            None,  # taxes[description][0],
            None,  # taxes[juris_type][0],
            None,  # taxes[juris_name][0],
            None,  # taxes[juris_code][0],
            invoice['paid_amount'],  # payments[amount][0],
            "other",  # payments[payment_method][0],
            format_date(invoice['paid_date']),  # payments[date][0],
            None,  # payments[reference_number][0],
            None,  # notes[entity_type][0],
            None,  # notes[entity_id][0],
            None,  # notes[note][0],
            None,  # line_items[date_from][1],
            None,  # line_items[date_to][1],
            None,  # line_items[description][1],
            None,  # line_items[unit_amount][1],
            None,  # line_items[quantity][1],
            None,  # line_items[amount][1],
            None,  # line_items[entity_type][1],
            None,  # line_items[entity_id][1],
        )
        rows.append(row)

    invoices_rows = rows

    chargify_subscriptions_rows = [flatten_dict(subscriptions[0]).keys()]
    for row in subscriptions:
        chargify_subscriptions_rows.append(flatten_dict(row).values())
    chargify_invoices_rows = [flatten_dict(invoices[0]).keys()]
    for row in invoices:
        chargify_invoices_rows.append(flatten_dict(row).values())
    stripe_customers_rows = [flatten_dict(customers[0]).keys()]
    for row in customers:
        stripe_customers_rows.append(flatten_dict(row).values())

    return customers_rows, subscriptions_rows, invoices_rows, chargify_subscriptions_rows, \
        chargify_invoices_rows, stripe_customers_rows


def format_date(datetime_str):
    """
    Format a datetime according to Chargify's requirements.
    """
    if not datetime_str:
        return None
    if isinstance(datetime_str, datetime):
        return datetime_str

    parsed = dateparse(datetime_str)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def flatten_dict(data, prefix="", output=None):
    """
    Create a flat representation of a dict.
    """
    if not output:
        output = dict()
    for key, value in data.items():
        if isinstance(value, dict):
            flatten_dict(value, key, output)
        else:
            fullkey = f"{prefix}[{key}]" if prefix else key
            output[fullkey] = value
    return output

def start_logger():
    """
    Log to stdout.
    """
    root = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)

def main():
    """
    Serve the application for production.
    """
    app = create_app()
    web.run_app(app, port=8080, print=app['log'].info)


if __name__ == "__main__":
    main()
