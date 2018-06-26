# -*- coding: utf-8 -*-

import croniter
import requests
import urllib
import time

from collections import namedtuple
from datetime import datetime, timedelta
from email.utils import make_msgid, parseaddr
from flask import session, Response, url_for, render_template
from flask_babel import gettext as __
from flask_login import login_user
from http.client import RemoteDisconnected
from werkzeug.utils import parse_cookie
from selenium.webdriver import chrome, firefox
from selenium.common.exceptions import WebDriverException

# Superset framework imports
from superset import app, security_manager
from superset.models.schedules import (
    ScheduleType,
    EmailDeliveryType,
    SliceEmailReportFormat,
    get_scheduler_model,
)
from superset.utils import (
    get_celery_app,
    get_email_address_list,
    send_email_smtp,
    retry,
)

# Globals
config = app.config
celery_app = get_celery_app(config)

# Time in seconds, we will wait for the page to load and render
PAGE_RENDER_WAIT = 30


EmailContent = namedtuple('EmailContent', ['body', 'data', 'images'])


def _get_recipients(schedule):
    bcc = config.get('EMAIL_REPORT_BCC_ADDRESS', None)

    if schedule.deliver_as_group:
        to = schedule.recipients,
        yield (to, bcc)
    else:
        for to in get_email_address_list(schedule.recipients):
            yield (to, bcc)


def _deliver_email(schedule, subject, email):
    for (to, bcc) in _get_recipients(schedule):
        send_email_smtp(
            to, subject, email.body, config,
            data=email.data,
            images=email.images,
            bcc=bcc,
            mime_subtype='related'
        )


def _generate_mail_content(schedule, screenshot, name, url):
    if schedule.delivery_type == EmailDeliveryType.attachment:
        images = None
        data = {
            'screenshot.jpg': screenshot,
        }
        body = __(
            '<b><a href="%(url)s">Explore in Superset</a></b><p></p>',
            name=name,
            url=url
        )
    elif schedule.delivery_type == EmailDeliveryType.inline:
        # Get the domain from the 'From' address ..
        # and make a message id without the < > in the ends
        domain = parseaddr(config.get('SMTP_MAIL_FROM'))[1].split('@')[1]
        msgid = make_msgid(domain=domain)[1:-1]

        images = {
            msgid: screenshot
        }
        data = None
        body = __(
            '''
            <b><a href="%(url)s">Explore in Superset</a></b><p></p>
            <img src="cid:%(msgid)s">
            ''',
            name=name, url=url, msgid=msgid
        )

    return EmailContent(body, data, images)


def _get_auth_cookies():
    # Login with the user specified to get the reports
    user = security_manager.find_user(config.get('EMAIL_REPORTS_USER'))
    login_user(user)

    # A mock response object to get the cookie information from
    response = Response()
    app.session_interface.save_session(app, session, response)

    cookies = []

    # Set the cookies in the driver
    for name, value in response.headers:
        if name.lower() == 'set-cookie':
            cookie = parse_cookie(value)
            cookies.append(cookie['session'])

    return cookies


def create_webdriver():
    # Create a webdriver for use in fetching reports
    if config.get('EMAIL_REPORTS_WEBDRIVER') == 'firefox':
        driver_class = firefox.webdriver.WebDriver
        options = firefox.options.Options()
    elif config.get('EMAIL_REPORTS_WEBDRIVER') == 'chrome':
        driver_class = chrome.webdriver.WebDriver
        options = chrome.options.Options()

    options.add_argument('--headless')

    # Prepare args for the webdriver init
    kwargs = dict(
        options=options
    )
    kwargs.update(config.get('WEBDRIVER_CONFIGURATION'))

    # Initialize the driver
    driver = driver_class(**kwargs)

    # Some webdrivers need an initial hit to the welcome URL
    # before we set the cookie
    welcome_url = urllib.parse.urljoin(
        config.get('WEBDRIVER_BASEURL'),
        url_for('Superset.welcome')
    )

    # Hit the welcome URL and check if we were asked to login
    driver.get(welcome_url)
    elements = driver.find_elements_by_id('loginbox')

    # This indicates that we were not prompted for a login box.
    if not elements:
        return driver

    # Set the cookies in the driver
    for cookie in _get_auth_cookies():
        info = dict(name='session', value=cookie)
        driver.add_cookie(info)

    return driver


def deliver_dashboard(schedule):
    dashboard = schedule.dashboard
    dashboard_url = urllib.parse.urljoin(
        config.get('WEBDRIVER_BASEURL'),
        url_for(
            'Superset.dashboard',
            dashboard_id=dashboard.slug
        )
    )

    # Create a driver, fetch the page, wait for the page to render
    driver = create_webdriver()
    window = config.get('WEBDRIVER_WINDOW')['dashboard']
    driver.set_window_size(*window)
    driver.get(dashboard_url)
    time.sleep(PAGE_RENDER_WAIT)

    # Set up a function to retry once for the element.
    # This is buggy in certain selenium versions with firefox driver
    get_element = retry((RemoteDisconnected, ConnectionResetError))(
        driver.find_element_by_id
    )

    element = get_element('grid-container')

    try:
        screenshot = element.screenshot_as_png
    except WebDriverException:
        # Some webdrivers do not support screenshots for elements.
        # In such cases, take a screenshot of the entire page.
        screenshot = driver.screenshot()
    finally:
        driver.quit()

    # Generate the email body and attachments
    email = _generate_mail_content(
        schedule,
        screenshot,
        dashboard.dashboard_title,
        dashboard_url,
    )

    subject = __(
        '%(prefix)s %(title)s',
        prefix=config.get('EMAIL_REPORTS_SUBJECT_PREFIX'),
        title=dashboard.dashboard_title,
    )

    _deliver_email(schedule, subject, email)


def _get_slice_data(schedule):
    slc = schedule.slice

    slice_url = urllib.parse.urljoin(
        config.get('WEBDRIVER_BASEURL'),
        url_for(
            'Superset.slice_json',
            slice_id=slc.id,
            csv='true',
        )
    )

    # URL to include in the email
    url = urllib.parse.urljoin(
        config.get('WEBDRIVER_BASEURL'),
        url_for(
            'Superset.slice',
            slice_id=slc.id,
        )
    )

    cookies = {}
    for cookie in _get_auth_cookies():
        cookies['session'] = cookie

    response = requests.get(slice_url, cookies=cookies)
    response.raise_for_status()
    rows = [r.split(b',') for r in response.content.splitlines()]

    if schedule.delivery_type == EmailDeliveryType.inline:
        data = None

        # Parse the csv file and generate HTML
        columns = rows.pop(0)
        body = render_template(
            'superset/reports/slice_data.html',
            columns=columns,
            rows=rows,
            name=slc.slice_name,
            link=url,
        )

    elif schedule.delivery_type == EmailDeliveryType.attachment:
        data = {
            __('%(name)s.csv', name=slc.slice_name): response.content
        }
        body = __(
            '<b><a href="%(url)s">%(name)s</a></b><br/>',
            name=slc.slice_name,
            url=url
        )

    return EmailContent(body, data, None)


def _get_slice_visualization(schedule):
    slc = schedule.slice

    # Create a driver, fetch the page, wait for the page to render
    driver = create_webdriver()
    window = config.get('WEBDRIVER_WINDOW')['slice']
    driver.set_window_size(*window)

    slice_url = urllib.parse.urljoin(
        config.get('WEBDRIVER_BASEURL'),
        url_for(
            'Superset.slice',
            slice_id=slc.id,
        )
    )

    driver.get(slice_url)
    time.sleep(PAGE_RENDER_WAIT)

    # Set up a function to retry once for the element.
    # This is buggy in certain selenium versions with firefox driver
    get_element = retry((RemoteDisconnected, ConnectionResetError))(
        driver.find_element_by_class_name
    )

    # Get the chart-container element
    element = get_element('chart-container')

    try:
        screenshot = element.screenshot_as_png
    except WebDriverException:
        # Some webdrivers do not support screenshots for elements.
        # In such cases, take a screenshot of the entire page.
        screenshot = driver.screenshot()
    finally:
        driver.quit()

    # Generate the email body and attachments
    return _generate_mail_content(
        schedule,
        screenshot,
        slc.slice_name,
        slice_url
    )


def deliver_slice(schedule):
    if schedule.email_format == SliceEmailReportFormat.data:
        email = _get_slice_data(schedule)
    elif schedule.email_format == SliceEmailReportFormat.visualization:
        email = _get_slice_visualization(schedule)
    else:
        raise RuntimeError('Unknown email report format')

    subject = __(
        '%(prefix)s %(title)s',
        prefix=config.get('EMAIL_REPORTS_SUBJECT_PREFIX'),
        title=schedule.slice.slice_name,
    )

    _deliver_email(schedule, subject, email)


@celery_app.task(bind=True, soft_time_limit=300)
def schedule_email_report(report_type, schedule_id):
    model_cls = get_scheduler_model(report_type)
    schedule = model_cls.get(id=schedule_id)

    # The user may have disabled the schedule. If so, ignore this
    if not schedule.active:
        # TODO: Log the action
        return

    if report_type == ScheduleType.dashboard:
        deliver_dashboard(schedule)
    else:
        deliver_slice(schedule)


def next_schedules(crontab, start_at, stop_at, resolution=0):
    crons = croniter.croniter(crontab, start_at)
    previous = datetime.min
    for eta in crons.all_next(datetime):
        # Do not cross the time boundary
        if eta >= stop_at:
            break

        if eta < start_at:
            continue

        # Do not allow very frequent tasks
        if eta - previous < timedelta(seconds=resolution):
            continue

        yield eta
        previous = eta


def schedule_window(report_type, start_at, stop_at, resolution):
    '''
    Find all active schedules and schedule celery tasks for
    each of them with a specific ETA (determined by parsing
    the cron schedule for the schedule)
    '''
    model_cls = get_scheduler_model(report_type)

    for schedule in model_cls.filter(active=True):
        args = (
            report_type,
            schedule.id,
        )

        # Schedule the job for the specified time window
        for eta in next_schedules(schedule.crontab,
                                  start_at,
                                  stop_at,
                                  resolution):
            schedule_email_report.apply_async(args=args, eta=eta)


@celery_app.task()
def schedule_hourly():
    ''' Celery beat job meant to be invoked hourly '''

    # Get the top of the hour
    start_at = datetime.now().replace(microsecond=0, second=0, minute=0)
    stop_at = start_at + timedelta(seconds=3600)
    schedule_window(ScheduleType.dashboard, start_at, stop_at)
    schedule_window(ScheduleType.slice, start_at, stop_at)
