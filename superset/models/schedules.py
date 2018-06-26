# -*- coding: utf-8 -*-
"""Models for scheduled execution of jobs"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import enum

from flask_appbuilder import Model
from sqlalchemy import (
    Column, String, Text, Boolean, Integer, Enum, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declared_attr
from superset.models.helpers import AuditMixinNullable, ImportMixin
from superset import security_manager


metadata = Model.metadata


class ScheduleType(enum.Enum):
    slice = 'slice'
    dashboard = 'dashboard'


class EmailDeliveryType(enum.Enum):
    attachment = 'Attachment'
    inline = 'Inline'


class SliceEmailReportFormat(enum.Enum):
    visualization = 'Visualization'
    data = 'Raw data'


class EmailSchedule(object):

    """Schedules for emailing slices / dashboards"""

    id = Column(Integer, primary_key=True)
    active = Column(Boolean, default=True, index=True)
    crontab = Column(String(50))

    @declared_attr
    def user_id(cls):
        return Column(Integer, ForeignKey('ab_user.id'))

    @declared_attr
    def user(cls):
        return relationship(
            security_manager.user_model,
            backref=cls.__tablename__,
            foreign_keys=[cls.user_id],
        )

    recipients = Column(Text)
    deliver_as_group = Column(Boolean, default=False)
    delivery_type = Column(Enum(EmailDeliveryType))


class DashboardEmailSchedule(Model,
                             AuditMixinNullable,
                             ImportMixin,
                             EmailSchedule):
    __tablename__ = 'dashboard_email_schedules'
    dashboard_id = Column(Integer, ForeignKey('dashboards.id'))
    dashboard = relationship(
        'Dashboard',
        backref='email_schedules',
        foreign_keys=[dashboard_id],
    )


class SliceEmailSchedule(Model,
                         AuditMixinNullable,
                         ImportMixin,
                         EmailSchedule):
    __tablename__ = 'slice_email_schedules'
    slice_id = Column(Integer, ForeignKey('slices.id'))
    slice = relationship(
        'Slice',
        backref='email_schedules',
        foreign_keys=[slice_id],
    )
    email_format = Column(Enum(SliceEmailReportFormat))


def get_scheduler_model(report_type):
    if report_type == ScheduleType.dashboard:
        return DashboardEmailSchedule
    elif report_type == ScheduleType.slice:
        return SliceEmailSchedule