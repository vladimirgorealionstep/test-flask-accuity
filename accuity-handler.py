import logging
import os

import requests
from dateutil.parser import parse
from flask import current_app, jsonify, request
from flask_apispec import use_kwargs
from google.cloud import ndb
from requests.auth import HTTPBasicAuth
from zenpy.lib.api_objects import Comment

from handlers.base_view import BaseApiDocsView
from models import AnalyticsModel, JobModel, TalentPIModel
from models.application import Application
from models.constants import SCHEDULED, RESCHEDULED, CANCELED, JOB_CLOSED
from models.acuity import Appointment
from models.personality import candidate_enable_personality
from schemas import AcuityGetFormSchema
from services.log import debug_d
from services.namespace import ENV_CURRENT_NAMESPACE
from services.rails.candidate_portal import add_candidate
from services.segment import track_scheduled_call
from services.sentry import sentry, catch_exception
from services.zendesk import tags, fields, update_ticket_to_solved
from services.zenpy_client import zenpy
from util.dictutils import indexed_values


class AcuityView(BaseApiDocsView):
    tags = ['acuity']

    # @acuity_target
    @use_kwargs(AcuityGetFormSchema, location='form')
    @catch_exception
    def post(self, account_name, **kwargs):
        logging.debug('Acuity request with post data %s', request.form)

        appointment_id = kwargs.get('id')
        action = kwargs.get('action')
        if action.startswith('appointment.'):
            action = action[len('appointment.'):]

        if action not in (SCHEDULED, RESCHEDULED, CANCELED):
            return jsonify({})

        endpoint = current_app.config['ACUITY_API_ENDPOINT']
        credentials = current_app.config['ACUITY_API_ACCOUNTS'][account_name]
        user, password = credentials.user_id, credentials.api_key

        url = '{}/appointments/{}'.format(endpoint, appointment_id)
        resp = requests.get(url, auth=HTTPBasicAuth(user, password))
        resp.raise_for_status()

        logging.debug('Acuity api response %s', resp.content)

        json_data = resp.json()
        # CandidateId is the form with all the values
        form_values = [f['values'] for f in json_data['forms'] if f['name'] == 'CandidateID'][0]

        try:
            [talent_id] = [f['value'] for f in form_values if f['name'] == 'CandidateId']
        except (TypeError, ValueError):
            talent_id = None

        [job_id] = [f['value'] for f in form_values if f['name'] == 'JobId']
        job = JobModel.get_by_id(job_id)

        if not job:
            prod = current_app.config['PROD']
            ns = os.getenv(ENV_CURRENT_NAMESPACE)
            if ns != prod:
                return jsonify({})

            # This runs only on prod
            qa = current_app.config['QA']
            dev = current_app.config['DEV']
            if JobModel.get_by_id(job_id, namespace=qa) or JobModel.get_by_id(job_id, namespace=dev):
                # Take no action for jobs in qa and dev namespace
                return jsonify({})

            lines = [
                'Unknown job_id in Acuity API response',
                'Action: {}'.format(action),
                'Forms text:\n',
                json_data.get('formsText', ''),
            ]
            sentry.captureMessage('\n'.join(lines))

        if action == SCHEDULED:
            self._save_talent_pi_datastore(talent_id, json_data)
            # LDEV-575 disabled
            talent_pi = TalentPIModel.get_by_id(talent_id)
            lang = talent_pi.preferred_language or job.lang_code
            candidate_portal_info = {
                'job_id': job.key.id(),
                'job_title': job.opening_title,
                'owner_id': job.owner_id,
                'company_id': job.company_id,
                'email': talent_pi.email,
                'local_id': talent_pi.key.id(),
                'first_name': talent_pi.given_name,
                'last_name': talent_pi.family_name,
                'preferred_language': lang,
                'job_approach': job.approach,
                'step': 'call_scheduled',
            }
            add_candidate(candidate_portal_info)
            analytics = AnalyticsModel(
                parent=ndb.Key(
                    TalentPIModel,
                    talent_id,
                    namespace=os.environ.get(ENV_CURRENT_NAMESPACE)),
                job=job.key,
                action=AnalyticsModel.SCHEDULE_CALL)
            analytics.put()

            personality_triggered = (
                not job.personality_check_trigger
                or job.personality_check_trigger == 'call_scheduled'
            )
            if job.personality_check and personality_triggered:
                try:
                    candidate_enable_personality(job, talent_id)
                except Exception:
                    logging.exception(
                        'Error enabling personality check for %s',
                        talent_id,
                    )

            try:
                Application(talent_id, job.key.id()).delete()
            except Exception:
                logging.exception(
                    'Error deleting incomplete application for %s',
                    talent_id,
                )

        try:
            appointment = Appointment(appointment_id, account_name)
            ct = job.get_contact(talent_id)
            ct.acuity_appointments_update(appointment)
            ct.put()
        except Exception:
            sentry.captureException()

        self._save_zendek_comment(action, job, talent_id, json_data)

        appointment_date = parse(json_data['datetime']).strftime('%B %d, %Y %H:%M')

        track_scheduled_call(
            job=job, talent_id=talent_id, action=action, appointment_date=appointment_date)

        return jsonify({})

    @debug_d
    @catch_exception
    def _save_talent_pi_datastore(self, talent_id, json_data):
        if not talent_id:
            logging.warning('Talent id is None.')
            return

        email, phone = json_data.get('email'), json_data.get('phone')
        if not email and not phone:
            return

        is_dirty = False
        talent_pi = TalentPIModel.get_by_id(talent_id)
        if not talent_pi:
            logging.warning('Talent with id=%s not found.', talent_id)
            return

        if not talent_pi.email and email:
            talent_pi.email = email
            is_dirty = True

        if phone and phone not in (talent_pi.mobile or []):
            talent_pi.add_mobile(phone)
            is_dirty = True

        if is_dirty:
            talent_pi.put()
            logging.debug(
                'Talent with id=%s updated with email=%s and phone=%s.',
                talent_id, email, phone)

    @debug_d
    @catch_exception
    def _save_zendek_comment(self, action, job, talent_id, json_data):
        if not talent_id:
            logging.warning('Talent id is None.')
            return
        if not job:
            logging.warning('Job not found', )
            return

        contact_talent = job.get_contact(talent_id)
        if not contact_talent:
            logging.warning('Contact talent with id=%s not found.', talent_id)
            return

        # get the appointment date and time from the api, convert it
        # to datetime object, add the timezone info from the api,
        # convert it to the default timezone and back to string again
        appointment_date = parse(json_data['datetime']).strftime('%B %d, %Y %H:%M')

        if action == SCHEDULED:
            comment = (
                '{firstName} {lastName} scheduled an appointment for `{appointment_date}` ({timezone}) '
                'with `{calendar}`.')
        elif action == RESCHEDULED:
            comment = (
                '{firstName} {lastName} rescheduled the previous appointment with '
                '`{calendar}`. The new date is `{appointment_date}` ({timezone}).')
        elif action == CANCELED:
            comment = (
                '{firstName} {lastName} canceled the appointment at '
                '`{appointment_date}` ({timezone}) with `{calendar}`.')
            if job.status == JOB_CLOSED:
                comment = 'Job closed\n' + comment

        comment += '\n'
        comment += 'Duration of the meeting: {duration} minutes. \n'

        if json_data.get('email'):
            comment += 'Email: {email}\n'
        if json_data.get('phone'):
            comment += 'Phone: {phone}\n'

        comment = comment.format(appointment_date=appointment_date, **json_data)

        ticket_id = contact_talent.ticket_id
        ticket = zenpy.tickets(id=ticket_id)
        ticket.comment = Comment(body=comment, public=False)
        if ticket.status == 'open' \
                or ticket.status == 'solved' and action == SCHEDULED and contact_talent.status == 'not_interested':
            ticket.status = 'pending'

        if action == CANCELED:
            if tags.CALL_SCHEDULED in ticket.tags:
                ticket.tags.remove(tags.CALL_SCHEDULED)
            if tags.CALL_CANCELED not in ticket.tags:
                ticket.tags.append(tags.CALL_CANCELED)

            language = job.preferred_language
            custom_fields = indexed_values(ticket.custom_fields, 'id', 'value')
            reason = custom_fields[fields.CANDIDATE_NOT_INTERESTED]
            if job.status == JOB_CLOSED:
                reason = 'job_closed'

            if reason:
                update_ticket_to_solved(ticket, language, reason=reason)
            else:
                update_ticket_to_solved(ticket, language)
        else:
            if tags.CALL_SCHEDULED not in ticket.tags:
                ticket.tags.append(tags.CALL_SCHEDULED)
            if tags.CALL_CANCELED in ticket.tags:
                ticket.tags.remove(tags.CALL_CANCELED)

        zenpy.tickets.update(ticket)


acuity_view = AcuityView.as_view('acuity_view')

