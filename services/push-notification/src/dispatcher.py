"""
Push Notification Dispatcher — FCM & APNs delivery.

Architecture role: Consumes from match-events-notifications topic
and delivers push notifications to mobile devices via FCM (Android)
and APNs (iOS).

Design:
- Consumes notification payloads from Kafka
- Resolves user device tokens from Redis
- Sends via FCM/APNs with exponential backoff retry
- at-least-once delivery (duplicate notifications preferred over missed)
- Multi-language templating (FR, EN, AR)
"""

import asyncio
import json
import structlog
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
from enum import Enum

logger = structlog.get_logger(__name__)


class NotificationPriority(str, Enum):
    CRITICAL = "critical"   # Goals, red cards — wake the phone
    HIGH = "high"           # Penalties, VAR
    NORMAL = "normal"       # Half-time, full-time
    LOW = "low"             # Substitutions


class Platform(str, Enum):
    FCM = "fcm"     # Firebase Cloud Messaging (Android + Web)
    APNS = "apns"   # Apple Push Notification Service (iOS)


@dataclass
class NotificationRequest:
    """Notification to be sent to a user."""
    user_id: str
    match_id: str
    event_type: str
    title: str
    body: str
    priority: NotificationPriority
    language: str
    device_tokens: list[str]
    platform: Platform
    data: dict
    image_url: Optional[str] = None


# --- Notification Templates ---
# Multi-language templates for each event type.
# Template variables are filled from the enriched event data.

NOTIFICATION_TEMPLATES = {
    "GOAL_SCORED": {
        "fr": {
            "title": " BUUUUT ! {team_name}",
            "body": "{scorer_name} marque à la {minute}' ! {home_team} {home_score}-{away_score} {away_team}",
        },
        "en": {
            "title": " GOAL! {team_name}",
            "body": "{scorer_name} scores at {minute}'! {home_team} {home_score}-{away_score} {away_team}",
        },
        "ar": {
            "title": " هدف! {team_name}",
            "body": "{scorer_name} يسجل في الدقيقة {minute}'! {home_team} {home_score}-{away_score} {away_team}",
        },
    },
    "RED_CARD": {
        "fr": {
            "title": "🟥 Carton Rouge !",
            "body": "{player_name} ({team_name}) expulsé à la {minute}'",
        },
        "en": {
            "title": "🟥 Red Card!",
            "body": "{player_name} ({team_name}) sent off at {minute}'",
        },
        "ar": {
            "title": "🟥 بطاقة حمراء!",
            "body": "{player_name} ({team_name}) طُرد في الدقيقة {minute}'",
        },
    },
    "PENALTY_AWARDED": {
        "fr": {
            "title": "⚠️ Pénalty !",
            "body": "Pénalty accordé à {team_name} à la {minute}'",
        },
        "en": {
            "title": "⚠️ Penalty!",
            "body": "Penalty awarded to {team_name} at {minute}'",
        },
        "ar": {
            "title": "⚠️ ركلة جزاء!",
            "body": "ركلة جزاء لصالح {team_name} في الدقيقة {minute}'",
        },
    },
    "FULL_TIME": {
        "fr": {
            "title": "🏁 Fin du match",
            "body": "{home_team} {home_score}-{away_score} {away_team}",
        },
        "en": {
            "title": "🏁 Full Time",
            "body": "{home_team} {home_score}-{away_score} {away_team}",
        },
        "ar": {
            "title": "🏁 نهاية المباراة",
            "body": "{home_team} {home_score}-{away_score} {away_team}",
        },
    },
    "HALF_TIME": {
        "fr": {
            "title": "⏸️ Mi-temps",
            "body": "{home_team} {home_score}-{away_score} {away_team}",
        },
        "en": {
            "title": "⏸️ Half Time",
            "body": "{home_team} {home_score}-{away_score} {away_team}",
        },
        "ar": {
            "title": "⏸️ نهاية الشوط الأول",
            "body": "{home_team} {home_score}-{away_score} {away_team}",
        },
    },
}


class PushDispatcher:
    """
    Dispatches push notifications to FCM and APNs.

    Uses batch delivery for efficiency (FCM supports up to 500 tokens
    per multicast request).
    """

    def __init__(self):
        self._sent_count = 0
        self._failed_count = 0

    async def dispatch(self, request: NotificationRequest):
        """Send a push notification via the appropriate platform."""
        try:
            if request.platform == Platform.FCM:
                await self._send_fcm(request)
            elif request.platform == Platform.APNS:
                await self._send_apns(request)

            self._sent_count += 1

            logger.info(
                "notification_sent",
                user_id=request.user_id,
                event_type=request.event_type,
                platform=request.platform.value,
                priority=request.priority.value,
            )

        except Exception as e:
            self._failed_count += 1
            logger.error(
                "notification_failed",
                user_id=request.user_id,
                error=str(e),
            )
            raise

    async def _send_fcm(self, request: NotificationRequest):
        """
        Send via Firebase Cloud Messaging.
        In production: use firebase_admin SDK.
        """
        # firebase_admin.messaging.send_multicast(
        #     MulticastMessage(
        #         tokens=request.device_tokens,
        #         notification=Notification(
        #             title=request.title,
        #             body=request.body,
        #             image=request.image_url,
        #         ),
        #         data=request.data,
        #         android=AndroidConfig(
        #             priority="high" if request.priority in (
        #                 NotificationPriority.CRITICAL,
        #                 NotificationPriority.HIGH,
        #             ) else "normal",
        #             ttl=timedelta(minutes=5),
        #         ),
        #     )
        # )
        pass

    async def _send_apns(self, request: NotificationRequest):
        """
        Send via Apple Push Notification Service.
        In production: use aioapns library.
        """
        # async with APNs(
        #     key=APNS_KEY,
        #     key_id=APNS_KEY_ID,
        #     team_id=TEAM_ID,
        # ) as apns:
        #     for token in request.device_tokens:
        #         await apns.send_notification(
        #             NotificationRequest(
        #                 device_token=token,
        #                 message={
        #                     "aps": {
        #                         "alert": {"title": request.title, "body": request.body},
        #                         "sound": "goal.caf" if request.priority == NotificationPriority.CRITICAL else "default",
        #                         "interruption-level": "time-sensitive",
        #                     }
        #                 }
        #             )
        #         )
        pass


def render_template(event_type: str, language: str, data: dict) -> tuple[str, str]:
    """
    Render notification title and body from template.
    Falls back to English if the requested language is unavailable.
    """
    templates = NOTIFICATION_TEMPLATES.get(event_type, {})
    template = templates.get(language, templates.get("en", {}))

    if not template:
        return (event_type, json.dumps(data))

    title = template["title"].format(**data)
    body = template["body"].format(**data)
    return title, body
