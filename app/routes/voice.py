from flask import Blueprint, Response, request
from twilio.twiml.voice_response import Gather, VoiceResponse

from ..assistant import generate_reply, should_use_booking_context
from ..call_state import ensure_call_context, pop_pending_user_text, set_pending_user_text
from ..db import save_message

voice_bp = Blueprint("voice", __name__)


def build_gather():
    return Gather(
        input="speech",
        action="/voice/respond",
        method="POST",
        speech_timeout="auto",
    )


@voice_bp.route("/voice", methods=["POST"])
def voice():
    call_sid = request.form.get("CallSid")
    ensure_call_context(call_sid)

    resp = VoiceResponse()
    gather = build_gather()
    gather.say(
        "Hello! Thanks for calling. Welcome to superstar motel. How can I help you today?",
        voice="Polly.Joanna",
    )
    resp.append(gather)
    resp.redirect("/voice", method="POST")
    return Response(str(resp), mimetype="text/xml")


@voice_bp.route("/voice/respond", methods=["POST"])
def voice_respond():
    call_sid = request.form.get("CallSid")
    ensure_call_context(call_sid)

    user_text = request.form.get("SpeechResult", "").strip()
    resp = VoiceResponse()
    if not user_text:
        resp.say(
            "Sorry, I didn't catch that. Please say that again.",
            voice="Polly.Joanna",
        )
        resp.redirect("/voice", method="POST")
        return Response(str(resp), mimetype="text/xml")

    if should_use_booking_context(user_text):
        save_message("user", user_text)
        set_pending_user_text(user_text)
        resp.say(
            "Thanks, give me a moment while I check the right room for you.",
            voice="Polly.Joanna",
        )
        resp.redirect("/voice/answer", method="POST")
        return Response(str(resp), mimetype="text/xml")

    reply = generate_reply(user_text, save_user=True)
    resp.say(reply, voice="Polly.Joanna")
    gather = build_gather()
    resp.append(gather)
    resp.redirect("/voice", method="POST")
    return Response(str(resp), mimetype="text/xml")


@voice_bp.route("/voice/answer", methods=["POST"])
def voice_answer():
    call_sid = request.form.get("CallSid")
    ensure_call_context(call_sid)

    user_text = pop_pending_user_text()
    resp = VoiceResponse()
    if not user_text:
        resp.say(
            "Sorry, I didn't catch that. Please say that again.",
            voice="Polly.Joanna",
        )
        resp.redirect("/voice", method="POST")
        return Response(str(resp), mimetype="text/xml")

    reply = generate_reply(user_text, save_user=False)
    resp.say(reply, voice="Polly.Joanna")
    gather = build_gather()
    resp.append(gather)
    resp.redirect("/voice", method="POST")
    return Response(str(resp), mimetype="text/xml")
