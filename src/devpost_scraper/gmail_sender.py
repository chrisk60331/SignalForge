"""Gmail SMTP sender service.

Sends emails through Gmail using an App Password over TLS.
"""

import os
import re
import smtplib
import ssl
from email.message import EmailMessage
from html import escape

from pydantic import BaseModel, Field


class EmailAttachment(BaseModel):
    """Email attachment payload."""

    filename: str = Field(description="Attachment file name")
    content: bytes = Field(description="Raw attachment bytes")
    mime_type: str = Field(default="application/octet-stream", description="MIME content type")


class SendEmailRequest(BaseModel):
    """Request model for sending a single email via Gmail."""

    to_email: str = Field(description="Recipient email address")
    subject: str = Field(description="Email subject line")
    body: str = Field(description="Plain-text email body")
    html_body: str | None = Field(
        default=None,
        description="Optional HTML body; plain text is always sent as fallback",
    )
    from_name: str = Field(default="", description="Sender display name")
    plain_text_only: bool = Field(
        default=False,
        description="Send plain text only (no HTML alternative). Required for SMS gateways.",
    )
    attachments: list[EmailAttachment] = Field(
        default_factory=list,
        description="Optional file attachments",
    )


class SendEmailResult(BaseModel):
    """Result of a send attempt."""

    success: bool = Field(description="Whether the email was sent")
    to_email: str = Field(description="Recipient address")
    error: str | None = Field(default=None, description="Error message if failed")


_URL_RE = re.compile(r"https?://[^\s<>()]+")
_TRAILING_PUNCT = ".,!?;:)"


def _linkify_text_for_html(text: str) -> str:
    """Escape text and convert URLs into clickable links."""
    parts: list[str] = []
    last = 0

    for match in _URL_RE.finditer(text):
        start, end = match.span()
        raw_url = match.group(0)
        clean_url = raw_url.rstrip(_TRAILING_PUNCT)
        trailing = raw_url[len(clean_url) :]

        parts.append(escape(text[last:start]))
        if clean_url:
            label = (
                "Get your free strategy report"
                if "/go" in clean_url
                else clean_url
            )
            href = escape(clean_url, quote=True)
            link_text = escape(label)
            parts.append(
                f'<a href="{href}" '
                f'style="color:#2563eb;text-decoration:underline;">{link_text}</a>'
            )
        parts.append(escape(trailing))
        last = end

    parts.append(escape(text[last:]))
    return "".join(parts).replace("\n", "<br>\n")


def _render_html_email(text_body: str) -> str:
    """Render a safe, minimal HTML email body from plain text."""
    linked = _linkify_text_for_html(text_body)
    return (
        "<!doctype html>"
        '<html><body style="margin:0;padding:0;background:#ffffff;">'
        '<div style="max-width:640px;margin:0 auto;padding:20px;'
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;'
        'font-size:15px;line-height:1.55;color:#111827;">'
        f"{linked}"
        "</div></body></html>"
    )


def send_email(req: SendEmailRequest) -> SendEmailResult:
    """Send a single email via Gmail SMTP with App Password."""

    gmail_user = os.getenv("GMAIL_USER", "")
    gmail_password = os.getenv("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_password:
        return SendEmailResult(
            success=False,
            to_email=req.to_email,
            error="GMAIL_USER and GMAIL_APP_PASSWORD must be set in .env",
        )

    msg = EmailMessage()
    msg["Subject"] = req.subject
    msg["To"] = req.to_email

    if req.from_name:
        msg["From"] = f"{req.from_name} <{gmail_user}>"
    else:
        msg["From"] = gmail_user

    msg.set_content(req.body)

    if not req.plain_text_only:
        html_body = req.html_body if req.html_body else _render_html_email(req.body)
        msg.add_alternative(html_body, subtype="html")

    for attachment in req.attachments:
        main_type, sub_type = "application", "octet-stream"
        if "/" in attachment.mime_type:
            main_type, sub_type = attachment.mime_type.split("/", 1)
        msg.add_attachment(
            attachment.content,
            maintype=main_type,
            subtype=sub_type,
            filename=attachment.filename,
        )

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(gmail_user, gmail_password)
            server.send_message(msg)

        return SendEmailResult(success=True, to_email=req.to_email)

    except smtplib.SMTPAuthenticationError as e:
        return SendEmailResult(
            success=False,
            to_email=req.to_email,
            error=f"Gmail authentication failed — check GMAIL_USER: {gmail_user} and GMAIL_APP_PASSWORD: {gmail_password} {e}",
        )
    except Exception as e:
        return SendEmailResult(
            success=False,
            to_email=req.to_email,
            error=str(e),
        )
