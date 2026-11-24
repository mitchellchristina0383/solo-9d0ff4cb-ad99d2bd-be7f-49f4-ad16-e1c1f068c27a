"""WSGI entry point used by the container (``gunicorn wsgi:app``)."""

from app import create_app

app = create_app()
