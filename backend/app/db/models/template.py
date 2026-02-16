"""
Model tabeli dbo_ext.Templates.
Szablony wiadomości dla monitów (email/SMS/print).
Przechowują treść z placeholderami Jinja2: {{ debtor_name }}, {{ total_debt }} itp.
"""

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import AuditMixin, Base

TEMPLATE_TYPES = frozenset({"email", "sms", "print"})


class Template(AuditMixin, Base):
    __tablename__ = "Templates"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Szablony monitów. "
            "Body używa składni Jinja2. "
            "Każdy MonitHistory.TemplateID wskazuje na rekord tej tabeli."
        ),
    }

    id_template: Mapped[int] = mapped_column(
        "ID_TEMPLATE", Integer, primary_key=True, autoincrement=True,
    )
    template_name: Mapped[str] = mapped_column(
        "TemplateName", String(100), nullable=False, unique=True,
        comment="Unikalna nazwa szablonu. Np. 'Wezwanie pierwsze email PL'",
    )
    template_type: Mapped[str] = mapped_column(
        "TemplateType", String(20), nullable=False,
        comment=f"Typ: {', '.join(sorted(TEMPLATE_TYPES))}",
    )
    subject: Mapped[str | None] = mapped_column(
        "Subject", String(200), nullable=True,
        comment="Temat wiadomości email. NULL dla SMS i print.",
    )
    body: Mapped[str] = mapped_column(
        "Body", Text, nullable=False,
        comment=(
            "Treść szablonu Jinja2. "
            "Dostępne zmienne: {{ debtor_name }}, {{ total_debt }}, "
            "{{ invoice_list }}, {{ due_date }}, {{ company_name }}"
        ),
    )

    # Relacje
    monit_histories: Mapped[list["MonitHistory"]] = relationship(  # type: ignore[name-defined]
        "MonitHistory", back_populates=None
    )

    def __repr__(self) -> str:
        return (
            f"<Template id={self.id_template} "
            f"name={self.template_name!r} "
            f"type={self.template_type!r}>"
        )