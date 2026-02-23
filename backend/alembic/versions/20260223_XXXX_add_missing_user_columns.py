"""Add missing columns to dbo_ext.Users (RoleID, IsActive, CreatedAt, UpdatedAt)

Revision ID: 20260223_add_missing_user_columns
Revises: 
Create Date: 2026-02-23 10:55:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "r20260223_add_user_cols"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    schema = "dbo_ext"
    table = "Users"

    # RoleID (FK do Roles)
    op.add_column(
        table,
        sa.Column("RoleID", sa.Integer(), nullable=True),
        schema=schema,
    )

    # IsActive (BIT)
    op.add_column(
        table,
        sa.Column("IsActive", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        schema=schema,
    )

    # CreatedAt
    op.add_column(
        table,
        sa.Column(
            "CreatedAt",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("GETDATE()"),
        ),
        schema=schema,
    )

    # UpdatedAt
    op.add_column(
        table,
        sa.Column("UpdatedAt", sa.DateTime(), nullable=True),
        schema=schema,
    )

    # Uzupełnij RoleID dla istniejących użytkowników (np. 1 = Admin)
    op.execute("UPDATE dbo_ext.Users SET RoleID = 1 WHERE RoleID IS NULL")

    # Wymuś NOT NULL
    op.alter_column(
        table,
        "RoleID",
        schema=schema,
        nullable=False,
        existing_type=sa.Integer(),
    )

    # Dodaj FK do Roles
    op.create_foreign_key(
    "FK_Users_Roles",
        source_table=table,
        referent_table="Roles",
        local_cols=["RoleID"],
        remote_cols=["ID_ROLE"],
        source_schema=schema,
        referent_schema=schema,
        # bez ondelete – SQL Server domyślnie NO ACTION/RESTRICT
    )


def downgrade():
    schema = "dbo_ext"
    table = "Users"

    op.drop_constraint("FK_Users_Roles", table, schema=schema)

    op.drop_column(table, "UpdatedAt", schema=schema)
    op.drop_column(table, "CreatedAt", schema=schema)
    op.drop_column(table, "IsActive", schema=schema)
    op.drop_column(table, "RoleID", schema=schema)