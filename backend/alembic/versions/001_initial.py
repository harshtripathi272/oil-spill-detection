"""Initial migration

Revision ID: 001_initial
Revises:
Create Date: 2026-05-04 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create incidents table
    op.create_table('incidents',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('latitude', sa.Float(), nullable=False),
        sa.Column('longitude', sa.Float(), nullable=False),
        sa.Column('confidence_score', sa.Float(), nullable=True),
        sa.Column('detection_time', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=True),
        sa.Column('status', sa.String(), nullable=True),
        sa.Column('bbox_coordinates', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('sar_image_path', sa.String(), nullable=True),
        sa.Column('processed_image_path', sa.String(), nullable=True),
        sa.Column('model_version', sa.String(), nullable=True),
        sa.Column('processing_time', sa.Float(), nullable=True),
        sa.Column('metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create dag_runs table
    op.create_table('dag_runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(), nullable=False),
        sa.Column('run_id', sa.String(), nullable=False),
        sa.Column('incident_id', sa.String(), nullable=True),
        sa.Column('state', sa.String(), nullable=False),
        sa.Column('start_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('end_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('execution_time', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(['incident_id'], ['incidents.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('run_id')
    )

    # Create task_instances table
    op.create_table('task_instances',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dag_run_id', sa.Integer(), nullable=False),
        sa.Column('task_id', sa.String(), nullable=False),
        sa.Column('state', sa.String(), nullable=False),
        sa.Column('start_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('end_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('duration', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(['dag_run_id'], ['dag_runs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create metrics table
    op.create_table('metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('value', sa.Float(), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=True),
        sa.Column('category', sa.String(), nullable=False),
        sa.Column('metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create system_status table
    op.create_table('system_status',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('component', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('last_check', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=True),
        sa.Column('details', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('system_status')
    op.drop_table('metrics')
    op.drop_table('task_instances')
    op.drop_table('dag_runs')
    op.drop_table('incidents')