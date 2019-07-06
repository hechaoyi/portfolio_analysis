"""add m1_portfolio table

Revision ID: b74ebb31aff6
Revises: 4927ba6f1247
Create Date: 2019-07-04 22:03:33.498185

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b74ebb31aff6'
down_revision = '4927ba6f1247'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('m1_portfolio',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.Date(), nullable=True),
    sa.Column('value', sa.Float(), nullable=True),
    sa.Column('day_net_cash_flow', sa.Float(), nullable=True),
    sa.Column('day_capital_gain', sa.Float(), nullable=True),
    sa.Column('day_dividend_gain', sa.Float(), nullable=True),
    sa.Column('day_total_gain', sa.Float(), nullable=True),
    sa.Column('day_return_rate', sa.Float(), nullable=True),
    sa.Column('day_start_time', sa.DateTime(), nullable=True),
    sa.Column('day_start_value', sa.Float(), nullable=True),
    sa.Column('all_net_cash_flow', sa.Float(), nullable=True),
    sa.Column('all_capital_gain', sa.Float(), nullable=True),
    sa.Column('all_dividend_gain', sa.Float(), nullable=True),
    sa.Column('all_total_gain', sa.Float(), nullable=True),
    sa.Column('all_return_rate', sa.Float(), nullable=True),
    sa.Column('all_start_time', sa.DateTime(), nullable=True),
    sa.Column('all_start_value', sa.Float(), nullable=True),
    sa.Column('last_update', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('m1_portfolio')
    # ### end Alembic commands ###