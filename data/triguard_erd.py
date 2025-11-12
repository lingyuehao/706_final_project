from graphviz import Digraph

g = Digraph('TriGuard_ERD', format='png')
g.attr(rankdir='LR', nodesep='0.4', ranksep='0.7', fontsize='10',
       label='TriGuard Subrogation — Logical ERD', labelloc='t')
g.attr('node', shape='plaintext')

# simple table helper
def table(name, header, fields):
    rows = ''.join(f'<TR><TD ALIGN="LEFT">{f}</TD></TR>' for f in fields)
    label = f'''<
    <TABLE BORDER="1" CELLBORDER="1" CELLSPACING="0">
      <TR><TD BGCOLOR="#FFF4C2"><B>{header}</B></TD></TR>
      {rows}
    </TABLE>>'''
    g.node(name, label=label)

# --- tables ---
table('Claim', 'Claim', [
   'PK claim_number',
   'FK accident_key', 'FK policyholder_key', 'FK vehicle_key', 'FK driver_key',
   'subrogation', 'claim_est_payout', 'liab_prct',
   'claim_date', 'claim_day_of_week', 'channel', 'zip_code',
   'witness_present_ind', 'policy_report_filed_ind', 'in_network_bodyshop'
])

table('Vehicle', 'Vehicle', [
   'PK vehicle_key',
   'vehicle_made_year', 'vehicle_category', 'vehicle_price',
   'vehicle_color', 'vehicle_weight', 'vehicle_mileage'
])

table('Driver', 'Driver', [
   'PK driver_key',
   'year_of_born', 'gender', 'age_of_DL', 'safety_rating'
])

table('Policyholder', 'Policyholder', [
   'PK policyholder_key',
   'annual_income', 'high_education_ind', 'email_or_tel_available',
   'address_change_ind', 'living_status' ,'past_num_of_claims'
])

table('Accident', 'Accident', [
   'PK accident_key',
   'accident_site', 'accident_type'
])

# layout rows
with g.subgraph() as right_top:
    right_top.attr(rank='same')
    right_top.node('Vehicle'); right_top.node('Driver')
with g.subgraph() as right_bottom:
    right_bottom.attr(rank='same')
    right_bottom.node('Policyholder'); right_bottom.node('Accident')

# relationships
g.attr('edge', penwidth='1.4', arrowhead='none')
g.edge('Claim', 'Vehicle', label=' vehicle_key')
g.edge('Claim', 'Driver', label=' driver_key')
g.edge('Claim', 'Policyholder', label=' policyholder_key')
g.edge('Claim', 'Accident', label=' accident_key')

g.render('TriGuard_ERD_pretty', cleanup=True)
print('PNG exported: TriGuard_ERD_pretty.png')
