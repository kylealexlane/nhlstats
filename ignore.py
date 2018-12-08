from sqlalchemy import create_engine

### LOCAL CONNECTION

# hostname = 'localhost'
# username = 'kylelane'
# password = ''
# database = 'kylelane'

# engine = create_engine('postgresql://kylelane@localhost:5432/kylelane')



# RDS CONNECTION

hostname = 'nhlstatsinstance.c6ihzpoxrual.us-east-2.rds.amazonaws.com'
username = 'nhlstats'
password = 'stats1029384756'
database = 'nhlstatsdb'

engine = create_engine('postgresql://nhlstats:stats1029384756@nhlstatsinstance.c6ihzpoxrual.us-east-2.rds.amazonaws.com:5432/nhlstatsdb')
dbString = 'postgresql://nhlstats:stats1029384756@nhlstatsinstance.c6ihzpoxrual.us-east-2.rds.amazonaws.com:5432/nhlstatsdb'
