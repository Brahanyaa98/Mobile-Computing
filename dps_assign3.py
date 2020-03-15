import psycopg2

DATABASE_NAME = 'dps_assign3'

def getconnection(user = 'postgres', password = '1234', dbname = 'postgres'):
	return psycopg2.connect("dbname='" + dbname + "' + user + "' host='localhost' password='" + password + "'")

def loadratings(ratingstablename, ratingsfilepath, connection):
	create_db(DATABASE_NAME)
	connect = connection
	cursor = connect.cursor()
	cursor.execute("create table " + ratingstablename +"(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
	cursor.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
	cursor.execute("alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
	cursor.close()
	cursor.commit()

def rangePartition(ratingstablename, ratingsfilepath, connection):
	connect = connection
	cursor = connect.cursor()
	value = 5/numberofpartitions
	RANGE_TABLE_PREFIX = 'range_part'
	for i in range(0,numberofpartitions):
		minumumRange = i*value
		maximumRange = minumumRange+value
		table_name = RANGE_TABLE_PREFIX + str(i)
		cursor.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
		if i==0:
			cursor.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minimumRange) + " and rating <= " + str(maximumRange) + ";")
		else:
			 cursor.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minimumRange) + " and rating <= " + str(maximumRange) + ";")

	cursor.close()
	connect.commit


def roundrobbinPartition(ratingstablename, ratingsfilepath, connection):
	connect = connection
	cursor = connect.cursor()
	RROBIN_TABLE_PREFIX = 'rrobin_part'
	for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
	    cursor.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
	    cursor.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(i) + ";")
        cursor.close()
        connect.commit()


def roundrobbinInsert(ratingstablename,userid,itemid, rating, connection):
 	connect = connection
 	cursor = connect.cursor()
 	RROBIN_TABLE_PREFIX = 'rrobin_part'
 	cursor.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
 	cursor.execute("select count(*) from " + ratingstablename + ";");
 	total_rows = (cursor.fetchall())[0][0]
 	numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, connection)
 	index = (total_rows-1) % numberofpartitions
 	table_name = RROBIN_TABLE_PREFIX + str(index)
 	cursor.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
 	cursor.close()
 	connect.commit()

def rangeinsert(ratingstablename,userid,itemid,connection):
	connect = connection
	cursor = connect.cursor()
	RANGE_TABLE_PREFIX = 'range_part'
	numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, connection)
	value = 5/numberofpartitions
	index = int(rating/value)
	if rating % value == 0 and index != 0:
		index = index-1
	table_name = RANGE_TABLE_PREFIX + str(index)
	cursor.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cursor.close()
    connect.commit()

def create_db(dbname):
	#establishing connection to the default database
	connect = getconnection(dbname='postgres')
	connect.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	cursor = connect.cursor()
	cursor.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    cursor.close()
    connect.close()

def count_partitions(prefix, openconnection):
	connect = connection
	cursor = connect.cursor()
	cursor.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
	count = cursor.fetchone()[0]
	cursor.close()


	return count
	



