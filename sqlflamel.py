import sqlalchemy
import sqlalchemy.orm
import contextlib

# This is a base class (intended to be derived from in each ORM object) that acts as
# a kind of proxy between the SQLAlchemy query results object and the the client
# interface allowing for much cleaner--though still necessarily dynamic--syntax.
class QueryProxy:
	def __init__(self, query):
		self._query = query

	def __getattr__(self, attr):
		if hasattr(self._query, attr):
			return getattr(self._query, attr)

		return getattr(self, attr)

class Database:
	def __init__(self, engine):
		self._engine = sqlalchemy.create_engine(engine)

		for obj in self.OBJECTS:
			obj.metadata.create_all(self._engine)

		self._session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker())

		self._session.configure(bind=self._engine)

	# Creates a session object that must be manually closed and commited; you generally do not
	# want to use this directly.
	def create_session(self):
		session = self._session()

		# FINALLY! Here is where we combine the Proxy objects above with some really
		# mastferful hackery. This block of code iterates over every ORM type in
		# the self.OBJECTS sequence and sets a property on the type itself that lets you
		# issue an SQLAlchemy query simply using attribute-access syntax. Further,
		# that attribute is itself a Proxy object which, when the desired attribute
		# or method isn't found in the native query result object (i.e., filter(),
		# filter_by(), etc.), pushes the __getattr__ request up into the Proxy as a
		# fallback.
		#
		# All of this nastiness combined lets us achieve the final syntactic goal of
		# the following:
		#
		# database = Database("sqlite", "foo.sql3")
		#
		# with database.create_context() as db:
		# 	db.users.all()
		# 	db,users.filter_by(id=1)
		# 	db.users.from_jid("cubicool@gmail.com")
		#
		# Notice how we never have to issue db.query(User) explicitly, and also note how,
		# when db.users.from_jid isn't found in the normal SQLAlchemy query resultobject,
		# the lookup is then propogated upwards into the Proxy object, where a matching method
		# is found and used.
		#
		# In each case, accessing db.users actually calls a property function that evaluates
		# db.query(User) for us, and instantiates a Proxy using those results.
		for obj in self.OBJECTS:
			setattr(
				type(session),
				obj.__tablename__,
				property(lambda self, obj=obj: obj.Proxy(self.query(obj)))
			)

		return session

	# Creates a context-compatible object that can be used with the special "with" keyword.
	# This is almost always what you want to use. :)
	@contextlib.contextmanager
	def create_context(self):
		session = self.create_session()

		try:
			yield session

			session.commit()

		except:
			session.rollback()

			raise

		finally:
			session.close()

