import sqlflamel
import sqlalchemy.ext.declarative
import datetime

from sqlalchemy     import Column, Enum, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship, backref

Base = sqlalchemy.ext.declarative.declarative_base()

# This is a simple, spectacularly-devisive method of having an implicit constructor
# for your ORM objects that lets you set the various columns/attributes with
# keyword argument syntax.
class KeywordMixin:
	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)

class User(KeywordMixin, Base):
	__tablename__ = "users"

	BROADCAST = "on", "off", "online"

	id        = Column(Integer, primary_key=True)
	node      = Column(String(256), nullable=False)
	domain    = Column(String(255), nullable=False)
	broadcast = Column(Enum(*BROADCAST), default=BROADCAST[0])

	# Here we derive from QueryProxy and define a single convenience method: from_jid.
	# This lets us have code like the following:
	#
	# user = User.Proxy(db.query(Users)).from_jid()
	#
	# HOWEVER! That syntax alone isn't any more or less unweildy than the default SQLAlchemy
	# syntax, so we combine this technique with our custom Database class.
	class Proxy(sqlflamel.QueryProxy):
		def from_jid(self, jid):
			node, domain = jid.split("@")

			return self._query.filter_by(node=node, domain=domain).one()

class Hours(KeywordMixin, Base):
	__tablename__ = "hours"

	id      = Column(Integer, primary_key=True)
	user_id = Column(Integer, ForeignKey(User.id), nullable=False)
	start   = Column(DateTime)
	end     = Column(DateTime)

	user = relationship(User, backref=backref(__tablename__, order_by=id))

	class Proxy(sqlflamel.QueryProxy):
		def between(self, start, end):
			return self._query.filter(Hours.start.between(start, end)).all()

class Database(sqlflamel.Database):
	OBJECTS = User, Hours

if __name__ == "__main__":
	# A sqlflamel Database object handles all of the syntactical binding between the user-defined
	# ORM objects and the sessions it creates and manages.
	database = Database("sqlite://")

	# Add a User and commit it right away.
	with database.create_context() as db:
		db.add(User(node="cubicool", domain="github.com"))

	# Add some Hours, and commit those right away too.
	with database.create_context() as db:
		# Here we use our Proxy from_jid method. The Proxy notices that the Query object
		# (which is what is natively returned from SQLAlchemy) doesn't contain a from_jid attribute,
		# and instead finds the appropriate method in the Proxy type itself. The Proxy instance
		# has access to the Query instance, such that each attribute access is actually a query
		# evaluation underneath.
		user = db.users.from_jid("cubicool@github.com")

		# We synthetically generate some Hours objects, saying we worked every day for 8 hours
		# for the last 10 days. I don't personally recommend working 10 days in a row, but such
		# is life when you're the admin, web site, server, and client application developer all
		# in one. :)
		now = datetime.datetime.now()

		for i in range(10):
			start = now - datetime.timedelta(i)
			end   = start + datetime.timedelta(0, 60 * 60 * 8)

			db.add(Hours(user_id=user.id, start=start, end=end))

	# Here we just demonstrate some more syntactic sugar, which is really the only thing SQLFlamel
	# currently tries to address. :)
	with database.create_context() as db:
		# The following are all the same row in the users table, but since db.users access is
		# actually a property that issues a query dynamically, they are all unique instances.
		# This is a double-edged sword! SQLFlamel provides some syntactic sugar, but you need
		# to be aware of this prevent youself from issuing multiple queries over and over for
		# what is conceptually the same data.
		print(db.users.all()[0])
		print(db.users.one())
		print(db.users.filter_by(id=1).all()[0])
		print(db.users.from_jid("cubicool@github.com"))

		# Here we will once again use the convenience of the Proxy object to simplify our
		# querying and SQLAlchemy usage.
		now  = datetime.datetime.now().date()
		then = now - datetime.timedelta(5)

		# The "between" method here is provided by our Hours.Proxy object. Like from_jid
		# above, it is used when SQLFlamel determines that between() doesn't exist on the
		# Query object returned by SQLAlchemy.
		for hours in db.hours.between(then, now):
		 	print(hours.start, hours.end)

