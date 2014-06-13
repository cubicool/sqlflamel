import sqlalchemy
import sqlalchemy.orm
import contextlib
import json

from sqlalchemy.types import TypeDecorator, TEXT
from sqlalchemy.ext.mutable import Mutable


# This helper function wraps the most common kind of relationship creation. It
# accepts an existing ORM type as the first argument, the column to match
# against using that type, the table name of the locally scoped ORM type (where
# it is usually safe to use __tablename__) to which the SQLAlchemy backref will
# be made, and finally the column by which the backref will be matched against.
#
# For example, if an ORM type BlogPost needs to create a relationship with a
# single instance of a different ORM type User, BlogPost could call this
# function with the User type, the attribute name corresponding to User's
# primary key, the name of the BlogPost table, and a BlogPost column to sort
# the backref by. The first returned value will be an SQLAlchemy Column type,
# and will create the relationship binding between BlogPost BACK to User. The
# second returned value will be an actual SQLAlchemy realtionship, which will
# provide a BlogPost instance with attribute-like access directly to the
# foreign User instance.
def relationship(cls, foreign_key, tablename, order_by):
    return (
        sqlalchemy.Column(
            sqlalchemy.Integer,
            sqlalchemy.ForeignKey(foreign_key),
            nullable=False
        ),
        sqlalchemy.orm.relationship(
            cls,
            backref=sqlalchemy.orm.backref(tablename, order_by=order_by)
        )
    )


# This is likely the relationship function you will find yourself most commonly
# using. It creates a relationship between the two passed in ORM types based on
# the specified attribute (which defaults to "id").
#
# The first argument will have an attribute corresponding to the name of the
# second argument (in lower case), and another corresponding to the name plus
# and underscore and the name of the attribute. FOR EXAMPLE (since there's no
# way you could possibly follow that):
#
# class User:
#   id = Column(Integer, primary_key=True)
#
# class BlogPost:
#   __tablename__ = "blog_posts"
#   id = Column(Integer, primary_key=True)
#
# sqlflamel.create_relationship(BlogPost, User)
#
# After this function call, the BlogPost ORM type will have the attributes
# "user" and "user_id", and the User ORM type will have an attribute called
# "blog_posts". We defaulted to using the attribute "id", but could just as
# easily have used any other attribute name they both shared.
#
# This is a hugely common relationship when using SQLAlchemy, and SQLFlamel
# tries to sipmlify it.
def create_relationship(cls, foreign_cls, attr="id"):
    fc_attr, fc = relationship(
        foreign_cls,
        getattr(foreign_cls, attr),
        cls.__tablename__,
        getattr(cls, attr)
    )

    fc_name = foreign_cls.__name__.lower()

    setattr(cls, fc_name + "_" + attr, fc_attr)
    setattr(cls, fc_name, fc)


# This is a base class (required to be derived from in each ORM object) that
# acts as a kind of proxy between the SQLAlchemy query results object and the
# the client interface, allowing for much cleaner--though still necessarily
# dynamic--syntax.
class QueryProxy:
    def __init__(self, query):
        self.query = query

    def __getattr__(self, attr):
        if hasattr(self.query, attr):
            return getattr(self.query, attr)

        return getattr(self, attr)


# The Database class acts purely as a base class, and relies on having a static
# method called types() defined somewhere in the namespace resolved by
# accessing self. This list returned by this static method should contain each
# ORM object that will be exposed in session instances created by this
# Database. Those tables will be available as attributes, and a new SQLAlchemy
# query will be issued with each access.
class Database:
    def __init__(self, engine):
        self._engine = sqlalchemy.create_engine(engine)

        for obj in self.types():
            obj.metadata.create_all(self._engine)

        self._session = sqlalchemy.orm.scoped_session(
            sqlalchemy.orm.sessionmaker()
        )

        self._session.configure(bind=self._engine)

    # Creates a session object that must be manually closed and commited; you
    # generally do not want to use this directly.
    def create_session(self):
        session = self._session()

        # FINALLY! Here is where we combine the Proxy objects above with some
        # really mastferful hackery. This block of code iterates over every ORM
        # type in the self.types() sequence and sets a property on the type
        # itself that lets you issue an SQLAlchemy query simply using
        # attribute-access syntax. Further, that attribute is itself a Proxy
        # object which, when the desired attribute or method isn't found in the
        # native query result object (i.e., filter(), filter_by(), etc.),
        # pushes the __getattr__ request up into the Proxy as a fallback.
        #
        # All of this nastiness combined lets us achieve the final syntactic
        # goal of the following:
        #
        # database = Database("sqlite://foo.sql3")
        #
        # with database.create_context() as db:
        #   db.users.all()
        #   db,users.filter_by(id=1)
        #   db.users.my_method(1, 10)
        #
        # Notice how we never have to issue db.query(User) explicitly, and also
        # note how, when db.users.my_method isn't found in the normal
        # SQLAlchemy Query results object, the lookup is then propogated
        # upwards into the Proxy object, where a matching method IS found and
        # used.
        #
        # In each case, accessing db.users actually calls a property function
        # that evaluates db.query(User) for us, and instantiates a Proxy using
        # those results.
        for obj in self.types():
            setattr(
                type(session),
                obj.__tablename__,
                property(lambda self, obj=obj: obj.Proxy(self.query(obj)))
            )

        return session

    @contextlib.contextmanager
    def create_context(self):
        """Creates a context-compatible object that can be used with the
        special "with" keyword. This is almost always what you want to use."""

        session = self.create_session()

        try:
            yield session

            session.commit()

        except:
            session.rollback()

            raise

        finally:
            session.close()


# This code is almost literally a copy/paste from the recipe found
# on the SQLAlchemy site at this URL:
#
# http://docs.sqlalchemy.org/en/rel_0_9/orm/extensions/mutable.html
#
# It is so handy and so useful, I decided to go ahead and include
# it here as well as the other SQLFlamel wrapper objects.
class JSON(TypeDecorator):
    impl = TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)

        return value


class MutableDict(Mutable, dict):
    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)

        self.changed()

    def __delitem__(self, key):
        dict.__delitem__(self, key)

        self._parent.changed()

    @classmethod
    def coerce(self, key, value):
        if not isinstance(value, MutableDict):
            if isinstance(value, dict):
                return MutableDict(value)

            return Mutable.coerce(key, value)

        else:
            return value


MutableDict.associate_with(JSON)
