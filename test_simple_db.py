import pytest
from . import database


@pytest.fixture(scope='function')
def loaded_db():
    data = [
        {
            str(l): chr(ord('a') + l) for l in range(0, k)
        } for k in range(1, 27)
    ]
    yield database.Db(stack=data)


def test_simple_db():
    my_db = database.Db()
    document = {'hello': 'world'}
    assert not my_db.insert(document), "insert should return None"
    assert my_db.find({'hello': 'world'}), "object is in the db"
    assert my_db.delete({'hello': 'world'}) == 1, "object will be deleted"
    assert not my_db.find({'hello': 'world'}), "object should not be found anymore"
    assert not my_db.insert(document), "insert should always return None"

    assert my_db.update(document, {'hello': 'space'}), "should update one document"
    assert my_db.find({'hello': 'space'}), "should find one document"

    return my_db


def test_loaded_db(loaded_db):
    my_db = loaded_db
    # by construction of loaded_db fixture, it should be so
    # the construct of the loaded db is basically:
    # [
    #    {'0': 'a'},
    #    {'0': 'a', '1': 'b'},
    #    {'0': 'a', '1': 'b', '2': 'c'},
    #    ...,
    #    {'0': 'a', '1': 'b', '2': 'c', ..., '24': 'y', '25': 'z'}
    # ]
    assert len(my_db.find({'0': 'a'})) == 26
    assert len(my_db.find({'1': 'b'})) == 25
    assert len(my_db.find({'2': 'd'})) == 0
    assert len(my_db.update({'0': 'a'}, {'2': 'd'})) == 26
    assert len(my_db.find({'2': 'd'})) == 26
    assert my_db.delete({'1': 'b'}) == 25
    assert len(my_db.stack) == 1


def test_with_transactions(loaded_db):
    db_copy = database.Db(stack=loaded_db.stack)

    # test an empty transaction
    my_tr = loaded_db.start_transaction()
    loaded_db.rollback_transaction(my_tr)
    assert not loaded_db.transactions

    # test a full delete of the DB and rollback
    my_tr = loaded_db.start_transaction()
    assert loaded_db.delete({'0': 'a'}, transaction=my_tr) == 26
    assert len(loaded_db.find({'0': 'a'}, transaction=my_tr)) == 0
    assert len(loaded_db.find({'0': 'a'}, transaction=None)) == 26
    loaded_db.rollback_transaction(my_tr)
    assert len(loaded_db.find({'0': 'a'})) == 26

    # test a full delete of the DB and commit
    my_tr = loaded_db.start_transaction()
    assert loaded_db.delete({'0': 'a'}, transaction=my_tr) == 26
    loaded_db.commit_transaction(transaction_id=my_tr)
    assert len(loaded_db.find({'0': 'a'})) == 0
    assert len(db_copy.find({'0': 'a'})) == 26


def test_full_transaction_with_update_and_so_on(loaded_db):
    db_copy = database.Db(stack=loaded_db.stack)
    tr1 = db_copy.start_transaction()
    tr2 = db_copy.start_transaction()
    tr3 = db_copy.start_transaction()

    # won't be commited
    assert db_copy.delete({'0': 'a'}, transaction=tr1) == 26
    assert not db_copy.find({'0': 'a'}, transaction=tr1)
    assert db_copy.find({'0': 'a'})
    # only transaction that will be commited
    assert len(
        db_copy.update(
            {'25': 'z'}, {'25': 'a', '24': -1}, transaction=tr2
        )
    ) == 1
    assert len(db_copy.find({'25': 'z'}, transaction=tr3)) == 1
    assert all(
        elem_a == elem_b
        for elem_a, elem_b in zip(db_copy.stack, loaded_db.stack)
    )
    db_copy.rollback_transaction(transaction_id=tr1)
    db_copy.rollback_transaction(transaction_id=tr3)

    assert len(db_copy.find({'25': 'a'}, transaction=tr2)) == 1

    db_copy.commit_transaction(transaction_id=tr2)
    assert len(db_copy.find({'25': 'a'})) == 1
