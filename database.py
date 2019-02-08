import copy
import threading
import uuid


def update_transaction(op, self, *args, **kwargs):
    db_state = self.transactions[kwargs['transaction']]
    new_db = Db(stack=db_state)
    res = op(new_db, *args, **kwargs)
    self.transactions[kwargs['transaction']] = new_db.stack
    return res


def manage_transactions(op):
    def wrapper(self, *args, **kwargs):
        if kwargs.get('transaction'):
            return update_transaction(op, self, *args, **kwargs)
        return op(self, *args, **kwargs)
    return wrapper


def match(document, query):
    return all(
        k in document and document[k] == query[k] for k in query
    )


class Db(object):
    def __init__(self, stack=None):
        # we can do a live dump of the db by passing
        # the stack as a parameter to the constructor
        # It works with any iterable.
        self.stack = [copy.deepcopy(item) for item in stack] if stack else []
        self.transactions = {}
        self.lock = threading.Lock()  # just lock to execute a transaction

    def insert(self, document, transaction=None):
        self.stack.append(document)

    @manage_transactions
    def find(self, query, transaction=None):
        return [
            elem for elem in self.stack
            if match(elem, query)
        ]

    @manage_transactions
    def update(
        self, select_query,
        update_query, transaction=None
    ):
        output = self.find(select_query)

        for document in output:
            document.update(update_query)

        return output

    @manage_transactions
    def delete(self, select_query, transaction=None):
        new_stack = [
            element for element in self.stack
            if not match(element, select_query)
        ]
        old_stack_length = len(self.stack)
        self.stack = new_stack
        return old_stack_length - len(self.stack)

    def start_transaction(self):
        transaction_id = str(uuid.uuid4())
        self.transactions[transaction_id] = list(self.stack)
        return transaction_id

    def rollback_transaction(self, transaction_id):
        self.transactions.pop(transaction_id)

    def commit_transaction(self, transaction_id):
        self.lock.acquire()
        self.stack = self.transactions.pop(transaction_id)
        self.lock.release()
