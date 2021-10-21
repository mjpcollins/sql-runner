# flask-service-template-pubsub
SQL runner that uses Google Cloud Source Repositories as a source for the SQL.

Accepts requests via PubSub.


# TODO 2021-10-18:
- Validation tests - how will these work?
    - Load validation scripts & run them before & after task
    - should equal 0 before task & 1 after task
    - These tests should be written to an output table
