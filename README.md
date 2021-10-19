# flask-service-template-pubsub
SQL runner that uses Google Cloud Source Repositories as a source for the SQL.

Accepts requests via PubSub.


# TODO 2021-10-18:
- Validation tests - how will these work?
    - These should be written to an output table
- Auto identify dependencies 
    - Dependency graph is working
    - need to pull the dependencies from the SQL
